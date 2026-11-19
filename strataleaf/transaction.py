import math
from dataclasses import dataclass
from .codec import digest, dumps, key as checked_key, loads, name
from .errors import Conflict, InvalidDocument, InvalidReference, NotFound
from .indexes import refresh_document
from .mvcc import latest_row
from .references import is_readable, normalize_identity, require_live_target, resolve_identity
from .schema import validate_document


@dataclass(frozen=True)
class CommitResult:
    revision: int
    count: int
    replayed: bool = False


class WriteBatch:
    def __init__(self, store, request_key=None):
        self.store = store
        self.request_key = checked_key(request_key) if request_key is not None else None
        self.operations = []
        self.references = []
        self.state = "open"

    def _append(self, operation):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        identity = (operation["collection"], operation["key"])
        if any((op["collection"], op["key"]) == identity for op in self.operations):
            raise InvalidDocument("one operation per document per batch")
        expected = operation["expected"]
        if expected is not None and (isinstance(expected, bool) or not isinstance(expected, int) or expected < 0):
            raise InvalidDocument("expected revision must be nonnegative or None")
        self.operations.append(operation)
        return self

    def reference(self, reference, target=None, source=None, *, name=None):
        """Register a fixed-version reference in this batch.

        The reference is committed or rolled back together with the document
        writes.  ``reference`` (or the keyword ``name``) is the reference name.
        ``target`` and ``source`` accept the same identity forms as
        :meth:`strataleaf.references.References.create`; a target written by
        this batch resolves to the revision produced by the commit, so a
        document and the references it carries become visible atomically.
        """
        reference_name = name if name is not None else reference
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        if not isinstance(reference_name, str) or not reference_name:
            raise InvalidDocument("reference requires a name")
        checked_key(reference_name)
        if target is None:
            raise InvalidDocument("reference requires a target version")
        if any(item["name"] == reference_name for item in self.references):
            raise InvalidDocument("duplicate reference in batch: " + reference_name)
        self.references.append({"name": reference_name,
                                "target": normalize_identity(target),
                                "source": None if source is None else normalize_identity(source)})
        return self

    ref = reference

    def put(self, collection, key, document, expected=None, expires_at=None):
        name(collection)
        checked_key(key)
        if not isinstance(document, dict):
            raise InvalidDocument("document must be an object")
        if expires_at is not None and (isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at)):
            raise InvalidDocument("expires_at must be a finite timestamp")
        return self._append({"collection": collection, "key": key, "document": loads(dumps(document)),
                             "expected": expected, "expires_at": expires_at})

    def delete(self, collection, key, expected=None):
        return self._append({"collection": name(collection), "key": checked_key(key), "document": None,
                             "expected": expected, "expires_at": None})

    def abort(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        self.operations.clear()
        self.references.clear()
        self.state = "aborted"

    def commit(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        if not self.operations and not self.references:
            raise InvalidDocument("batch must contain between 1 and 10000 operations")
        if len(self.operations) > 10000:
            raise InvalidDocument("batch must contain between 1 and 10000 operations")
        fingerprint = digest(self.operations) if not self.references else digest(
            {"operations": self.operations, "references": self.references})
        try:
            with self.store.atomic(writable=True) as connection:
                if self.request_key is not None:
                    prior = connection.execute("SELECT * FROM receipts WHERE request_key=?", (self.request_key,)).fetchone()
                    if prior:
                        if prior["request_hash"] != fingerprint:
                            raise Conflict("request key was used with different operations")
                        self.state = "committed"
                        return CommitResult(prior["rev"], prior["count"], True)
                head = int(self.store.get("head"))
                for op in self.operations:
                    schema = connection.execute("SELECT schema FROM collections WHERE name=?", (op["collection"],)).fetchone()
                    if schema is None:
                        raise NotFound("unknown collection: " + op["collection"])
                    old = latest_row(connection, op["collection"], op["key"], head)
                    current = old["rev"] if old is not None else 0
                    if op["expected"] is not None and op["expected"] != current:
                        raise Conflict(f"revision conflict for {op['collection']}/{op['key']}: expected {op['expected']}, found {current}")
                    if op["document"] is not None:
                        validate_document(op["document"], loads(schema[0]))
                for item in self.references:
                    if connection.execute("SELECT 1 FROM version_refs WHERE name=?", (item["name"],)).fetchone():
                        raise Conflict("reference already exists: " + item["name"])
                revision = head + 1
                for op in self.operations:
                    body = dumps(op["document"]) if op["document"] is not None else None
                    connection.execute("INSERT INTO versions VALUES (?,?,?,?,?)",
                                       (op["collection"], op["key"], revision, body, op["expires_at"]))
                    refresh_document(connection, op["collection"], op["key"], op["document"])
                pending = {(op["collection"], op["key"]) for op in self.operations}
                floor = int(self.store.get("floor"))
                for item in self.references:
                    target = resolve_identity(connection, item["target"], head, revision, pending)
                    require_live_target(connection, target)
                    source = None
                    if item["source"] is not None:
                        source = resolve_identity(connection, item["source"], head, revision, pending)
                        if not is_readable(connection, source, floor):
                            raise InvalidReference("reference source version is no longer readable")
                    connection.execute("INSERT INTO version_refs VALUES (?,?,?,?,?,?,?,?)",
                                       (item["name"],
                                        None if source is None else source.collection,
                                        None if source is None else source.key,
                                        None if source is None else source.revision,
                                        target.collection, target.key, target.revision, revision))
                if self.references:
                    self.store.set("reference_epoch", int(self.store.get("reference_epoch")) + 1)
                payload = {"revision": revision, "operations": self.operations}
                connection.execute("INSERT INTO commits VALUES (?,?,?,?)",
                                   (revision, dumps(payload), self.request_key, fingerprint))
                if self.request_key is not None:
                    connection.execute("INSERT INTO receipts VALUES (?,?,?,?)", (self.request_key, revision, fingerprint, len(self.operations)))
                self.store.set("head", revision)
        except BaseException:
            self.state = "aborted"
            raise
        self.state = "committed"
        return CommitResult(revision, len(self.operations))

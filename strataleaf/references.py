"""Persistent fixed-version references.

A reference records a *version identity* -- ``(collection, key, revision)`` --
so that later rewrites, deletions or same-key recreations of the target do not
change what the reference means.  References may be attached to a source
version; while that source version is still readable (directly through an
ordinary historical read, or transitively through another reference) its
target versions are protected from historical compaction.

Ordinary key based reads keep their existing behaviour: a read below the
retained floor still fails with :class:`SnapshotTooOld`.  Only an explicit
reference lookup may resolve the exact target version below the floor.
"""
from dataclasses import dataclass

from .codec import key as checked_key, loads, name
from .errors import Conflict, InvalidDocument, InvalidReference, NotFound
from .mvcc import Record


@dataclass(frozen=True)
class VersionIdentity:
    """The immutable identity of one physical document version."""
    collection: str
    key: str
    revision: int

    @property
    def triple(self):
        return (self.collection, self.key, self.revision)

    def as_dict(self):
        return {"collection": self.collection, "key": self.key, "revision": self.revision}


@dataclass(frozen=True)
class Reference:
    """A named fixed-version dependency together with its resolved target."""
    name: str
    source: VersionIdentity | None
    target: VersionIdentity
    created_revision: int
    record: Record | None = None

    @property
    def collection(self):
        return self.target.collection

    @property
    def key(self):
        return self.target.key

    @property
    def revision(self):
        return self.target.revision

    @property
    def document(self):
        return None if self.record is None else self.record.document

    @property
    def expires_at(self):
        return None if self.record is None else self.record.expires_at

    def as_dict(self):
        return {"name": self.name,
                "source": None if self.source is None else self.source.as_dict(),
                "target": self.target.as_dict(),
                "created_revision": self.created_revision,
                "document": self.document, "expires_at": self.expires_at}

    def __getitem__(self, item):
        return self.as_dict()[item]


def _unpack_identity(value):
    """Normalize caller input to ``(collection, key, revision_or_None)``."""
    if isinstance(value, VersionIdentity):
        collection, document_key, revision = value.collection, value.key, value.revision
    elif isinstance(value, Record):
        collection, document_key, revision = value.collection, value.key, value.revision
    elif isinstance(value, dict):
        if set(value) - {"collection", "key", "revision", "rev"} or "collection" not in value or "key" not in value:
            raise InvalidDocument("version identity requires collection, key and revision")
        collection, document_key = value["collection"], value["key"]
        revision = value.get("revision", value.get("rev"))
    elif isinstance(value, (tuple, list)) and len(value) in (2, 3):
        collection, document_key = value[0], value[1]
        revision = value[2] if len(value) == 3 else None
    elif isinstance(value, str):
        head, separator, at = value.rpartition("@")
        if not separator:
            head, at = value, None
        collection, slash, document_key = head.partition("/")
        if not slash:
            raise InvalidDocument("version identity strings must look like collection/key@revision")
        try:
            revision = None if at is None else int(at)
        except (TypeError, ValueError) as exc:
            raise InvalidDocument("invalid revision in version identity") from exc
    else:
        raise InvalidDocument("version identity must be a triple, record, mapping or collection/key@revision")
    try:
        name(collection)
        checked_key(document_key)
    except (InvalidDocument, TypeError) as exc:
        raise InvalidDocument("invalid version identity") from exc
    if revision is not None and (isinstance(revision, bool) or not isinstance(revision, int) or revision < 0):
        raise InvalidDocument("version identity revision must be a nonnegative integer")
    return collection, document_key, revision


def normalize_identity(value):
    """Return a JSON-safe identity mapping without touching storage."""
    collection, document_key, revision = _unpack_identity(value)
    return {"collection": collection, "key": document_key, "revision": revision}


def version_row(connection, identity):
    return connection.execute("SELECT * FROM versions WHERE collection=? AND key=? AND rev=?",
                              identity.triple).fetchone()


def _latest_revision(connection, collection, document_key, head):
    row = connection.execute("SELECT MAX(rev) FROM versions WHERE collection=? AND key=? AND rev<=?",
                             (collection, document_key, head)).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0]


def resolve_identity(connection, value, head, pending_revision=None, pending_keys=None):
    """Resolve caller input to the concrete identity used by storage.

    A two element identity (or a mapping without ``revision``) means "the
    current version of that key": a version written by the batch being
    committed when one exists, otherwise the latest version at ``head``.
    Every returned identity is backed by a physical version row.
    """
    collection, document_key, revision = _unpack_identity(value)
    if revision is None:
        if pending_keys is not None and (collection, document_key) in pending_keys:
            revision = pending_revision
        else:
            revision = _latest_revision(connection, collection, document_key, head)
            if revision is None:
                raise InvalidReference(f"no version of {collection}/{document_key} at or before revision {head}")
    identity = VersionIdentity(collection, document_key, revision)
    if version_row(connection, identity) is None:
        raise InvalidReference(f"version {collection}/{document_key}@{revision} does not exist")
    return identity


def require_live_target(connection, identity):
    if not _live_version(connection, identity):
        raise InvalidReference(
            f"cannot reference deleted version {identity.collection}/{identity.key}@{identity.revision}")


def reference_rows(connection):
    return connection.execute("SELECT * FROM version_refs").fetchall()


def row_source(row):
    if row["source_revision"] is None:
        return None
    return VersionIdentity(row["source_collection"], row["source_key"], row["source_revision"])


def row_target(row):
    return VersionIdentity(row["target_collection"], row["target_key"], row["target_revision"])


def row_reference(row, record=None):
    return Reference(row["name"], row_source(row), row_target(row), row["created_revision"], record)


def _live_version(connection, identity):
    version = version_row(connection, identity)
    return version is not None and version["body"] is not None


def ordinary_readable(connection, identity, floor):
    """True when ordinary reads can still observe this exact version.

    A version is observable when its own revision is at or above the floor
    (ordinary historical reads serve it) or when it is the newest version at
    or below the floor for its key -- the value a read fixed at ``floor`` or a
    plain key read still returns.  Compaction always retains that baseline
    version, even though its revision number is below the floor.
    """
    if not _live_version(connection, identity):
        return False
    if identity.revision >= floor:
        return True
    newest = connection.execute("SELECT MAX(rev) FROM versions WHERE collection=? AND key=? AND rev<=?",
                                (identity.collection, identity.key, floor)).fetchone()[0]
    return newest == identity.revision


def protected_identities(connection, floor):
    """Return ``(protected_version_identities, active_reference_names)``.

    A reference is *active* while the version that carries it can still be
    observed through ordinary reads (see :func:`ordinary_readable`), or when
    it is itself the target of another active reference.  Detached references
    (no source) are always active.

    Active references protect their exact target versions.  References whose
    carrying source history has been reclaimed become *dormant*: they stay
    stored (``get`` reports that their source was reclaimed and ``release``
    can drop them), but they no longer pin content, so abandoned dependencies
    do not keep all history forever.

    Targets are walked as sources of further references, which keeps shared
    targets and arbitrarily deep chains alive.  Every row is visited at most
    once, so cyclic chains terminate.
    """
    references = reference_rows(connection)
    by_source = {}
    roots = []
    for row in references:
        if row["source_revision"] is None:
            roots.append(row)
        elif ordinary_readable(connection, row_source(row), floor):
            roots.append(row)
        else:
            by_source.setdefault((row["source_collection"], row["source_key"], row["source_revision"]), []).append(row)
    protected = set()
    active = set()
    pending = list(roots)
    while pending:
        row = pending.pop()
        if row["name"] in active:
            continue
        active.add(row["name"])
        target = (row["target_collection"], row["target_key"], row["target_revision"])
        protected.add(target)
        pending.extend(by_source.get(target, ()))
    return protected, active


def is_readable(connection, identity, floor, protected=None):
    """A live version is readable if an ordinary read still observes it, or a
    live reference chain reaches it.  Physical presence is required in both
    cases; a deletion tombstone carries no content and never sources a
    reference."""
    if not _live_version(connection, identity):
        return False
    if ordinary_readable(connection, identity, floor):
        return True
    if protected is None:
        protected, _ = protected_identities(connection, floor)
    return identity.triple in protected


def reference_states(connection, floor):
    """Describe every stored reference as active or dormant.

    Returns a list of dicts with the reference name, whether it still protects
    its target, and whether that target version is still physically present.
    """
    protected, active = protected_identities(connection, floor)
    states = []
    for row in reference_rows(connection):
        target = row_target(row)
        states.append({"name": row["name"], "active": row["name"] in active,
                       "target_present": version_row(connection, target) is not None,
                       "target": target.as_dict()})
    return states


class References:
    def __init__(self, store):
        self.store = store

    def create(self, reference, target=None, source=None, *, name=None):
        """Create a persistent fixed-version reference.

        ``reference`` (or the keyword ``name``) is the reference name,
        ``target`` the referenced version identity and ``source`` an optional
        version identity that carries the reference.  Identities accept a
        :class:`VersionIdentity`, :class:`~strataleaf.mvcc.Record`, a mapping,
        a ``(collection, key, revision)`` triple, or a ``collection/key``
        pair naming the current version.
        """
        reference_name = name if name is not None else reference
        if not isinstance(reference_name, str) or not reference_name:
            raise InvalidDocument("reference requires a name")
        checked_key(reference_name)
        if target is None:
            raise InvalidDocument("reference requires a target version")
        with self.store.atomic(writable=True) as connection:
            if connection.execute("SELECT 1 FROM version_refs WHERE name=?", (reference_name,)).fetchone():
                raise Conflict("reference already exists: " + reference_name)
            head, floor = int(self.store.get("head")), int(self.store.get("floor"))
            target_identity = resolve_identity(connection, target, head)
            require_live_target(connection, target_identity)
            source_identity = None
            if source is not None:
                source_identity = resolve_identity(connection, source, head)
                if not is_readable(connection, source_identity, floor):
                    raise InvalidReference("reference source version is no longer readable")
            connection.execute("INSERT INTO version_refs VALUES (?,?,?,?,?,?,?,?)",
                               (reference_name,
                                None if source_identity is None else source_identity.collection,
                                None if source_identity is None else source_identity.key,
                                None if source_identity is None else source_identity.revision,
                                target_identity.collection, target_identity.key, target_identity.revision,
                                head))
            self.store.set("reference_epoch", int(self.store.get("reference_epoch")) + 1)
        return Reference(reference_name, source_identity, target_identity, head)

    def get(self, reference):
        """Resolve a reference to its exact target version.

        This is the only read path that may return a version older than the
        retained floor.  It never changes the ordinary key based read rules.
        A reference whose carrying source version has itself been reclaimed
        raises :class:`InvalidReference`; release it explicitly instead.
        """
        if not isinstance(reference, str) or not reference:
            raise InvalidDocument("reference name must be a nonempty string")
        with self.store.read() as connection:
            row = connection.execute("SELECT * FROM version_refs WHERE name=?", (reference,)).fetchone()
            if row is None:
                raise NotFound("unknown reference: " + reference)
            target = row_target(row)
            version = version_row(connection, target)
            if version is None:
                raise InvalidReference(
                    f"referenced version {target.collection}/{target.key}@{target.revision} was reclaimed")
            source = row_source(row)
            if source is not None:
                floor = int(self.store.get("floor"))
                if not is_readable(connection, source, floor):
                    raise InvalidReference(
                        f"reference {reference} source version was reclaimed; release it explicitly")
            record = None if version["body"] is None else Record(version["collection"], version["key"],
                                                                 version["rev"], loads(version["body"]),
                                                                 version["expires"])
            return row_reference(row, record)

    resolve = get
    fetch = get

    def read(self, reference):
        return self.get(reference).document

    def record(self, reference):
        return self.get(reference).record

    def document(self, reference):
        return self.get(reference).document

    add = create

    def exists(self, reference):
        if not isinstance(reference, str) or not reference:
            raise InvalidDocument("reference name must be a nonempty string")
        with self.store.read() as connection:
            return connection.execute("SELECT 1 FROM version_refs WHERE name=?", (reference,)).fetchone() is not None

    def list(self):
        with self.store.read() as connection:
            return [row_reference(row) for row in reference_rows(connection)]

    def names(self):
        with self.store.read() as connection:
            return [row["name"] for row in connection.execute("SELECT name FROM version_refs ORDER BY name")]

    def release(self, reference):
        """Drop one named dependency.  Shared or chained content stays alive
        as long as another reachable reference still protects it."""
        if not isinstance(reference, str) or not reference:
            raise InvalidDocument("reference name must be a nonempty string")
        with self.store.atomic(writable=True) as connection:
            if not connection.execute("DELETE FROM version_refs WHERE name=?", (reference,)).rowcount:
                raise NotFound("unknown reference: " + reference)
            self.store.set("reference_epoch", int(self.store.get("reference_epoch")) + 1)
        return reference

    remove = release
    delete = release
    drop = release

    def state(self, reference):
        """Report whether one reference still protects its target."""
        with self.store.read() as connection:
            if not connection.execute("SELECT 1 FROM version_refs WHERE name=?", (reference,)).fetchone():
                raise NotFound("unknown reference: " + reference)
            floor = int(self.store.get("floor"))
            for item in reference_states(connection, floor):
                if item["name"] == reference:
                    return item
        raise NotFound("unknown reference: " + reference)

    def states(self):
        with self.store.read() as connection:
            return reference_states(connection, int(self.store.get("floor")))

    def release_dormant(self):
        """Release every reference whose source history has already been
        reclaimed.  These can no longer be resolved and do not protect
        anything, so this only removes abandoned dependency rows."""
        with self.store.atomic(writable=True) as connection:
            floor = int(self.store.get("floor"))
            dormant = [state["name"] for state in reference_states(connection, floor) if not state["active"]]
            for reference_name in dormant:
                connection.execute("DELETE FROM version_refs WHERE name=?", (reference_name,))
            if dormant:
                self.store.set("reference_epoch", int(self.store.get("reference_epoch")) + 1)
        return dormant

    def release_source(self, collection, document_key, revision=None):
        """Release every reference carried by one source version (or by every
        version of a key when ``revision`` is omitted)."""
        name(collection)
        checked_key(document_key)
        if revision is not None and (isinstance(revision, bool) or not isinstance(revision, int) or revision < 0):
            raise InvalidDocument("source revision must be a nonnegative integer")
        with self.store.atomic(writable=True) as connection:
            if revision is None:
                removed = connection.execute("DELETE FROM version_refs WHERE source_collection=? AND source_key=?",
                                             (collection, document_key)).rowcount
            else:
                removed = connection.execute(
                    "DELETE FROM version_refs WHERE source_collection=? AND source_key=? AND source_revision=?",
                    (collection, document_key, revision)).rowcount
            if removed:
                self.store.set("reference_epoch", int(self.store.get("reference_epoch")) + 1)
        return removed

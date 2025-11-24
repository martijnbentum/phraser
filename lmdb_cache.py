import lmdb_helper
import locations
import pickle


class Cache:
    """
    Barebones LMDB-backed store with:
    - safe caching
    - no nested pickles
    - resolver for parent, children, speaker, audio
    """

    def __init__(self, env = None, path = locations.cgn_lmdb ):
        self.env = lmdb_helper.open_lmdb(env, path)
        self._cache = {}      # id:str → object
        self.CLASS_MAP = {}   # filled externally (AudioFile, etc.)

    # ---------------------------------------------------------------
    # Register classes that can be deserialized
    # ---------------------------------------------------------------
    def register(self, cls):
        self.CLASS_MAP[cls.object_type] = cls

    # ---------------------------------------------------------------
    # Save object (no nested pickles)
    # ---------------------------------------------------------------
    def save(self, obj):
        raw = pickle.dumps(obj.to_dict())
        key = obj.identifier.encode("utf-8")

        with self.env.begin(write=True) as txn:
            txn.put(key, raw)

        # Cache the actual Python object
        self._cache[obj.id] = obj

    def load(self, identifier, with_links = False):
        if with_links: return self.load_with_links(identifier)
        if isinstance(identifier, bytes):
            identifier = identifier.decode()

        # Cached?
        if identifier in self._cache:
            return self._cache[identifier]
        object_type = identifier.split(":")[0]
        cls = self.CLASS_MAP[object_type]
        obj = cls(identifier = identifier)
        self._cache[identifier] = obj
        return obj

    def load_with_links(self, identifier):
        obj = self.load(identifier)
        if obj is None:
            return None

        visited = set()
        self._resolve_links(obj, visited)
        return obj

    def _resolve_links(self, obj, visited):
        if obj.identifier in visited:
            return
        visited.add(obj.identifier)
        if getattr(obj, "parent_id", None):
            parent = self.load(obj.parent_id)
            if parent is not None:
                obj.parent = parent
                self._resolve_links(parent, visited)

        if getattr(obj, "child_ids", None):
            for cid in obj.child_ids:
                print(cid)
                child = self.load(cid)
                if child is not None:
                    if child not in obj.children:
                        obj.children.append(child)
                    child.parent = obj
                    self._resolve_links(child, visited)

        if getattr(obj, "speaker_id", None):
            sp = self.load(obj.speaker_id)
            if sp is not None:
                obj.speaker = sp
                if obj.id not in sp.phrase_ids:
                    pass  # speaker-to-segment linkage optional
                self._resolve_links(sp, visited)

        if getattr(obj, "audio_id", None):
            au = self.load(obj.audio_id)
            if au is not None:
                obj.audio = au
                self._resolve_links(au, visited)



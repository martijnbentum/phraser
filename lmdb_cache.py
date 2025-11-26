import lmdb_helper
import lmdb_key
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
        self.CLASS_MAP = {}   # filled externally (Audio, etc.)

    # ---------------------------------------------------------------
    # Register classes that can be deserialized
    # ---------------------------------------------------------------
    def register(self, cls):
        self.CLASS_MAP[cls.__name__] = cls

    # ---------------------------------------------------------------
    # Save object (no nested pickles)
    # ---------------------------------------------------------------
    def save(self, obj, overwrite = False, fail_gracefully = False):
        key = lmdb_key.item_to_key(obj)
        d = obj.to_dict()
        try: 
            lmdb_helper.lmdb_write(
                key = key,
                value = pickle.dumps(d, protocol=pickle.HIGHEST_PROTOCOL),
                env = self.env,
                overwrite = overwrite,
            )
        except KeyError as e:
            if fail_gracefully:
                m = f"Object with key {key} already exists. "
                m += "Skipping save."
                print(m)
            else:
                raise e
        # Cache the actual Python object
        self._cache[key] = obj


    def load(self, key, with_links = False):
        if isinstance(key, bytes):
            key = key.decode()

        # Cached?
        if key in self._cache:
            return self._cache[key]
        object_type = lmdb_key.key_to_object_type(key)
        cls = self.CLASS_MAP[object_type]
        obj = cls(key = key)
        self._cache[key] = obj
        return obj

    def delete(self, key):
        lmdb_helper.lmdb_delete(
            key = key,
            env = self.env,
        )
        if key in self._cache:
            del self._cache[key]

    def update(old_key, obj):
        self.delete(old_key)
        self.save(obj, overwrite=True)




import os
from pathlib import Path

from decouple import AutoConfig

ROOT = Path(__file__).resolve().parents[1]
config = AutoConfig(search_path=ROOT)


def _path_config(name, default):
    value = config(name, default=str(default))
    return Path(value).expanduser()


def _export_prefixed_env(prefix):
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key.startswith(prefix):
            continue
        os.environ.setdefault(key, value.strip().strip("'").strip('"'))


_export_prefixed_env("SSH_AUDIO_PLAY_")

data = _path_config("PHRASER_DATA_DIR", ROOT / "data")
data.mkdir(parents=True, exist_ok=True)
default_lmdb = _path_config("PHRASER_DEFAULT_LMDB", data / "default_lmdb")
cgn_lmdb = _path_config("PHRASER_CGN_LMDB", data / "cgn_lmdb")

audio_filenames = data / 'audio_filenames.txt'

textgrids = data / 'textgrids'
cgn_ort_directory = data / 'ort'

cgn_base = Path('/vol/bigdata/corpora2/CGN2/')
cgn_audio = cgn_base / 'data/audio/wav/'
cgn_speaker_file = cgn_base / 'data/meta/text/speakers.txt'

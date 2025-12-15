import subprocess
import shlex
from decouple import config


def play_audio(path, verbose = False):
    """
    Stream audio from remote server to local machine via reverse SSH
    and play it locally.
    """
    user = config('AUDIO_LOCAL_USER')
    port = config('AUDIO_SSH_PORT', cast=int)
    sox  = config('AUDIO_REMOTE_SOX')
    play = config('AUDIO_LOCAL_PLAY')

    path = shlex.quote(path)

    cmd = (
        f'{sox} {path} -t wav - | '
        f'ssh -p {port} {user}@localhost "{play} -"'
    )

    if not verbose: cmd += ' -q'

    subprocess.Popen(cmd, shell=True)


import secrets
import pyescrypt
from xkcdpass import xkcd_password as xp


def get_mcf_hasher():
    return pyescrypt.Yescrypt(n=4096,
                              r=32,
                              t=0,
                              p=1,
                              mode=pyescrypt.pyescrypt.Mode.MCF)


def hash_yescrypt(hasher: pyescrypt.Yescrypt,
                  string: str):
    return hasher.digest(password=string.encode(),
                         salt=secrets.token_bytes(32))


def generate_password():
    wordfile = xp.locate_wordfile()
    words = xp.generate_wordlist(wordfile=wordfile,
                                 min_length=5)
    return xp.generate_xkcdpassword(words, delimiter='-')


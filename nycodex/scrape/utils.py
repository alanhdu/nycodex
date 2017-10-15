from contextlib import contextmanager
import tempfile
import typing

import requests


@contextmanager
def download_file(url: str, params: typing.Optional[dict] = None) -> str:
    r = requests.get(url, params=params, stream=True)
    with tempfile.NamedTemporaryFile() as fout:
        for chunk in r.iter_content(None):
            if chunk:  # filter out keep-alive chunks
                fout.write(chunk)
        fout.flush()
        yield fout.name

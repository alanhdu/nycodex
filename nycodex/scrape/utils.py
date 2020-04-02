import contextlib
import tempfile
from typing import Any, Dict, Iterator, Optional

import requests

from . import exceptions


@contextlib.contextmanager
def download_file(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    maxsize: int = 256 * 1024 * 1024,
) -> Iterator[str]:
    r = requests.get(url, params=params, stream=True)
    size = 0
    with tempfile.NamedTemporaryFile() as fout:
        # https://github.com/python/typeshed/pull/1784
        for chunk in r.iter_content(None):  # type: ignore
            if chunk:  # filter out keep-alive chunks
                fout.write(chunk)
            size += len(chunk)
            if size > maxsize:
                raise exceptions.SocrataDatasetTooLarge

        fout.flush()
        yield fout.name

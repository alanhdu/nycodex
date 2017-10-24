from contextlib import contextmanager
import tempfile
from typing import Any, Dict, Optional

import requests

from . import exceptions


@contextmanager
def download_file(url: str,
                  params: Optional[Dict[str, Any]] = None,
                  maxsize: Optional[int] = 128 * 1024 * 1024) -> str:
    r = requests.get(url, params=params, stream=True)
    size = 0
    with tempfile.NamedTemporaryFile() as fout:
        for chunk in r.iter_content(None):
            if chunk:  # filter out keep-alive chunks
                fout.write(chunk)
            size += len(chunk)
            if size > maxsize:
                raise exceptions.SocrataDatasetTooLarge

        fout.flush()
        yield fout.name

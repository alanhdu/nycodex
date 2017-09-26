# Contributing to NYC Open Data Explorer

## Environment Setup

1. Install [PostgreSQL](https://www.postgresql.org/download/). We are
   tentatively using PostgreSQL 9.5, although 9.6 and 10 should also work.
2. Set up your Python environment:
    1. Install [Python 3.6](https://www.python.org/downloads/)
    2. `pip3 install pipenv`
    3. `pipenv install --dev`
3. Set up the Postgresql database:
    1. `echo "DATABASE_URI=postgresql://adi:password@localhost/nycodex" > .env`
    2. `./scripts/bootstrap.sh`
    3. `pipenv run python scripts/socrata.py`


## Dependencies

We use the wonderful [Pipenv](https://pipenv.readthedocs.io/en/latest/)
file to manage our dependencies. Almost all of your developer actions
will be mediated via `pipenv` (either though `pipenv run` or `pipenv
shell`).

## Style Guide

We use modern Python 3 and follow standard [PEP
8](https://www.python.org/dev/peps/pep-0008/) style guides.  For
docstrings, please use [Google-style
docstrings](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
with Python 3 type annotations.

Please use Python 3 type annotations for all functions and class
variables.  In the future, we will also integrate
[mypy](http://mypy-lang.org/) to help us type-check our code.

## Tests

Our Python style guide is enforced with `flake8` and our test suite is
run with `pytest`. To run our test suite, run:

```bash
pipenv run flake8
pipenv run pytest
```

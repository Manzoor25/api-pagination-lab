# macOS / Linux entry point. On Windows use run.ps1.
PY ?= python3
VENV := .venv
BIN := $(VENV)/bin
PYTHON := $(BIN)/python

.PHONY: setup api demo benchmark test real site

setup:
	$(PY) -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

api:
	$(PYTHON) -m mock_api.server

demo:
	$(PYTHON) demo.py

benchmark:
	$(PYTHON) benchmark.py --mode both

test:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m pytest

real:
	$(PYTHON) diagnose.py --preset fdic --samples 3 --page-size 1000
	$(PYTHON) fetch.py --adapter fdic --strategy offset --page-size 10000 --rate-limit-rps 2
	$(PYTHON) -c "import pathlib,subprocess,sys; runs=sorted(pathlib.Path('data/landing/fdic-offset').glob('*')); subprocess.check_call([sys.executable,'validate.py','--run-dir',str(runs[-1]),'--no-dense-ids'])"

site:
	$(PYTHON) -m http.server 8080 --directory site

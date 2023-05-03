VENV_NAME ?= venv
VENV_ACTIVATE ?=. ${CURDIR}/$(VENV_NAME)/bin/activate
PYTHON_VERSION ?= python3.10
PYTHON ?= $(CURDIR)/$(VENV_NAME)/bin/$(PYTHON_VERSION)
REQUIREMENTS ?= requirements.txt


.PHONY: run
run:
	$(PYTHON) src/main.py hydra.run.dir=/tmp/ hydra/job_logging=none hydra/hydra_logging=none hydra.output_subdir=null

.PHONY: create_venv
create_venv:
	test -d $(VENV_NAME) || virtualenv -p $(PYTHON_VERSION) $(VENV_NAME)
	$(VENV_ACTIVATE);  pip install -Ur requirements.txt

.PHONY: ipython
ipython:
	$(VENV_NAME)/bin/ipython

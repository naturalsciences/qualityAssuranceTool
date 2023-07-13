VENV_NAME ?= venv
VENV_ACTIVATE ?=. ${CURDIR}/$(VENV_NAME)/bin/activate
PYTHON_VERSION ?= python3.11
PYTHON ?= $(CURDIR)/$(VENV_NAME)/bin/$(PYTHON_VERSION)
REQUIREMENTS ?= requirements.txt


.PHONY: run
run:
	$(PYTHON) src/main.py
.PHONY: create_venv
create_venv:
	test -d $(VENV_NAME) || virtualenv -p $(PYTHON_VERSION) $(VENV_NAME)
	$(VENV_ACTIVATE);  pip install -Ur requirements.txt
	echo $(CURDIR)/src/ >> $(VENV_NAME)/lib/$(PYTHON_VERSION)/site-packages/own.pth
	echo $(CURDIR)/tests/ >> $(VENV_NAME)/lib/$(PYTHON_VERSION)/site-packages/own.pth

.PHONY: ipython
ipython:
	$(VENV_NAME)/bin/ipython

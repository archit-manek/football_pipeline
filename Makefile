PY=python
PIP_COMPILE=pip-compile --resolver=backtracking
PIP_SYNC=pip-sync

REQ_IN=requirements.in
REQ_TXT=requirements.txt

.PHONY: deps deps-add deps-rm deps-up

# First-time setup (or on a new machine): compile + install everything
deps:
	$(PY) -m pip install -U "pip<25.1" "pip-tools>=7.4,<7.5"
	$(PIP_COMPILE) -o $(REQ_TXT) $(REQ_IN)
	$(PIP_SYNC) $(REQ_TXT)

# Add a new dependency without editing files
# Usage: make deps-add PKG="socceraction[statsbomb]"
deps-add:
	@grep -qE "^$(PKG)([=<>!]|$$)" $(REQ_IN) || echo "$(PKG)" >> $(REQ_IN)
	$(PIP_COMPILE) -o $(REQ_TXT) $(REQ_IN)
	$(PIP_SYNC) $(REQ_TXT)

# Remove a dependency
# Usage: make deps-rm PKG="seaborn"
deps-rm:
	@sed -i.bak "/^$(PKG)\(\\b\|[=<>!]\)/d" $(REQ_IN) && rm -f $(REQ_IN).bak
	$(PIP_COMPILE) -o $(REQ_TXT) $(REQ_IN)
	$(PIP_SYNC) $(REQ_TXT)

# Upgrade all pinned versions (keeping your top-level choices)
deps-up:
	$(PIP_COMPILE) -U -o $(REQ_TXT) $(REQ_IN)
	$(PIP_SYNC) $(REQ_TXT)
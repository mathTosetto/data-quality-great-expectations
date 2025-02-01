.PHONY: install activate test

install:
	poetry install --no-root

activate:
	eval $(poetry env activate)

test:
	pytest -W ignore tests/
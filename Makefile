.PHONY: install activate test open-report

install:
	poetry install

activate:
	eval $(poetry env activate)


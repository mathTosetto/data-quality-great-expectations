.PHONY: install activate test up down

install:
	echo "Definining the poetry configs..."
	poetry config virtualenvs.in-project true

	echo "Installing the dependencies..."
	poetry install --quiet

	echo "Installing pre-commit..."
	pre-commit install

activate:
	eval $(poetry env activate)

test:
	pytest -W ignore --cov=tests/

up:
	@if [ ! -f .env ]; then \
		echo "WARNING: .env file does not exist! Creating the '.env'. Please configure it according to the README.md"; \
		touch .env; \
        exit 1; \
	fi
	docker build . --tag extending_airflow:latest
	docker compose up -d;

down:
	docker compose down -v
	@if [[ "$(docker ps -q -f name=${DOCKER_CONTAINER})" ]]; then \
		echo "Terminating running container..."; \
		docker rm ${DOCKER_CONTAINER}; \
	fi

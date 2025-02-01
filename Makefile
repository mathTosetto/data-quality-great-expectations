.PHONY: install activate test up down

install:
	poetry install --no-root

activate:
	eval $(poetry env activate)

test:
	pytest -W ignore tests/

up:
	@if [ ! -f .env ]; then \
		echo "WARNING: .env file does not exist! 'example.env' copied to '.env'. Please update the configurations in the .env file running this target."; \
		cp example.env .env; \
        exit 1; \
	fi
	docker compose up -d;

down:
	docker compose down -v
	@if [[ "$(docker ps -q -f name=${DOCKER_CONTAINER})" ]]; then \
		echo "Terminating running container..."; \
		docker rm ${DOCKER_CONTAINER}; \
	fi

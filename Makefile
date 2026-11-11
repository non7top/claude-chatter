DOCKER_UID := $(shell id -u)
DOCKER_GID := $(shell id -g)
export DOCKER_UID
export DOCKER_GID

.PHONY: build test lint package clean stop destroy

build:
	docker compose build

test: build
	docker compose run --rm test python -m unittest discover -s tests -p test_suite.py

lint: build
	docker compose run --rm test flake8 claude_chatter/ tests/

package: build
	docker compose run --rm test python -m build

clean:
	rm -rf dist/ build/ *.egg-info claude_chatter/*.egg-info

stop:
	docker compose stop

destroy:
	docker compose down -v --rmi local

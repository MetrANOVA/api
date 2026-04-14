run-api:
	uv run fastapi dev

setup:
	git submodule update --init --recursive

reload-pipeline:
	docker rm -f pipeline && docker compose -p api_devcontainer -f .devcontainer/docker-compose.yml up -d --no-deps --build pipeline

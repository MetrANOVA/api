run-api:
	uv run fastapi dev

stop-simulators:
	docker stop telegraf snmp-simulator

inject-message:
	python bin/inject-kafka-message.py $(MSG)

import-ch-dump:
	docker exec -i clickhouse clickhouse-client --multiquery < $(DUMP)

setup:
	git submodule update --init --recursive
	uv sync --frozen --all-groups

reload-pipeline:
	docker rm -f pipeline && docker compose -p api_devcontainer -f .devcontainer/docker-compose.yml up -d --no-deps --build pipeline

test:
	uv run python bin/run-tests.py $(ARGS)

rm-volumes:
	docker volume rm api_devcontainer_clickhouse_data api_devcontainer_kafka_data api_devcontainer_grafana_data

DEVCONTAINER := devcontainer --workspace-folder .
COMPOSE := docker compose -p api_devcontainer -f .devcontainer/docker-compose.yml

.PHONY: dc-up dc-rebuild dc-down dc-shell dc-api dc-bootstrap dc-api-ip

# Start the devcontainer stack (reuses existing containers/images)
dc-up:
	$(DEVCONTAINER) up

# Recreate everything from scratch, e.g. after bumping the pipeline submodule
dc-rebuild:
	$(DEVCONTAINER) up --remove-existing-container --build-no-cache

# Stop and remove the stack (keeps volumes; use `make rm-volumes` to wipe data)
dc-down:
	$(COMPOSE) down --remove-orphans

# Shell inside the api container
dc-shell:
	$(DEVCONTAINER) exec bash

# Run the admin API inside the container (foreground)
dc-api:
	$(DEVCONTAINER) exec python bin/start-admin-api.py

# Load type/transformer definitions into the running admin API
dc-bootstrap:
	$(DEVCONTAINER) exec python bin/bootstrap.py --from bootstrap --api-url http://localhost:8000

# Print the api container's IP (port 8000 isn't published to the host)
dc-api-ip:
	@docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
		$$(docker ps -qf label=com.docker.compose.project=api_devcontainer -f label=com.docker.compose.service=api)

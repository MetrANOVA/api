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

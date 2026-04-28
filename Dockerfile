FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

RUN apt-get update \
	&& apt-get install --no-install-recommends -y build-essential \
	&& rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
COPY packages ./packages
COPY src ./src
COPY bin ./bin

RUN uv sync --frozen --all-packages

CMD ["admin_api"]
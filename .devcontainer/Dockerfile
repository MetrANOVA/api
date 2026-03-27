FROM python:3.14-rc-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY .python-version pyproject.toml uv.lock ./

RUN pip install --upgrade pip && pip install uv
RUN echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc
RUN uv sync

COPY . .

CMD ["bash"]

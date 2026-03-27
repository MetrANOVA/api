FROM mcr.microsoft.com/devcontainers/python:3-3.14-trixie

WORKDIR /app

RUN pip install --upgrade pip && pip install uv

COPY .python-version pyproject.toml uv.lock ./
RUN echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc

RUN uv sync

COPY . .

CMD ["bash"]

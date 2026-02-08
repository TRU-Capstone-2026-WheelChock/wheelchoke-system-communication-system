FROM mcr.microsoft.com/devcontainers/python:3.12

# This is for development & test only. 

# 1. Environmental variable
ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1
ENV PATH="$POETRY_HOME/bin:$PATH"

# 2. Poetry install
RUN curl -sSL https://install.python-poetry.org | python3 -


WORKDIR /workspaces/app

COPY pyproject.toml poetry.lock* ./

RUN poetry install --no-root

RUN chown -R vscode:vscode /workspaces/app
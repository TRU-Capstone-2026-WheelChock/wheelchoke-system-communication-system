FROM mcr.microsoft.com/devcontainers/python:3.12

# 1. 環境変数の設定 (venvをプロジェクト内に作らせない設定にするのがコツ)
ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1
ENV PATH="$POETRY_HOME/bin:$PATH"

# 2. Poetryのインストール
RUN curl -sSL https://install.python-poetry.org | python3 -

# 3. 作業ディレクトリの設定
WORKDIR /workspaces/app

# 4. 【重要】依存関係ファイルだけ先にコピーする
# (ソースコードが変わっても、ここが変わらなければキャッシュが効く)
COPY pyproject.toml poetry.lock* ./

# 5. インストール実行
# --no-root: パッケージ本体はまだインストールしない（まだCOPYしてないから）
RUN poetry install --no-root

# 6. ディレクトリの所有権をvscodeユーザーに変更
# (これがないと、あとで書き込めなくてエラーになる)
RUN chown -R vscode:vscode /workspaces/app
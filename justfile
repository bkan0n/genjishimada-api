lint:
    ruff format .
    ruff check .
    basedpyright

run:
    litestar run --reload --host 0.0.0.0 --debug

updatesdk:
    uv remove genjipk-sdk
    uv add "genjipk-sdk @ git+https://github.com/bkan0n/genjipk-sdk"

test:
    pytest -n 8 .

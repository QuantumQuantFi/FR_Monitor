# Repository Guidelines

## Project Structure & Module Organization
- core: `exchange_connectors.py` (WebSocket collectors), `market_info.py` (dynamic symbol discovery), `config.py` (runtime config), `database.py` (SQLite storage/aggregation).
- web: `simple_app.py` (Flask server, port 4002), `app.py` (Socket.IO variant, port 5000), `templates/` (HTML views).
- tests/tools: `test_*.py` scripts (integration/load checks), `verify_config.py` (sanity checks), cache file `dynamic_symbols_cache.json`, DB `market_data.db`.

## Build, Test, and Development Commands
- install: `pip install -r requirements.txt` — install runtime deps.
- run (Flask): `python simple_app.py` — starts HTTP server at `:4002`.
- run (Socket.IO): `python app.py` — alternative server at `:5000`.
- config check: `python verify_config.py` — validates symbol lists and capacity hints.
- market integration: `python test_market_integration.py` — async discovery/coverage report.
- websocket limits: `python test_websocket_limits.py binance|okx|bybit|bitget` — probe exchange limits.

## Coding Style & Naming Conventions
- language: Python 3.8+; 4‑space indentation; UTF‑8.
- naming: modules/functions `snake_case`; classes `PascalCase`; constants `UPPER_SNAKE_CASE` (see `config.py`).
- imports: standard → third‑party → local, grouped with a blank line.
- formatting/lint: follow PEP 8; keep functions focused; prefer type hints in new code.

## Testing Guidelines
- framework: repository uses runnable scripts, not pytest. Prefer small, focused checks.
- naming: place ad‑hoc tests as `test_*.py` in project root.
- running: execute directly, e.g. `python test_market_integration.py`.
- coverage: no formal threshold; for features touching data flow, include a quick script or log output demonstrating correctness.

## Commit & Pull Request Guidelines
- commits: imperative, concise subject (<=72 chars), e.g. “Optimize OKX reconnect backoff” or “Refactor dynamic symbol merge”. Bilingual messages acceptable; prefer English for code changes.
- scope: group related changes; avoid formatting‑only noise in functional commits.
- PRs: include summary, motivation, before/after behavior, test evidence (logs/screenshots of pages under `templates/`), and any config changes (`config.py`). Link issues when applicable.

## Security & Configuration Tips
- rate limits: tune `WS_UPDATE_INTERVAL`, backoff in `WS_CONNECTION_CONFIG` to avoid bans.
- data: SQLite file `market_data.db` is created locally; don’t commit large DBs.
- cache: `dynamic_symbols_cache.json` refreshes hourly; delete to force rebuild or call the refresh endpoints/tests.

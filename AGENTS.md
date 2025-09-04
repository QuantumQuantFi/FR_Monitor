# Repository Guidelines

## Project Structure & Module Organization
- core: `exchange_connectors.py` (WebSocket collectors), `market_info.py` (dynamic symbol discovery), `config.py` (runtime config), `database.py` (SQLite storage/aggregation), `exchange_availability.py` (exchange reachability).
- web: `simple_app.py` (Flask server at :4002), `templates/` (HTML views: `simple_index.html`, `enhanced_aggregated.html`, `chart_index.html`).
- tests/tools: `test_*.py` scripts (`test_market_integration.py`, `test_websocket_limits.py`, `test_rest_apis.py`, `test_multi_symbols.py`, `simple_test.py`), `verify_config.py` (sanity checks), `scripts/` (ops helpers), cache `dynamic_symbols_cache.json`.
- data: SQLite DB `market_data.db` is created locally at runtime; it is not committed.

## Build, Test, and Development Commands
- install: `pip install -r requirements.txt` — install runtime dependencies.
- run server: `python simple_app.py` — starts Flask on port 4002.
- config check: `python verify_config.py` — validates symbol lists and capacity hints.
- market integration: `python test_market_integration.py` — async discovery/coverage report.
- websocket limits: `python test_websocket_limits.py binance|okx|bybit|bitget` — probe exchange stream limits.
- REST checks: `python test_rest_apis.py` — quick HTTP/REST sanity tests.

## Coding Style & Naming Conventions
- language: Python 3.8+; 4-space indentation; UTF‑8.
- naming: modules/functions `snake_case`; classes `PascalCase`; constants `UPPER_SNAKE_CASE` (see `config.py`).
- imports: standard → third‑party → local, group blocks with one blank line.
- style: follow PEP 8; keep functions focused; prefer type hints in new code.

## Testing Guidelines
- approach: runnable scripts, not pytest; keep tests small and targeted.
- naming: place ad‑hoc tests in repo root as `test_*.py`.
- run: execute directly, e.g., `python test_market_integration.py`.
- evidence: attach short logs or screenshots for templates under `templates/` when relevant.

## Commit & Pull Request Guidelines
- commits: imperative, concise subject (<=72 chars), e.g., “Optimize OKX reconnect backoff”.
- scope: group related changes; avoid formatting‑only churn in functional commits.
- PRs: include summary, motivation, before/after behavior, test evidence (logs/screenshots), and any config updates (`config.py`). Link issues when applicable.

## Security & Configuration Tips
- rate limits: tune `WS_UPDATE_INTERVAL` and `WS_CONNECTION_CONFIG` backoff to avoid bans.
- data: do not commit large SQLite DBs; they are generated locally.
- cache: `dynamic_symbols_cache.json` refreshes hourly; delete to force rebuild or use refresh tests/endpoints.

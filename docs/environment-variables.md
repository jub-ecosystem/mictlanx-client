# Environment Variables

All variables are optional. Each has a sensible default so the client works out of the box.

---

## SDK runtime

These variables are read by the `mictlanx` package itself and affect every program that imports it.
All boolean variables accept `1`, `true`, or `yes` (case-insensitive). Level variables accept `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.

| Variable | Default | Configures |
|---|---|---|
| `MICTLANX_LOG_PATH` | `.mictlanx/log` | Log directory for all rotating files. Created automatically if absent. |
| `MICTLANX_LOG_DISABLED` | `0` | Set to `1` to suppress all log output globally. Maps to the `disabled` / `enable_logging` constructor parameter. |
| `MICTLANX_LOG_LEVEL` | `DEBUG` | Minimum level written to all handlers. Maps to the `log_level` constructor parameter. |
| `MICTLANX_LOG_RICH` | `0` | Set to `1` to use `RichHandler` for syntax-coloured console output. Requires `pip install mictlanx[rich]`. Maps to the `use_rich` constructor parameter. |
| `MICTLANX_LOG_JSON_INDENT` | `0` | Console JSON indentation. `0` = compact single-line; any positive integer = pretty-printed with that many spaces. |
| `MICTLANX_LOG_TO_FILE` | `1` | Set to `0` to disable the rotating `.log` file entirely. |
| `MICTLANX_LOG_ERROR_FILE` | `0` | Set to `1` to write a separate `.error.log` file (ERROR and CRITICAL only). |
| `MICTLANX_LOG_ROTATION_WHEN` | `m` | Rotation time unit passed to `TimedRotatingFileHandler` (`s`, `m`, `h`, `d`). |
| `MICTLANX_LOG_ROTATION_INTERVAL` | `10` | Rotation interval (integer, interpreted in units of `MICTLANX_LOG_ROTATION_WHEN`). |
| `MICTLANX_LOG_CONSOLE_LEVEL` | `DEBUG` | Minimum level for the console handler. |
| `MICTLANX_LOG_FILE_LEVEL` | `DEBUG` | Minimum level for the main rotating file handler. |

```bash
export MICTLANX_LOG_PATH=/var/log/mictlanx
export MICTLANX_LOG_DISABLED=1
export MICTLANX_LOG_RICH=1
export MICTLANX_LOG_LEVEL=INFO
export MICTLANX_LOG_JSON_INDENT=4
```

> **Note:** `MICTLANX_LOG_DISABLED` and `MICTLANX_LOG_RICH` replace the old `MICTLANX_DISABLE_LOGGING` and `MICTLANX_USE_RICH_LOGGER` names. The old names are no longer read.

> **Note:** `mictlanx/asyncx/utils.py` reads `MICTLANX_LOG_DISABLED` at import time (a known limitation). Load your `.env` file with `dotenv.load_dotenv()` *before* importing `mictlanx` to ensure the variable is visible. All other variables are resolved at `Log` / `AsyncClient` construction time.

---

## Integration tests

These variables are only used by the test suite. Copy `.env.test.example` to `.env.test` and adjust the values for your local VSS.

| Variable | Default | Description |
|---|---|---|
| `MICTLANX_ENV_FILE` | `.env.test` | Path to the dotenv file loaded before the test session. Set this if your env file lives somewhere other than the project root. |
| `MICTLANX_URI` | `mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0` | Full router URI used by `AsyncClient` in tests. |
| `CLIENT_ID` | `client-0` | Client identity string passed to `AsyncClient`. |
| `MICTLANX_LOG_PATH` | `/mictlanx/client` | Log directory (same as the runtime variable above). |
| `MICTLANX_ROUTER_PORT` | `60666` | Port used when constructing `AsyncRouter` directly in router tests. |
| `MICTLANX_TEST_PEER_PORT` | `25000` | Port used when connecting directly to a single peer in peer tests. |
| `MICTLANX_TEST_BUCKET` | `pytest-bucket` | Bucket name used by peer-level tests. |
| `MICTLANX_SUMMONER_PORT` | `15000` | Port for the Summoner service (reserved, not yet active). |

Minimal `.env.test` for a local single-router VSS:

```bash
MICTLANX_URI=mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0
CLIENT_ID=client-0
MICTLANX_ROUTER_PORT=60666
MICTLANX_TEST_PEER_PORT=25000
```

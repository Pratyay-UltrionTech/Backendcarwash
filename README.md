# Car Wash API (Backend)

FastAPI service for the car wash application. It uses PostgreSQL (including Azure Database for PostgreSQL) and JWT-based auth.

## Requirements

- Python 3.11+ (recommended)
- PostgreSQL reachable from where the API runs

## Local setup

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

2. Copy environment variables (do not commit real secrets):

Create a `.env` file in this folder. See **Configuration** below for variable names. For local development you can mirror production-like values against a dev database.

3. Run the API:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

- API: `http://localhost:8000`
- OpenAPI docs: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`

## Configuration

The app reads settings from **environment variables** (and optionally a local `.env` file for development only). In Azure, use **Application settings** (App Service) or **environment variables** (Container Apps / AKS); do not upload `.env` to the repo.

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Recommended on Azure | SQLAlchemy URL, e.g. `postgresql+psycopg2://USER:URL_ENCODED_PASSWORD@HOST:5432/DB?sslmode=require`. Special characters in the password must be URL-encoded (e.g. `@` → `%40`). If set, it overrides the individual `POSTGRES_*` fields below. |
| `POSTGRES_HOST` | If no `DATABASE_URL` | Database hostname |
| `POSTGRES_PORT` | No | Default `5432` |
| `POSTGRES_USER` | If no `DATABASE_URL` | Database user |
| `POSTGRES_PASSWORD` | If no `DATABASE_URL` | Database password |
| `POSTGRES_DB` | If no `DATABASE_URL` | Database name |
| `POSTGRES_SSLMODE` | No | Use `require` for Azure PostgreSQL if not already in the URL. For `*.database.azure.com` hosts the app defaults to `require` when unset. |
| `ADMIN_ID` or `ADMIN_USERNAME` | Yes | Bootstrap admin login identifier (email or username) |
| `ADMIN_PASSWORD` | Yes | Admin password |
| `JWT_SECRET_KEY` | **Yes in production** | Long random secret for signing access tokens. Do not use the dev default in production. |
| `JWT_ALGORITHM` | No | Default `HS256` |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | No | Default `1440` (24h) unless overridden |
| `CORS_ORIGINS` | No | Comma-separated list of allowed origins (e.g. `http://localhost:5173,http://localhost:5174`). |
| `CORS_ALLOW_LOCALHOST_REGEX` | No | Default `true`: allows any `http(s)://localhost` or `127.0.0.1` with any port, which suits **local UIs on different machines** as long as the browser origin is localhost. |
| `LOG_LEVEL` | No | e.g. `INFO`, `DEBUG` |

### CORS when the UI runs only on developers’ machines

If each machine runs the Vite (or similar) app on **localhost** with any port, the default `CORS_ALLOW_LOCALHOST_REGEX=true` is usually enough: the browser sends `Origin: http://localhost:<port>`, which is allowed.

If anyone opens the UI via a **LAN hostname or IP** (for example `http://192.168.1.10:5173`), add that exact origin to `CORS_ORIGINS` in Azure (comma-separated).

## Pushing this folder to GitHub

If this repository should contain **only** the backend (e.g. `Backendcarwash`):

```bash
cd path\to\CARWASH\backend
git init
git add .
git commit -m "Initial commit: Car Wash API"
git branch -M main
git remote add origin https://github.com/Pratyay-UltrionTech/Backendcarwash.git
git push -u origin main
```

Ensure `.env` is **not** tracked (it is listed in `.gitignore`). Rotate any credentials that were ever committed or shared.

## Azure: what to put in environment variables

Configure the same names as in the table above in your Azure host:

- **Azure App Service**: Portal → your Web App → **Settings** → **Environment variables** → **App settings** (add each name/value; mark secrets as **Deployment slot setting** / Key Vault references if you use them).
- **Azure Container Apps**: **Containers** → your container → **Environment variables**.

Minimum recommended set for production:

1. **`DATABASE_URL`** (or the full set of `POSTGRES_*` values).
2. **`POSTGRES_SSLMODE`** = `require` if it is not already part of the URL.
3. **`ADMIN_ID`** (or `ADMIN_USERNAME`) and **`ADMIN_PASSWORD`**.
4. **`JWT_SECRET_KEY`** — strong random string (32+ bytes).
5. **`JWT_ALGORITHM`** — `HS256` unless you change the app.
6. **`ACCESS_TOKEN_EXPIRE_MINUTES`** — as needed.
7. **`CORS_ORIGINS`** — only if you need explicit origins in addition to localhost; optional if all clients use localhost and regex stays enabled.
8. **`CORS_ALLOW_LOCALHOST_REGEX`** — `true` for local UIs on various PCs hitting the cloud API.
9. **`LOG_LEVEL`** — `INFO` in production.

**Networking:** Allow the Azure compute resource (App Service outbound IPs or Container Apps environment) to reach PostgreSQL (firewall rules, VNet integration, or private endpoint as you design).

**Startup command** (App Service example): `uvicorn app.main:app --host 0.0.0.0 --port 8000` (or set `WEBSITES_PORT` / platform port to match your chosen port).

For sensitive values, prefer [Azure Key Vault references](https://learn.microsoft.com/azure/app-service/app-service-key-vault-references) in App Service app settings instead of plain text when possible.

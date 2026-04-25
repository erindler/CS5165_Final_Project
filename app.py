import os
import hmac
import secrets
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg
from psycopg.rows import dict_row
from flask import Flask, redirect, render_template, request, session, url_for


app = Flask(__name__)
is_app_service = bool(os.environ.get("WEBSITE_HOSTNAME"))
configured_secret = os.environ.get("FLASK_SECRET_KEY")
if configured_secret:
    app.secret_key = configured_secret
elif is_app_service:
    # Keep production deploys safe even if FLASK_SECRET_KEY was not configured yet.
    app.secret_key = secrets.token_hex(32)
else:
    app.secret_key = "dev-secret-change-me"

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=is_app_service,
)

# For this assignment starter, credentials are intentionally simple.
APP_USERNAME = os.environ.get("APP_USERNAME", "admin")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "password123")
APP_EMAIL = os.environ.get("APP_EMAIL", "admin@example.com")


def _normalize_conninfo_key(raw_key: str) -> str:
    return raw_key.lower().replace(" ", "").replace("_", "")


def _parse_semicolon_connstr(conn_str: str) -> dict[str, Any]:
    parts = [segment.strip() for segment in conn_str.split(";") if segment.strip()]
    values: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        values[_normalize_conninfo_key(key)] = value.strip()

    host = values.get("host") or values.get("server")
    dbname = values.get("database") or values.get("dbname")
    user = values.get("username") or values.get("user") or values.get("uid")
    password = values.get("password") or values.get("pwd")
    port = values.get("port", "5432")
    sslmode = values.get("sslmode") or values.get("ssl") or "require"

    if host and dbname and user and password:
        return {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port,
            "sslmode": sslmode,
        }

    return {}


def _connect_kwargs_from_url(database_url: str) -> dict[str, Any]:
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        return {"conninfo": database_url}

    query_params = parse_qs(parsed.query)
    sslmode = query_params.get("sslmode", ["require"])[0]

    return {
        "host": parsed.hostname,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
        "port": parsed.port or 5432,
        "sslmode": sslmode,
    }


def get_db_connect_kwargs() -> dict[str, Any]:
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return _connect_kwargs_from_url(database_url)

    for key, value in os.environ.items():
        if key.startswith("POSTGRESQLCONNSTR_") and value:
            parsed = _parse_semicolon_connstr(value)
            if parsed:
                return parsed

    host = os.environ.get("PGHOST")
    dbname = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    password = os.environ.get("PGPASSWORD")
    port = os.environ.get("PGPORT", "5432")
    sslmode = os.environ.get("PGSSLMODE", "require")

    if host and dbname and user and password:
        return {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port,
            "sslmode": sslmode,
        }

    host = os.environ.get("POSTGRES_HOST")
    dbname = os.environ.get("POSTGRES_DB")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    port = os.environ.get("POSTGRES_PORT", "5432")
    sslmode = os.environ.get("POSTGRES_SSLMODE", "require")

    if host and dbname and user and password:
        return {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port,
            "sslmode": sslmode,
        }

    present_keys = [
        key
        for key in (
            "DATABASE_URL",
            "PGHOST",
            "PGDATABASE",
            "PGUSER",
            "PGPASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
        )
        if os.environ.get(key)
    ]
    present = ", ".join(present_keys) if present_keys else "none"
    raise RuntimeError(
        "PostgreSQL connection settings are not configured. "
        "Set DATABASE_URL, or PGHOST/PGDATABASE/PGUSER/PGPASSWORD, "
        "or POSTGRES_HOST/POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD. "
        f"Present keys: {present}."
    )


def fetch_data_pulls(hshd_num: str) -> list[dict[str, Any]]:
    query = """
        SELECT
            t.hshd_num,
            t.basket_num,
            t.product_num,
            COALESCE(p.department, '') AS department,
            COALESCE(p.commodity, '') AS commodity,
            t.spend,
            t.units,
            COALESCE(t.store_r, '') AS store_region,
            t.week_num,
            t.year,
            COALESCE(h.loyalty_flag, '') AS loyalty_flag,
            COALESCE(h.age_range, '') AS age_range,
            COALESCE(h.marital, '') AS marital_status,
            COALESCE(h.income_range, '') AS income_range,
            COALESCE(h.homeowner, '') AS homeowner_desc,
            COALESCE(h.hh_size, '') AS hshd_size,
            COALESCE(h.children, '') AS children
        FROM retail.transactions t
        LEFT JOIN retail.products p ON p.product_num = t.product_num
        LEFT JOIN retail.households h ON h.hshd_num = t.hshd_num
        WHERE t.hshd_num = %(hshd_num)s
        ORDER BY
            t.hshd_num,
            t.basket_num,
            t.purchase_date,
            t.product_num,
            p.department,
            p.commodity
    """

    with psycopg.connect(**get_db_connect_kwargs(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"hshd_num": hshd_num})
            db_rows = cur.fetchall()

    return [
        {
            "Hshd_num": row["hshd_num"],
            "Basket_num": row["basket_num"],
            "Product_num": row["product_num"],
            "Department": row["department"],
            "Commodity": row["commodity"],
            "Spend": row["spend"],
            "Units": row["units"],
            "Store_region": row["store_region"],
            "Week_num": row["week_num"],
            "Year": row["year"],
            "Loyalty_flag": row["loyalty_flag"],
            "Age_range": row["age_range"],
            "Marital_status": row["marital_status"],
            "Income_range": row["income_range"],
            "Homeowner_desc": row["homeowner_desc"],
            "Hshd_size": row["hshd_size"],
            "Children": row["children"],
        }
        for row in db_rows
    ]


def get_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def is_valid_csrf(form_token: str) -> bool:
    session_token = session.get("csrf_token", "")
    return bool(form_token) and bool(session_token) and hmac.compare_digest(form_token, session_token)


@app.before_request
def enforce_https_on_app_service():
    # On Azure App Service, redirect plain HTTP requests to HTTPS.
    if not os.environ.get("WEBSITE_HOSTNAME"):
        return None

    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    if proto != "https":
        return redirect(request.url.replace("http://", "https://", 1), code=301)

    return None


@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers[
        "Content-Security-Policy"
    ] = "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:"

    if is_app_service:
        response.headers["Strict-Transport-Security"] = "max-age=31536000"

    return response


@app.route("/")
def index():
    if session.get("logged_in"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    form_data = {"username": "", "email": ""}

    if request.method == "POST":
        if not is_valid_csrf(request.form.get("csrf_token", "")):
            error = "Your session expired. Please refresh and try again."
            return render_template("login.html", error=error, form_data=form_data, csrf_token=get_csrf_token()), 400

        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        email = request.form.get("email", "").strip().lower()
        form_data = {"username": username, "email": email}

        if username == APP_USERNAME and password == APP_PASSWORD and email == APP_EMAIL.lower():
            session["logged_in"] = True
            session["username"] = username
            session["email"] = email
            return redirect(url_for("dashboard"))

        error = "Invalid username, password, or email"

    return render_template("login.html", error=error, form_data=form_data, csrf_token=get_csrf_token())


@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template(
        "dashboard.html",
        username=session.get("username", "User"),
        email=session.get("email", ""),
        csrf_token=get_csrf_token(),
    )


@app.route("/data-pulls")
def data_pulls():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    hshd_num = request.args.get("hshd_num", "").strip()
    rows: list[dict[str, Any]] = []
    error = None

    if hshd_num:
        if not hshd_num.isdigit():
            error = "Please enter a numeric hshd_num value."
        else:
            try:
                rows = fetch_data_pulls(hshd_num)
            except RuntimeError as exc:
                error = str(exc)
            except psycopg.Error as exc:
                error = f"Unable to query PostgreSQL right now: {exc.__class__.__name__}."
            except Exception:
                error = "Unable to load data pulls right now. Please try again shortly."

    return render_template(
        "data_pulls.html",
        username=session.get("username", "User"),
        email=session.get("email", ""),
        csrf_token=get_csrf_token(),
        hshd_num=hshd_num,
        rows=rows,
        error=error,
    )


@app.route("/db-health")
def db_health():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    try:
        with psycopg.connect(**get_db_connect_kwargs(), row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
                if row and row.get("ok") == 1:
                    return {"ok": True, "database": "reachable"}, 200
    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500

    return {"ok": False, "error": "Unknown connectivity error."}, 500


@app.route("/logout", methods=["POST"])
def logout():
    if not is_valid_csrf(request.form.get("csrf_token", "")):
        return redirect(url_for("login"))
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

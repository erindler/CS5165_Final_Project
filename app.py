import os
import hmac
import csv
import secrets
import threading

from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from datetime import datetime
from decimal import Decimal, InvalidOperation
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
    app.secret_key = secrets.token_hex(32)
else:
    app.secret_key = "dev-secret-change-me"

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=is_app_service,
)

APP_USERNAME = os.environ.get("APP_USERNAME", "admin")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "password123")
APP_EMAIL = os.environ.get("APP_EMAIL", "admin@example.com")

# Directory containing CSV files for local dev (ignored when PGHOST is set)
DATA_DIR = os.environ.get("DATA_DIR", "./data")
HOUSEHOLD_HEADERS = [
    "HSHD_NUM",
    "L",
    "AGE_RANGE",
    "MARITAL",
    "INCOME_RANGE",
    "HOMEOWNER",
    "HSHD_COMPOSITION",
    "HH_SIZE",
    "CHILDREN",
]

PRODUCT_HEADERS = [
    "PRODUCT_NUM",
    "DEPARTMENT",
    "COMMODITY",
    "BRAND_TY",
    "NATURAL_ORGANIC_FLAG",
]

TRANSACTION_HEADERS = [
    "BASKET_NUM",
    "HSHD_NUM",
    "PURCHASE_DATE",
    "PRODUCT_NUM",
    "SPEND",
    "UNITS",
    "STORE_R",
    "WEEK_NUM",
    "YEAR",
]


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


def _is_nullish(value: str | None) -> bool:
    if value is None:
        return True
    text = value.strip()
    return text == "" or text.lower() == "null"


def _null_if_empty(value: str | None) -> str | None:
    if _is_nullish(value):
        return None
    return value.strip()


def _parse_optional_int(value: str | None, field_name: str) -> int | None:
    cleaned = _null_if_empty(value)
    if cleaned is None:
        return None
    if not cleaned.isdigit():
        raise ValueError(f"{field_name} must be a whole number. Got '{value}'.")
    return int(cleaned)


def _parse_decimal(value: str | None, field_name: str) -> Decimal:
    cleaned = _null_if_empty(value)
    if cleaned is None:
        raise ValueError(f"{field_name} is required.")
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field_name} must be numeric. Got '{value}'.")


def _parse_purchase_date(value: str | None) -> datetime.date:
    cleaned = _null_if_empty(value)
    if cleaned is None:
        raise ValueError("PURCHASE_DATE is required.")

    # Accept both assignment format (DD-MON-YY) and ISO format.
    for date_format in ("%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, date_format).date()
        except ValueError:
            continue

    raise ValueError(
        f"PURCHASE_DATE '{value}' is invalid. Use DD-MON-YY or YYYY-MM-DD."
    )


def _load_csv_rows(uploaded_file, required_headers: list[str], label: str) -> list[dict[str, str]]:
    if uploaded_file is None or uploaded_file.filename == "":
        raise ValueError(f"Please provide the {label} CSV file.")

    raw_bytes = uploaded_file.stream.read()
    if not raw_bytes:
        raise ValueError(f"The {label} CSV file is empty.")

    content = raw_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.reader(content.splitlines())

    try:
        header_row = next(reader)
    except StopIteration:
        raise ValueError(f"The {label} CSV file is empty.")

    normalized_headers = [header.strip().upper() for header in header_row]
    missing_headers = [name for name in required_headers if name not in normalized_headers]
    if missing_headers:
        missing_list = ", ".join(missing_headers)
        raise ValueError(f"{label} CSV is missing required headers: {missing_list}.")

    index_by_header = {header: normalized_headers.index(header) for header in required_headers}

    rows: list[dict[str, str]] = []
    for row in reader:
        if not row or all(cell.strip() == "" for cell in row):
            continue

        shaped_row: dict[str, str] = {}
        for header in required_headers:
            cell_index = index_by_header[header]
            value = row[cell_index] if cell_index < len(row) else ""
            shaped_row[header] = value.strip()
        rows.append(shaped_row)

    if not rows:
        raise ValueError(f"The {label} CSV file has no data rows.")

    return rows


def _insert_uploaded_rows(
    household_rows: list[dict[str, str]],
    product_rows: list[dict[str, str]],
    transaction_rows: list[dict[str, str]],
) -> dict[str, int]:
    household_values = [
        (
            row["HSHD_NUM"],
            _null_if_empty(row["L"]),
            _null_if_empty(row["AGE_RANGE"]),
            _null_if_empty(row["MARITAL"]),
            _null_if_empty(row["INCOME_RANGE"]),
            _null_if_empty(row["HOMEOWNER"]),
            _null_if_empty(row["HSHD_COMPOSITION"]),
            _parse_optional_int(row["HH_SIZE"], "HH_SIZE"),
            _parse_optional_int(row["CHILDREN"], "CHILDREN"),
        )
        for row in household_rows
    ]

    product_values = [
        (
            row["PRODUCT_NUM"],
            _null_if_empty(row["DEPARTMENT"]),
            _null_if_empty(row["COMMODITY"]),
            _null_if_empty(row["BRAND_TY"]),
            _null_if_empty(row["NATURAL_ORGANIC_FLAG"]),
        )
        for row in product_rows
    ]

    transaction_values = [
        (
            row["BASKET_NUM"],
            row["HSHD_NUM"],
            _parse_purchase_date(row["PURCHASE_DATE"]),
            row["PRODUCT_NUM"],
            _parse_decimal(row["SPEND"], "SPEND"),
            _parse_decimal(row["UNITS"], "UNITS"),
            _null_if_empty(row["STORE_R"]),
            _parse_optional_int(row["WEEK_NUM"], "WEEK_NUM"),
            _parse_optional_int(row["YEAR"], "YEAR"),
        )
        for row in transaction_rows
    ]

    with psycopg.connect(**get_db_connect_kwargs(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO retail.households (
                    hshd_num,
                    loyalty_flag,
                    age_range,
                    marital,
                    income_range,
                    homeowner,
                    hshd_composition,
                    hh_size,
                    children
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                household_values,
            )

            cur.executemany(
                """
                INSERT INTO retail.products (
                    product_num,
                    department,
                    commodity,
                    brand_ty,
                    natural_organic_flag
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                product_values,
            )

            cur.executemany(
                """
                INSERT INTO retail.transactions (
                    basket_num,
                    hshd_num,
                    purchase_date,
                    product_num,
                    spend,
                    units,
                    store_r,
                    week_num,
                    year
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                transaction_values,
            )

        conn.commit()

    return {
        "households": len(household_values),
        "products": len(product_values),
        "transactions": len(transaction_values),
    }


def fetch_data_pulls(hshd_num: str) -> list[dict[str, Any]]:
    query = """
        SELECT
            t.hshd_num,
            t.basket_num,
            t.purchase_date,
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
            "Date": row["purchase_date"],
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
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "base-uri 'self'; "
        "object-src 'none'; "
        "frame-ancestors 'none'; "
        "form-action 'self'; "
        "script-src 'self' https://cdn.jsdelivr.net; "  # Chart.js
        "style-src 'self'; "
        "img-src 'self' data:"
    )
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
    upload_error = request.args.get("upload_error", "")
    upload_success = request.args.get("upload_success", "")

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
        upload_error=upload_error,
        upload_success=upload_success,
    )


@app.route("/upload-csvs", methods=["POST"])
def upload_csvs():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    if not is_valid_csrf(request.form.get("csrf_token", "")):
        return redirect(
            url_for("data_pulls", upload_error="Session expired. Refresh and try again.")
        )

    try:
        household_rows = _load_csv_rows(
            request.files.get("households_csv"), HOUSEHOLD_HEADERS, "households"
        )
        product_rows = _load_csv_rows(
            request.files.get("products_csv"), PRODUCT_HEADERS, "products"
        )
        transaction_rows = _load_csv_rows(
            request.files.get("transactions_csv"), TRANSACTION_HEADERS, "transactions"
        )

        counts = _insert_uploaded_rows(household_rows, product_rows, transaction_rows)

        success_message = (
            f"Imported {counts['households']} household rows, "
            f"{counts['products']} product rows, and "
            f"{counts['transactions']} transaction rows."
        )
        return redirect(url_for("data_pulls", upload_success=success_message))
    except RuntimeError as exc:
        return redirect(url_for("data_pulls", upload_error=str(exc)))
    except ValueError as exc:
        return redirect(url_for("data_pulls", upload_error=str(exc)))
    except psycopg.Error as exc:
        return redirect(
            url_for(
                "data_pulls",
                upload_error=f"Unable to import data into PostgreSQL: {exc.__class__.__name__}.",
            )
        )
    except Exception:
        return redirect(
            url_for(
                "data_pulls",
                upload_error="Unexpected import failure. Please verify the CSV formats and retry.",
            )
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


_analysis_cache: dict | None = None
_analysis_lock = threading.Lock()
_analysis_error: str | None = None


def _run_analysis_background() -> None:
    global _analysis_cache, _analysis_error
    try:
        from basket_ml import run_basket_analysis
        result = run_basket_analysis(csv_dir=DATA_DIR)
        with _analysis_lock:
            _analysis_cache = result
            _analysis_error = None
    except Exception as exc:
        with _analysis_lock:
            _analysis_error = str(exc)


@app.route("/basket-analysis")
def basket_analysis():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    with _analysis_lock:
        cached = _analysis_cache
        errored = _analysis_error

    # Start background job on first visit
    if cached is None and errored is None:
        t = threading.Thread(target=_run_analysis_background, daemon=True)
        t.start()

    return render_template(
        "basket_analysis.html",
        username=session.get("username", "User"),
        email=session.get("email", ""),
        results=cached,
        error=errored,
        csrf_token=get_csrf_token(),
    )


@app.route("/basket-analysis/results")
def basket_analysis_results():
    """Polling endpoint — returns JSON status while the model runs."""
    if not session.get("logged_in"):
        return jsonify({"status": "unauthorized"}), 401

    with _analysis_lock:
        cached = _analysis_cache
        errored = _analysis_error

    if errored:
        return jsonify({"status": "error", "message": errored})
    if cached is None:
        return jsonify({"status": "running"})
    return jsonify({"status": "done", "data": cached})


@app.route("/basket-analysis/refresh", methods=["POST"])
def basket_analysis_refresh():
    """Force a fresh run — useful after uploading new data."""
    if not session.get("logged_in"):
        return jsonify({"status": "unauthorized"}), 401
    if not is_valid_csrf(request.form.get("csrf_token", "")):
        return jsonify({"status": "forbidden"}), 403

    global _analysis_cache, _analysis_error
    with _analysis_lock:
        _analysis_cache = None
        _analysis_error = None

    t = threading.Thread(target=_run_analysis_background, daemon=True)
    t.start()
    return jsonify({"status": "started"})


_churn_cache: dict | None = None
_churn_lock = threading.Lock()
_churn_error: str | None = None
 
 
def _run_churn_background() -> None:
    global _churn_cache, _churn_error
    try:
        from churn_ml import run_churn_analysis
        result = run_churn_analysis(csv_dir=DATA_DIR)
        with _churn_lock:
            _churn_cache = result
            _churn_error = None
    except Exception as exc:
        with _churn_lock:
            _churn_error = str(exc)
 
 
@app.route("/churn")
def churn():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
 
    with _churn_lock:
        cached = _churn_cache
        errored = _churn_error
 
    if cached is None and errored is None:
        t = threading.Thread(target=_run_churn_background, daemon=True)
        t.start()
 
    return render_template(
        "churn.html",
        username=session.get("username", "User"),
        email=session.get("email", ""),
        results=cached,
        error=errored,
        csrf_token=get_csrf_token(),
    )
 
 
@app.route("/churn/results")
def churn_results():
    if not session.get("logged_in"):
        return jsonify({"status": "unauthorized"}), 401
 
    with _churn_lock:
        cached = _churn_cache
        errored = _churn_error
 
    if errored:
        return jsonify({"status": "error", "message": errored})
    if cached is None:
        return jsonify({"status": "running"})
    return jsonify({"status": "done", "data": cached})
 
 
@app.route("/churn/refresh", methods=["POST"])
def churn_refresh():
    if not session.get("logged_in"):
        return jsonify({"status": "unauthorized"}), 401
    if not is_valid_csrf(request.form.get("csrf_token", "")):
        return jsonify({"status": "forbidden"}), 403
 
    global _churn_cache, _churn_error
    with _churn_lock:
        _churn_cache = None
        _churn_error = None
 
    t = threading.Thread(target=_run_churn_background, daemon=True)
    t.start()
    return jsonify({"status": "started"})
 
 
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

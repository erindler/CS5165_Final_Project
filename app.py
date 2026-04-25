import os
import hmac
import secrets
import csv
from datetime import datetime
from functools import lru_cache
from pathlib import Path

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
DATA_DIR = Path(__file__).resolve().parent / "8451_The_Complete_Journey_2_Sample-2"


def _normalize(value: str) -> str:
    return (value or "").strip()


@lru_cache(maxsize=1)
def load_households() -> dict[str, dict[str, str]]:
    households_path = DATA_DIR / "400_households.csv"
    households: dict[str, dict[str, str]] = {}

    with households_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            hshd_num = _normalize(row.get("HSHD_NUM"))
            if not hshd_num:
                continue
            households[hshd_num] = {
                "loyalty_flag": _normalize(row.get("L")),
                "age_range": _normalize(row.get("AGE_RANGE")),
                "marital_status": _normalize(row.get("MARITAL")),
                "income_range": _normalize(row.get("INCOME_RANGE")),
                "homeowner_desc": _normalize(row.get("HOMEOWNER")),
                "hshd_size": _normalize(row.get("HH_SIZE")),
                "children": _normalize(row.get("CHILDREN")),
            }

    return households


@lru_cache(maxsize=1)
def load_products() -> dict[str, dict[str, str]]:
    products_path = DATA_DIR / "400_products.csv"
    products: dict[str, dict[str, str]] = {}

    with products_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            product_num = _normalize(row.get("PRODUCT_NUM"))
            if not product_num:
                continue
            products[product_num] = {
                "department": _normalize(row.get("DEPARTMENT")),
                "commodity": _normalize(row.get("COMMODITY")),
            }

    return products


@lru_cache(maxsize=1)
def load_transactions() -> list[dict[str, object]]:
    transactions_path = DATA_DIR / "400_transactions.csv"
    transactions: list[dict[str, object]] = []

    with transactions_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            purchase_date_str = _normalize(row.get("PURCHASE_"))
            if purchase_date_str:
                purchase_date = datetime.strptime(purchase_date_str, "%d-%b-%y").date()
            else:
                purchase_date = datetime.min.date()

            transactions.append(
                {
                    "hshd_num": _normalize(row.get("HSHD_NUM")),
                    "basket_num": _normalize(row.get("BASKET_NUM")),
                    "product_num": _normalize(row.get("PRODUCT_NUM")),
                    "spend": _normalize(row.get("SPEND")),
                    "units": _normalize(row.get("UNITS")),
                    "store_region": _normalize(row.get("STORE_R")),
                    "week_num": _normalize(row.get("WEEK_NUM")),
                    "year": _normalize(row.get("YEAR")),
                    "purchase_date": purchase_date,
                }
            )

    return transactions


def fetch_data_pulls(hshd_num: str) -> list[dict[str, str]]:
    households = load_households()
    products = load_products()
    transactions = load_transactions()

    household = households.get(hshd_num, {})
    rows: list[dict[str, str]] = []

    for tx in transactions:
        if tx["hshd_num"] != hshd_num:
            continue

        product = products.get(tx["product_num"], {})
        rows.append(
            {
                "Hshd_num": tx["hshd_num"],
                "Basket_num": tx["basket_num"],
                "Product_num": tx["product_num"],
                "Department": product.get("department", ""),
                "Commodity": product.get("commodity", ""),
                "Spend": tx["spend"],
                "Units": tx["units"],
                "Store_region": tx["store_region"],
                "Week_num": tx["week_num"],
                "Year": tx["year"],
                "Loyalty_flag": household.get("loyalty_flag", ""),
                "Age_range": household.get("age_range", ""),
                "Marital_status": household.get("marital_status", ""),
                "Income_range": household.get("income_range", ""),
                "Homeowner_desc": household.get("homeowner_desc", ""),
                "Hshd_size": household.get("hshd_size", ""),
                "Children": household.get("children", ""),
                "_sort_date": tx["purchase_date"],
            }
        )

    rows.sort(
        key=lambda row: (
            row["Hshd_num"],
            row["Basket_num"],
            row["_sort_date"],
            row["Product_num"],
            row["Department"],
            row["Commodity"],
        )
    )

    for row in rows:
        row.pop("_sort_date", None)

    return rows


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
    rows: list[dict[str, str]] = []
    error = None

    if hshd_num:
        if not hshd_num.isdigit():
            error = "Please enter a numeric hshd_num value."
        else:
            rows = fetch_data_pulls(hshd_num)

    return render_template(
        "data_pulls.html",
        username=session.get("username", "User"),
        email=session.get("email", ""),
        csrf_token=get_csrf_token(),
        hshd_num=hshd_num,
        rows=rows,
        error=error,
    )


@app.route("/logout", methods=["POST"])
def logout():
    if not is_valid_csrf(request.form.get("csrf_token", "")):
        return redirect(url_for("login"))
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

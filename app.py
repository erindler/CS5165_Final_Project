import os
import hmac
import secrets
import threading

from flask import Flask, jsonify, redirect, render_template, request, session, url_for


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
DATA_DIR = os.environ.get("DATA_DIR", ".")


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


# ---------------------------------------------------------------------------
# Auth routes — unchanged from partner's version
# ---------------------------------------------------------------------------

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


@app.route("/logout", methods=["POST"])
def logout():
    if not is_valid_csrf(request.form.get("csrf_token", "")):
        return redirect(url_for("login"))
    session.clear()
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Basket Analysis — Step 7
# ---------------------------------------------------------------------------

# In-memory cache: the ML run takes ~15-30 s, so we do it once in a
# background thread.  Swap for Redis in a multi-instance production deploy.
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
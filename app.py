import os
import hmac
import secrets

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


@app.route("/logout", methods=["POST"])
def logout():
    if not is_valid_csrf(request.form.get("csrf_token", "")):
        return redirect(url_for("login"))
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

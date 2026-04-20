import os

from flask import Flask, redirect, render_template, request, session, url_for


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

# For this assignment starter, credentials are intentionally simple.
APP_USERNAME = os.environ.get("APP_USERNAME", "admin")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "password123")
APP_EMAIL = os.environ.get("APP_EMAIL", "admin@example.com")


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

    return render_template("login.html", error=error, form_data=form_data)


@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template(
        "dashboard.html",
        username=session.get("username", "User"),
        email=session.get("email", ""),
    )


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

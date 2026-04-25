# Flask Login Dashboard (Azure App Service Ready)

This project is a minimal Flask web app for your assignment:

- Login page at `/login`
- Protected dashboard at `/dashboard`
- Dashboard currently shows: `Empty Dashboard`

## Security Notes (Now Implemented)

The app now includes baseline web security protections:

- HTTPS redirect support for Azure App Service
- Secure session cookies on Azure (`Secure`, `HttpOnly`, `SameSite=Lax`)
- CSRF protection for login and logout forms
- Security response headers:
	- `Strict-Transport-Security` (on Azure)
	- `Content-Security-Policy`
	- `X-Frame-Options: DENY`
	- `X-Content-Type-Options: nosniff`
	- `Referrer-Policy`
	- `Permissions-Policy`

## Local Run

From the project root:

```powershell
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
flask --app app run
```

Open `http://localhost:5000`.

## PostgreSQL Configuration For Data Pulls

The `/data-pulls` page now reads from PostgreSQL (`retail` schema tables), not CSV files.

Configure one of these environment variable sets before running:

- `DATABASE_URL` (for example: `postgresql://user:password@host:5432/dbname?sslmode=require`)
- `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` (optional: `PGPORT`, `PGSSLMODE`)
- `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` (optional: `POSTGRES_PORT`, `POSTGRES_SSLMODE`)

Azure App Service `POSTGRESQLCONNSTR_*` connection strings are also detected automatically.

Default login credentials:

- Username: `admin`
- Password: `password123`
- Email: `admin@example.com`

You can override these with environment variables:

- `APP_USERNAME`
- `APP_PASSWORD`
- `APP_EMAIL`
- `FLASK_SECRET_KEY`

## Deploy To Azure App Service (Quickstart Style)

Use Azure CLI from this project folder.

1. Sign in:

```powershell
az login
```

2. Create and deploy in one command:

```powershell
az webapp up --runtime PYTHON:3.13 --sku B1 --name <unique-app-name> --logs
```

Notes:

- `az webapp up` creates the resource group, app service plan, and web app if needed.
- `--name` must be globally unique.
- If you omit `--name`, Azure generates one.
- You can add `--location <region>` if desired.

3. Configure app settings for production credentials:

```powershell
az webapp config appsettings set --name <unique-app-name> --resource-group <resource-group-name> --settings APP_USERNAME=<your-user> APP_PASSWORD=<your-pass> APP_EMAIL=<your-email> FLASK_SECRET_KEY=<long-random-secret>
```

4. Enforce HTTPS at the App Service level:

```powershell
az webapp update --name <unique-app-name> --resource-group <resource-group-name> --set httpsOnly=true
```

5. Browse to:

```text
https://<unique-app-name>.azurewebsites.net
```

## If Browser Shows "Dangerous Website"

If your Azure URL is flagged as dangerous, it is usually a domain reputation/interstitial issue (browser safe-browsing or SmartScreen), not only an app code issue.

Use this checklist:

1. Verify HTTPS-only and TLS settings:

```powershell
az webapp update --name <app-name> --resource-group <resource-group> --set httpsOnly=true
az webapp config set --name <app-name> --resource-group <resource-group> --min-tls-version 1.2
```

2. Confirm production secrets are set (do not use defaults):

```powershell
az webapp config appsettings set --name <app-name> --resource-group <resource-group> --settings APP_USERNAME=<your-user> APP_PASSWORD=<your-pass> APP_EMAIL=<your-email> FLASK_SECRET_KEY=<long-random-secret>
```

3. Check response headers from your deployed URL:

```powershell
curl -I https://<app-name>.azurewebsites.net/
```

4. Consider using a custom domain for better trust/reputation than a random generated subdomain:

```powershell
az webapp config hostname add --webapp-name <app-name> --resource-group <resource-group> --hostname <your-domain>
```

5. Request reputation review if still flagged:

- Microsoft SmartScreen report site (report as safe)
- Google Safe Browsing false-positive review

Note: reputation systems can take time to reclassify a site even after technical fixes are complete.

## Optional: Stream Logs

```powershell
az webapp log config --web-server-logging filesystem --name <unique-app-name> --resource-group <resource-group-name>
az webapp log tail --name <unique-app-name> --resource-group <resource-group-name>
```

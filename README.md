# Flask Login Dashboard (Azure App Service Ready)

This project is a minimal Flask web app for your assignment:

- Login page at `/login`
- Protected dashboard at `/dashboard`
- Dashboard currently shows: `Empty Dashboard`

## Local Run

From the project root:

```powershell
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
flask --app app run
```

Open `http://localhost:5000`.

Default login credentials:

- Username: `admin`
- Password: `password123`

You can override these with environment variables:

- `APP_USERNAME`
- `APP_PASSWORD`
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
az webapp config appsettings set --name <unique-app-name> --resource-group <resource-group-name> --settings APP_USERNAME=<your-user> APP_PASSWORD=<your-pass> FLASK_SECRET_KEY=<long-random-secret>
```

4. Browse to:

```text
https://<unique-app-name>.azurewebsites.net
```

## Optional: Stream Logs

```powershell
az webapp log config --web-server-logging filesystem --name <unique-app-name> --resource-group <resource-group-name>
az webapp log tail --name <unique-app-name> --resource-group <resource-group-name>
```

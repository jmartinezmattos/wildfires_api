## Running the server

```powershell
uvicorn app.main:app --reload
```

## Connecting to Cloud SQL Using Cloud SQL Auth Proxy (Windows)

### 1. Download Cloud SQL Auth Proxy

Download the **Windows 64-bit** binary from the official Google Cloud documentation:

https://docs.cloud.google.com/sql/docs/mysql/connect-auth-proxy?hl=es-419#windows-64-bit

Move the downloaded file to the project folder

### 2. Run the proxy

Make sure you have the Google Cloud CLI installed and authenticate with your account:

```powershell
gcloud auth login
```

Open a terminal (PowerShell or Command Prompt) in the directory where the executable is located and run:

```powershell
.\cloud-sql-proxy.x64.exe wildfires-479718:us-central1:wildfires
```

Where:

wildfires-479718 is the Google Cloud project ID

us-central1 is the region

wildfires is the Cloud SQL instance ID

The proxy will start listening on the default local port (3306) and securely forward connections to the Cloud SQL instance.
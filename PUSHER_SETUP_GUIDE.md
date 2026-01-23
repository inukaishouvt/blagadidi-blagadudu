# Pusher Setup Guide

To use the new **Serverless Broadcaster**, you need to get API keys from Pusher Channels (it's free).

## 1. Create an Account
1.  Go to [pusher.com](https://pusher.com/).
2.  Click **Sign Up** (or Log In).
3.  Select **Channels** (not Beams).

## 2. Create an App
1.  Click **"Create App"** or **"Get Started"**.
2.  **Name**: `ad-pipeline` (or anything you want).
3.  **Cluster**: Choose the one closest to you (e.g., `mt1` for US East, `ap1` for Asia Pacific).
    *   *Note: Remember which cluster you picked!*
4.  Frontend: `Vanilla JS`.
5.  Backend: `Python`.
6.  Click **Create App**.

## 3. Get Credentials
1.  In your new app dashboard, click on the **"App Keys"** tab on the left sidebar.
2.  You will see:
    *   `app_id`
    *   `key`
    *   `secret`
    *   `cluster`

## 4. Configure Your Project

### A. Update `secrets/secrets.toml`
Open `secrets/secrets.toml` and fill in the values:

```toml
[pusher]
app_id = "YOUR_APP_ID"
key = "YOUR_KEY"
secret = "YOUR_SECRET"
cluster = "YOUR_CLUSTER"
```

### B. Update `broadcaster_client.html`
Open `broadcaster_client.html` (Lines 32-33) and update the Key and Cluster so the browser can connect:

```javascript
const PUSHER_KEY = 'YOUR_KEY'; 
const PUSHER_CLUSTER = 'YOUR_CLUSTER'; 
```
*(The `secret` is NOT needed in the HTML file for security reasons)*

## 5. Run & Verify
1.  **Start the Broadcaster**:
    ```bash
    python kafka_to_pusher.py
    ```
2.  **Open the Client**:
    Open `broadcaster_client.html` in your web browser.
3.  **Generate Events**:
    Run your existing producer or pipeline script. You should see events pop up in the browser instantly!

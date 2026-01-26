# Deploying to Streamlit Cloud

Your app is ready for Streamlit Cloud! Here is how to deploy it.

## 1. Prepare GitHub
1.  Push your code to a GitHub repository.
2.  Make sure `requirements.txt` is in the root (it is!).

## 2. Deploy App
1.  Go to [share.streamlit.io](https://share.streamlit.io/).
2.  Click **"New App"**.
3.  Select your Repository, Branch, and Main File Path (`app.py` or `korique/app.py`).
4.  Click **"Deploy!"**.

## 3. Configure Secrets (CRITICAL)
Your app needs credentials. On Streamlit Cloud:
1.  Go to your app's **Settings** -> **Secrets**.
2.  Paste the contents of your `secrets/secrets.toml` file into the text box.
    *   It should look exactly like your local file:
        ```toml
        [kafka]
        bootstrap_servers = "..."
        ...

        [database]
        user = "postgres"
        password = "..."
        host = "db.dmbzzovtrykfwqsbkduq.supabase.co"
        port = "5432" # <-- READ NOTE BELOW
        dbname = "postgres"

        [pusher]
        ...
        ```

### ⚠️ Troubleshooting Supabase Connection
If you see **"Cannot assign requested address"**, it means Streamlit Cloud is having trouble with Supabase's IPv6-only address.
**FIX**: Use the Transaction Pooler (Supavisor) on port **6543**.

1.  Change `port = "5432"` to `port = "6543"`.
2.  Your `host` usually stays the same (`db.dmbzzovtrykfwqsbkduq.supabase.co`).
3.  Click **Save**.

## 4. The "Broadcaster" Limitation
**Important**: Streamlit Cloud only hosts `app.py`. It will **NOT** run `kafka_to_pusher.py` automatically.

If you deploy only `app.py`:
*   ✅ **Dashboard works**: You can see data and run ETL.
*   ❌ **Live Broadcasts stop**: The Pusher updates to the external HTML client will stop working because `kafka_to_pusher.py` isn't running.

### Solution
To keep the Live Broadcasts working, you must run `kafka_to_pusher.py` somewhere:
1.  **Locally**: Just keep `python kafka_to_pusher.py` running on your laptop.
2.  **Separate Cloud**: Deploy the script to a worker service like **Railway**, **Render**, or **Heroku**.

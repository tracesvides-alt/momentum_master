import os
import requests
import json
from datetime import datetime

# Discord Webhook URL (Environment Variable)
# In Streamlit Cloud, this should be set in secrets or env vars
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', '')

def send_discord_alert(title, description, color=0x1DA1F2, fields=None, footer_text="Momentum Master", content=""):
    """
    Send a rich embed message to Discord.
    """
    if not DISCORD_WEBHOOK_URL:
        # Check Streamlit secrets if env var is missing (optional fallback)
        import streamlit as st
        try:
            if "DISCORD_WEBHOOK_URL" in st.secrets:
                DISCORD_WEBHOOK_URL_SECRET = st.secrets["DISCORD_WEBHOOK_URL"]
            else:
                 return False, "Webhook URL not found in env or secrets"
        except:
            return False, "Webhook URL not found"
    else:
        DISCORD_WEBHOOK_URL_SECRET = DISCORD_WEBHOOK_URL

    embed = {
        "title": title,
        "description": description,
        "color": color,
        "footer": {
            "text": footer_text
        },
        "timestamp": datetime.utcnow().isoformat()
    }

    if fields:
        embed["fields"] = fields

    payload = {
        "embeds": [embed],
        "content": content
    }

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL_SECRET,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return True, "Success"
    except Exception as e:
        return False, str(e)

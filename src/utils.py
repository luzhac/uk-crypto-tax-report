import yfinance as yf
import os
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

_sent_email_cache = {}

def get_usd_to_gbp_from_yahoo(start="2024-06-21", end="2025-06-21"):
    ticker = yf.download("GBPUSD=X", start=start, end=end, interval="1d")
    ticker["USD_to_GBP"] = 1 / ticker["Close"]
    df = ticker.reset_index()[["Date", "USD_to_GBP"]].dropna()
    folder = './data'
    filename =  'usd_gbp.csv'
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath)
    return df

def _cleanup_cache(expiry_seconds=3600):
    current_time = time.time()
    expired_keys = [k for k, v in _sent_email_cache.items() if current_time - v > expiry_seconds]
    for k in expired_keys:
        del _sent_email_cache[k]

def smtp_send_mail(subject, body):

    current_time = time.time()
    _cleanup_cache()

    if subject in _sent_email_cache:
        print("Duplicate email detected — not sending.")
        return False

    try:
        load_dotenv()
        gmail_to = os.getenv('email_to')
        gmail_user = os.getenv('gmail_user')
        gmail_password = os.getenv('gmail_password')
    except Exception as e:
        print(f"Error retrieving configuration: {e}")
        return False

    if not all([gmail_user, gmail_password, gmail_to]):
        print("Missing email configuration.")
        return False

    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = gmail_to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    smtp_server = "smtp.gmail.com"
    port = 465  # SSL port
    server = None

    try:
        server = smtplib.SMTP_SSL(smtp_server, port)
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, gmail_to, msg.as_string())
        print("Email sent successfully!")
        _sent_email_cache[subject] = current_time
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass

if __name__ == "__main__":

    smtp_send_mail('a','b')

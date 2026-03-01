import yfinance as yf
import os
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv



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



if __name__ == "__main__":

    pass

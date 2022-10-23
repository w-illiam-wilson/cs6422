import json
import sys

def print_progress(desc, i, n):
    sys.stdout.write('\r')
    j = (i + 1) / n
    sys.stdout.write("Progress %s: [%-20s] %d%%" % (desc,'='*int(20*j), 100*j))
    sys.stdout.flush()

def load_mysql_args():
    username = ""
    password = ""
    no_password = False

    with open('config.json') as f:
        data = json.load(f)
        username = data['username']
        if "no_password" in data.keys():
            no_password = True
        else:
            password = data['password']

    args = {
        "host": "localhost",
        "port": "3306",
        "user": username,
        "database": "transactions"
    }

    if not no_password:
        args["password"] = password

    return args

def load_clickhouse_args():
    args = {
        "host": "localhost",
    }
    password = ""
    no_password = False
    with open('config.json') as f:
        data = json.load(f)
        if "no_password_clickhouse" in data.keys():
            no_password = True
        else:
            if "password_clickhouse" in data.keys():
                password = data['password_clickhouse']
            elif "password" in data.keys():
                password = data['password']
    if not no_password:
        args["password"] = password
    return args

STOCK_NAMES = [
    "ACC",                "ADANIGREEN",         "ADANIPORTS",     "AMBUJACEM",      "APOLLOHOSP", 
    "ADANIENT",           "ASIANPAINT",         "AUROPHARMA",     "AXISBANK",       "BAJAJ-AUTO",
    "BAJAJFINSV",         "BAJAJHLDNG",         "BAJFINANCE",     "BANDHANBNK",     "BANKBARODA",     
    "BERGEPAINT",         "BHARTIARTL",         "BIOCON",         "BOSCHLTD",       "BPCL", 
    "BRITANNIA",          "CADILAHC",           "CHOLAFIN",       "CIPLA",          "COALINDIA",
    "COLPAL",             "DABUR",              "DIVISLAB",       "DLF",            "DMART",
    "DRREDDY",            "EICHERMOT",          "GAIL",           "GLAND",          "GODREJCP", 
    "GRASIM",             "HAVELLS",            "HCLTECH",        "HDFC",           "HDFCAMC",
    "HDFCBANK",           "HDFCLIFE",           "HEROMOTOCO",     "HINDALCO",       "HINDPETRO", 
    "HINDUNILVR",         "ICICIBANK",          "ICICIGI",        "ICICIPRULI",     "IGL",
    "INDIGO",             "INDUSINDBK",         "INDUSTOWER",     "INFY",           "IOC",
    "ITC",                "JINDALSTEL",         "JSWSTEEL",       "JUBLFOOD",       "KOTAKBANK",
    "LT",                 "LTI",                "LUPIN",          "MARICO",         "MARUTI", 
    "MCDOWELL-N",         "MUTHOOTFIN",         "M_M",            "NAUKRI",         "NESTLEIND",
    "NMDC",               "NTPC",               "ONGC",           "PEL",            "PGHH",
    "PIDILITIND",         "PIIND",              "PNB",            "POWERGRID",      "RELIANCE",
    "SAIL",               "SBICARD",            "SBILIFE",        "SBIN",           "SHREECEM",
    "SIEMENS",            "SUNPHARMA",          "TATACONSUM",     "TATAMOTORS",     "TATASTEEL", 
    "TCS",                "TECHM",              "TITAN",          "TORNTPHARM",     "ULTRACEMCO",
    "UPL",                "VEDL",               "WIPRO",          "YESBANK",
]

STOCK_TO_DATA_FILE_NAME_MAP = dict([(stock_name, f"./data/{stock_name}_with_indicators_.csv") for stock_name in STOCK_NAMES])

COLUMN_NAMES = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "sma5",
    "sma10",
    "sma15",
    "sma20",
    "ema5",
    "ema10",
    "ema15",
    "ema20",
    "upperband",
    "middleband",
    "lowerband",
    "HT_TRENDLINE",
    "KAMA10",
    "KAMA20",
    "KAMA30",
    "SAR",
    "TRIMA5",
    "TRIMA10",
    "TRIMA20",
    "ADX5",
    "ADX10",
    "ADX20",
    "APO",
    "CCI5",
    "CCI10",
    "CCI15",
    "macd510",
    "macd520",
    "macd1020",
    "macd1520",
    "macd1226",
    "MOM10",
    "MOM15",
    "MOM20",
    "ROC5",
    "ROC10",
    "ROC20",
    "PPO",
    "RSI14",
    "RSI8",
    "slowk",
    "slowd",
    "fastk",
    "fastd",
    "fastksr",
    "fastdsr",
    "ULTOSC",
    "WILLR",
    "ATR",
    "Trange",
    "TYPPRICE",
    "HT_DCPERIOD",
    "BETA"
]
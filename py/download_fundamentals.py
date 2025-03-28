import sys
sys.path.append("py")
import mysql.connector
import requests

cursor = db.cursor()

from get_key import get_key

# Clave API de EODHD

API_KEY = get_key()

BASE_URL = "https://eodhd.com/api/fundamentals/"

# Leer los tickers de la tabla


MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "B4yesian",
    "database": "quantitative_db"
}

def get_fundamentals(ticker):
    url = f"https://eodhd.com/api/fundamentals/{ticker}?api_token={API_KEY}"
    print(url)
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None

def save_income_statement(ticker, data, cursor):
    income_data = data.get("Financials", {}).get("Income_Statement", {}).get("quarterly", {})
    for date, report in income_data.items():
        cursor.execute("""
            INSERT INTO income_statement (ticker, date, revenue, cost_of_revenue, gross_profit, operating_income, net_income)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                revenue=VALUES(revenue), cost_of_revenue=VALUES(cost_of_revenue),
                gross_profit=VALUES(gross_profit), operating_income=VALUES(operating_income),
                net_income=VALUES(net_income)
        """, (
            ticker, date,
            report.get("revenue"),
            report.get("cost_of_revenue"),
            report.get("gross_profit"),
            report.get("operating_income"),
            report.get("net_income")
        ))

def save_balance_sheet(ticker, data, cursor):
    bs_data = data.get("Financials", {}).get("Balance_Sheet", {}).get("quarterly", {})
    for date, report in bs_data.items():
        cursor.execute("""
            INSERT INTO balance_sheet (ticker, date, total_assets, total_liabilities, total_equity,
                                       cash_and_cash_equivalents, short_term_debt, long_term_debt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_assets=VALUES(total_assets),
                total_liabilities=VALUES(total_liabilities),
                total_equity=VALUES(total_equity),
                cash_and_cash_equivalents=VALUES(cash_and_cash_equivalents),
                short_term_debt=VALUES(short_term_debt),
                long_term_debt=VALUES(long_term_debt)
        """, (
            ticker, date,
            report.get("totalAssets"),
            report.get("totalLiabilities"),
            report.get("totalEquity"),
            report.get("cashAndCashEquivalents"),
            report.get("shortTermDebt"),
            report.get("longTermDebt")
        ))

def save_cash_flow(ticker, data, cursor):
    cf_data = data.get("Financials", {}).get("Cash_Flow", {}).get("quarterly", {})
    for date, report in cf_data.items():
        cursor.execute("""
            INSERT INTO cash_flow (ticker, date, operating_cash_flow, investing_cash_flow,
                                   financing_cash_flow, free_cash_flow)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                operating_cash_flow=VALUES(operating_cash_flow),
                investing_cash_flow=VALUES(investing_cash_flow),
                financing_cash_flow=VALUES(financing_cash_flow),
                free_cash_flow=VALUES(free_cash_flow)
        """, (
            ticker, date,
            report.get("netCashProvidedByOperatingActivities"),
            report.get("netCashUsedForInvestingActivites"),
            report.get("netCashUsedProvidedByFinancingActivities"),
            report.get("freeCashFlow")
        ))

def main():
    
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""SELECT CONCAT(symbol, '.', exchange) as ticker FROM tickers where country in ("USA", "Germany", "Canada", "Australia", "Sweden", "France", "Denmark", "Poland", "Norway", "Spain")""")
    tickers = [row[0] for row in cursor.fetchall()]

    for symbol in tickers:
        print(f"📥 Descargando estados financieros de {symbol}")
        data = get_fundamentals(symbol)
        if data:
            save_income_statement(symbol, data, cursor)
            save_balance_sheet(symbol, data, cursor)
            save_cash_flow(symbol, data, cursor)
            conn.commit()
        else:
            print(f"⚠️ No se pudieron obtener datos para {symbol}")

    cursor.close()
    conn.close()
    print("✅ Proceso completado")

if __name__ == "__main__":
    main()
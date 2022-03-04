import config
import sqlite3
import alpaca_trade_api as tradeapi

connection = sqlite3.connect("app.db")
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("""
    SELECT * FROM stock
""")
rows = cursor.fetchall()
symbols = [row['symbol'] for row in rows]

api = tradeapi.REST(config.API_KEY,
                    config.API_SECRET, base_url=config.BASEURL)
assets = api.list_assets()


for asset in assets:
    try:
        if asset.status == 'active' and asset.tradable and asset.symbol not in symbols:
            print(f"added a new stock {asset.symbol} || {asset.name}")
            cursor.execute(
                "INSERT INTO stock (symbol, name) VALUES (?, ?)", (asset.symbol, asset.name))
        else:
            print('stock exist ', asset.symbol)

    except Exception as e:
        print("error in populate.py, saving stocks to database")
        print(e)
        break


connection.commit()

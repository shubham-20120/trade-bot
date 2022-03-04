
import sqlite3
import config
import alpaca_trade_api as tradeapi

connection = sqlite3.connect('app.db')
connection.row_factory = sqlite3.Row
# to make every Row an object so that we can access individual as row['symbol']
cursor = connection.cursor()
cursor.execute("""
    SELECT * FROM stock
""")
rows = cursor.fetchall()

symbols = [row['symbol'] for row in rows]
stock_dict = {}

for row in rows:
    try:
        symbol = row['symbol']
        symbols.append(symbol)
        stock_dict[symbol] = row['id']
    except Exception as e:
        print('error in populate_price.py... line 28', e)
        break

api = tradeapi.REST(config.API_KEY,
                    config.API_SECRET, base_url=config.BASEURL)


chunk_size = 200
for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]
    barsets = api.get_barset(symbol_chunk, 'day')
    for symbol in barsets:
        print(f"Parsing the symbol {symbol}")
        for bar in barsets[symbol]:
            stock_id = stock_dict[symbol]
            cursor.execute("""
                INSERT INTO stock_price (stock_id, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)
            """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v))
    break

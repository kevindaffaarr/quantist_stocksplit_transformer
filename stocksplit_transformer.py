# =================================================================================================
# MODULE IMPORT
# =================================================================================================
import psycopg
from psycopg import Error
from psycopg.rows import dict_row

import json
import pandas as pd
import time

# =================================================================================================
# POSTGRESQL Function
# =================================================================================================
def create_connection(conninfo):
	# Create a database connection to SQLite Database
	conn = None
	try:
		conn = psycopg.connect(\
			conninfo=conninfo,\
			row_factory=dict_row)
		print("postgresql:" + str(psycopg.pq.version()))
		cur = conn.cursor()
		return conn,cur
	except Error as e:
		print(e)
		raise

def select(cur,sql,param=[]):
	try:
		cur.execute(sql,param)
		results = cur.fetchall()
		results = pd.DataFrame(results)
	except Exception as e:
		print(e)
		raise
	else:
		return results

def execute(conn,cur,sql,param=[]):
	try:
		cur.execute(sql,param)
	except Exception as e:
		conn.rollback()
		print(e)
		raise
	else:
		conn.commit()
		print('Done Execute')
	finally:
		return

def dbquery_stocksplit(date,code,ca,ratio,list_broker):
	date_str = "'" + date + "'"
	if ca == 'ss':
		t = '*'
		d = '/'
	elif ca == 'rs':
		t = '/'
		d = '*'
	else:
		raise Exception("Unidentified Corporate Action. Stock Split: ss, Reverse Stock Split: rs")
	
	list_pair = f"""
				previous = previous{d}{ratio},
				openprice = openprice{d}{ratio},
				firsttrade = firsttrade{d}{ratio},
				high = high{d}{ratio},
				low = low{d}{ratio},
				close = close{d}{ratio},
				change = change{d}{ratio},
				volume = volume{t}{ratio},
				offer = offer{d}{ratio},
				offervolume = offervolume{t}{ratio},
				bid = bid{d}{ratio},
				bidvolume = bidvolume{t}{ratio},
				listedshares = listedshares{t}{ratio},
				tradebleshares = tradebleshares{t}{ratio},
				weightforindex = weightforindex{t}{ratio},
				foreignsell = foreignsell{t}{ratio},
				foreignbuy = foreignbuy{t}{ratio},
				nonregularvolume = nonregularvolume{t}{ratio}
	"""

	for broker in list_broker:
		list_pair += f""",
					broker_{broker}_bavg = broker_{broker}_bavg{d}{ratio},
					broker_{broker}_bvol = broker_{broker}_bvol{t}{ratio},
					broker_{broker}_savg = broker_{broker}_savg{d}{ratio},
					broker_{broker}_svol = broker_{broker}_svol{t}{ratio}
		"""

	sql = f"""
			UPDATE stockdata_{code}
			SET {list_pair}
			WHERE date < {date_str};
	"""
	return sql
# =================================================================================================
# QUANTIST FUNCTION
# =================================================================================================
def quantist_connect():
	with open("credentials\credentials.json","r") as f:
		credentials = json.load(f)
		pg_db_name = credentials["pg_db_name"]
		pg_user = credentials["pg_user"]
		pg_password = credentials["pg_password"]
		pg_host = credentials["pg_host"]
		pg_port = credentials["pg_port"]
		conninfo = f"""postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db_name}"""
		conn,cur = create_connection(conninfo)
	return conn,cur

def quantist_stocksplit_transformer():
	start = time.time()
	# READ CSV FOR LIST OF STOCKSPLIT
	# FORMAT: date,code,ca,ratio
	stocksplit_list = pd.read_csv("stocksplit.csv",header=0)
	stocksplit_list.code = stocksplit_list.code.str.lower()
	stocksplit_list.ca = stocksplit_list.ca.str.lower()
	stocksplit_list = stocksplit_list.sort_values(by='date')

	# READ BROKER LIST
	conn, cur = quantist_connect()
	sql = "SELECT code FROM list_broker"
	list_broker = select(cur,sql,param=[]).code.sort_values(ignore_index=True)

	# FOR EACH STOCKSPLIT ACTION
	for idx, row in stocksplit_list.iterrows():
		# CREATE QUERY
		sql = dbquery_stocksplit(row["date"],row["code"],row["ca"],row["ratio"],list_broker)
		# EXECUTE QUERY
		print(f"Execute UPDATE (Reverse) Stock Split for {row['code']}")
		execute(conn,cur,sql)
	total = time.time() - start
	conn.close()
	print(f"""ALL DONE for {total} seconds""")
	return

if __name__ == "__main__":
	quantist_stocksplit_transformer()
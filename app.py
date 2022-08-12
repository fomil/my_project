# initial my course projectcd
# import packajes

import sqlite3
import pandas as pd
import shutil
import os

conn = sqlite3.connect('bank.db')
cursor = conn.cursor()

# init source data
print('Creating source data tables...')
with open('./sql_scripts/ddl_dml.sql', 'r', encoding='utf-8') as f:
    table = f.read()
    conn.executescript(table)
print("Done")

# conn.close()

# # import data rename and move to backup
def csv2sql(path, table_name, separatop=','):
	print('Import data from...'+ path)
	df = pd.read_csv(path, sep=separatop)
	df.to_sql(table_name, conn, if_exists = 'replace')
	new_path = './archive/' + path +'.backup'
	shutil.move(path, new_path)
	print("Done")

# # import data rename and move to backup
def xlsx2sql(path, table_name):
	print('Import data from...'+ path)
	df = pd.read_excel(path)
	df.to_sql(table_name, conn, if_exists = 'replace')
	new_path = './archive/' + path +'.backup'
	shutil.move(path, new_path)
	print("Done")

def showTable(table_name):
	cursor.execute('select * from ' + table_name)
	for row in cursor.fetchall():
		print(row)

def init_transactions():
	cursor.execute('''
		CREATE TABLE if not exists transactions(
			trans_id varchar(128),
			trans_date date,
			card_num varchar(128),
			open_type varchar(128),
			amt decimal,
			open_result varchar(128),
			terminal varchar(128)
			);
	''')

def init_passport_blacklist():
	cursor.execute('''
		CREATE TABLE if not exists passport_blacklist(
			passport_num varchar(128),
			entry_dt date
			);
	''')

def init_terminals():
	cursor.execute('''
		CREATE TABLE if not exists terminals(
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128)
			);
	''')


# def backup_file(path):
# 	new_path = './archive/' + path +'.backup'
# 	shutil.move(path, new_path)


init_transactions()
init_terminals()
init_passport_blacklist()


csv2sql('transactions_01032021.txt','transactions',';')
xlsx2sql('passport_blacklist_01032021.xlsx', 'passport_blacklist')
xlsx2sql('terminals_01032021.xlsx','terminals')

# showTable('cards')
# showTable('accounts')
# showTable('clients')
# showTable('transactions')
# showTable('passport_blacklist')
# showTable('terminals')


# initial my course projectcd

import sqlite3
import pandas as pd

conn = sqlite3.connect('bank.db')
cursor = conn.cursor()


def csv2sql(path, table_name, separatop=','):
	df = pd.read_csv(path, sep=separatop)
	df.to_sql(table_name, conn, if_exists = 'replace')

def xlsx2sql(path, table_name):
	df = pd.read_excel(path)
	df.to_sql(table_name, conn, if_exists = 'replace')

def showTable(table_name):
	cursor.execute('select * from ' + table_name)
	for row in cursor.fetchall():
		print(row)

def init_transactions_hist():
	cursor.execute('''
		CREATE TABLE if not exists transactions_hist(
			id integer primary key autoincrement,
			trans_id varchar(128),
			trans_date date,
			card_num varchar(128),
			open_type varchar(128),
			amt decimal,
			open_result varchar(128),
			terminal varchar(128)
			);
	''')

def init_passport_blacklist_hist():
	cursor.execute('''
		CREATE TABLE if not exists passport_blacklist_hist(
			id integer primary key autoincrement,
			passport_num varchar(128),
			entry_dt date
			);
	''')

def init_terminals_hist():
	cursor.execute('''
		CREATE TABLE if not exists terminals_hist(
			id integer primary key autoincrement,
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128)
			);
	''')

# initial sample tables in db

cursor.execute('''
	create table if not exists cards(
		card_num varchar(128), 
		account varchar(128), 
		create_dt date,
		update_dt date
	);
	''')


cursor.execute('''
	create table if not exists accounts(
		account varchar(128), 
		valid_to date, 
		client integer,
		create_dt date, 
		update_dt date
	);
''')

cursor.execute('''
	create table if not exists clients(
    	client_id integer, 
    	last_name varchar(128), 
    	first_name varchar(128), 
    	patronymic varchar(128), 
    	date_of_birth date, 
    	passport_num varchar(128), 
    	passport_valid_to date, 
    	phone varchar(128),
    	create_dt date, 
    	update_dt date
	);
''')

csv2sql('clients.csv','clients')
csv2sql('cards.csv','cards')
csv2sql('accounts.csv','accounts')
init_transactions_hist()
init_terminals_hist()
csv2sql('transactions_01032021.txt','transactions_hist',';')
xlsx2sql('passport_blacklist_01032021.xlsx', 'passport_blacklist_hist')
xlsx2sql('terminals_01032021.xlsx','terminals_hist')

showTable('cards')
showTable('accounts')
showTable('clients')
# showTable('transactions_hist')
showTable('passport_blacklist_hist')
showTable('terminals_hist')
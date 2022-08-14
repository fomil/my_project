# initial my course projectcd
# import packajes

import sqlite3
import pandas as pd
import shutil
import os

conn = sqlite3.connect('bank.db')
cursor = conn.cursor()

# init source data from sql_scripts
def init_source_data():
	print('Creating source data tables...')
	with open('./sql_scripts/ddl_dml.sql', 'r', encoding='utf-8') as f:
		table = f.read()
		conn.executescript(table)
		cursor.execute('ALTER TABLE cards RENAME TO STG_cards')
		cursor.execute('ALTER TABLE accounts RENAME TO STG_accounts')
		cursor.execute('ALTER TABLE clients RENAME TO STG_clients')
	print("Done")

# conn.close()

 	# import data rename and move to backup
def csv2sql(path, table_name, separatop=','):
	print('Import data from...'+ path)
	df = pd.read_csv(path, sep=separatop)
	df.to_sql(table_name, conn, if_exists = 'replace')
	new_path = './archive/' + path +'.backup'
	shutil.move(path, new_path)
	print("Done")

	# import data rename and move to backup
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
		CREATE TABLE if not exists DWH_FACT_transactions(
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
		CREATE TABLE if not exists DWH_FACT_passport_blacklist(
			passport_num varchar(128),
			entry_dt date
			);
	''')

def init_terminals():
	cursor.execute('''
		CREATE TABLE if not exists DWH_FACT_terminals(
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128)
			);
	''')

def init_reports():
	cursor.execute('''
		CREATE TABLE if not exists REP_FRAUD_HIST (
		event_dt date,
		passport_num varchar(128),
		FIO varchar(128),
		phone varchar(128),
		event_type varchar(128),
		report_dt date
			);
	''')

def scam_catcher_type_1():
	cursor.execute('''
		INSERT INTO REP_FRAUD_HIST(
			event_dt,
			passport_num,
			FIO,
			phone,
			event_type,
			report_dt
	) 	select 
     		transaction_date as event_dt,
     		passport_num,
     		last_name ||' '||first_name||' '||patronymic as FIO,
     		phone,
     		'overdue or blocked passport' as event_type,
    		date() as report_dt
 		from STG_clients cl
 		join STG_accounts ac
     		on cl.client_id=ac.client
 		join STG_cards ca
     		on ca.account=ac.account
 		join DWH_FACT_transactions tr
     		on tr.card_num=ca.card_num
 		left join DWH_FACT_passport_blacklist bl
     		on bl.passport=cl.passport_num
 		where tr.transaction_date>cl.passport_valid_to
 		or tr.transaction_date>bl.date
 		;
	''')

def scam_catcher_type_2():
	cursor.execute('''
		INSERT INTO REP_FRAUD_HIST(
			event_dt,
			passport_num,
			FIO,
			phone,
			event_type,
			report_dt
	) 	select
     		transaction_date as event_dt,
     		passport_num as passport,
     		last_name ||' '||first_name||' '||patronymic as FIO,
     		phone,
     		'unvalid acc' as event_type,
     		date() as report_dt
 		from STG_clients cl
 		join STG_accounts ac
     		on cl.client_id=ac.client
 		join STG_cards ca
     		on ca.account=ac.account
 		join DWH_FACT_transactions tr
     		on tr.card_num=ca.card_num
 		left join DWH_FACT_passport_blacklist bl
     		on bl.passport=cl.passport_num
 		where tr.transaction_date>ac.valid_to
 		;
	''')

def scam_catcher_type_3():
	cursor.execute('''
		INSERT INTO REP_FRAUD_HIST(
			event_dt,
			passport_num,
			FIO,
			phone,
			event_type,
			report_dt
	) 	with cte_pre as (
		select 
		     transaction_date as event_dt,
		     passport_num as passport,
		     last_name ||' '||first_name||' '||patronymic as FIO,
		     phone,
		     'different towns in less then hour' as event_type,
		     tr.transaction_date,
		     case     
		         when lag(terminal_city,1,null) over  (partition by cl.passport_num order by tr.transaction_date )!=terminal_city
	             and (julianday(tr.transaction_date) - julianday(lag(tr.transaction_date,1,null) over  (partition by cl.passport_num order by tr.transaction_date )) ) * 24 <1   
	             then 1
	             else 0 end IS_DIFF_CITY
		 from STG_clients cl
		 join STG_accounts ac
		     on cl.client_id=ac.client
		 join STG_cards ca
		     on ca.account=ac.account
		 join DWH_FACT_transactions tr
		     on tr.card_num=ca.card_num
		 join DWH_FACT_terminals te
		     on te.terminal_id=tr.terminal 
	     	order by 2,1)
	     	select   
	     		event_dt,
		    	passport,
		    	FIO,
		    	phone,
		    	'different towns in less then hour' as event_type,
		     	date() as report_dt
		    from cte_pre
	     where IS_DIFF_CITY=1
 		;
	''')



conn.commit()

# def backup_file(path):
# 	new_path = './archive/' + path +'.backup'
# 	shutil.move(path, new_path)


init_source_data()
init_transactions()
init_terminals()
init_passport_blacklist()
init_reports()

csv2sql('transactions_01032021.txt','DWH_FACT_transactions',';')
xlsx2sql('passport_blacklist_01032021.xlsx', 'DWH_FACT_passport_blacklist')
xlsx2sql('terminals_01032021.xlsx','terminals')
print('')
#scam_catcher_type_1()
#scam_catcher_type_2()
scam_catcher_type_3()
showTable('REP_FRAUD_HIST')
print(' start_analize_next_day >> ' * 5)


csv2sql('transactions_02032021.txt','DWH_FACT_transactions',';')
xlsx2sql('passport_blacklist_02032021.xlsx', 'DWH_FACT_passport_blacklist')
xlsx2sql('terminals_02032021.xlsx','DWH_FACT_terminals')
print('')
#scam_catcher_type_1()
#scam_catcher_type_2()
scam_catcher_type_3()
showTable('REP_FRAUD_HIST')
print(' start_analize_next_day >> ' * 5)



csv2sql('transactions_03032021.txt','DWH_FACT_transactions',';')
xlsx2sql('passport_blacklist_03032021.xlsx', 'DWH_FACT_passport_blacklist')
xlsx2sql('terminals_03032021.xlsx','DWH_FACT_terminals')
print('')
#scam_catcher_type_1()
#scam_catcher_type_2()
scam_catcher_type_3()
showTable('REP_FRAUD_HIST')
print(' >< finished_analize_last_day >< ' * 5)


# showTable('STG_cards')
# showTable('STG_accounts')
# showTable('STG_clients')
# showTable('DWH_FACT_transactions')
# showTable('DWH_FACT_passport_blacklist')
# showTable('DWH_FACT_terminals')
#showTable('REP_FRAUD_HIST')


# initial my course projectcd
# import packajes

import sqlite3
import pandas as pd
import shutil
import os

conn = sqlite3.connect('bank.db')
cursor = conn.cursor()

# init raw data from sql_scripts and transform table names

def init_source_data():
	print('Creating source data tables...')
	with open('./sql_scripts/ddl_dml.sql', 'r', encoding='utf-8') as f:
		table = f.read()
		conn.executescript(table)
		cursor.execute('ALTER TABLE cards RENAME TO STG_cards')
		cursor.execute('ALTER TABLE accounts RENAME TO STG_accounts')
		cursor.execute('ALTER TABLE clients RENAME TO STG_clients')
	print("Done successfully!")


 	# import csv raw data rename import files and move to backup
def csv2sql(path, table_name, separatop=','):
	print('Import data from file '+ path)
	df = pd.read_csv(path, sep=separatop)
	df.to_sql(table_name, conn, if_exists = 'replace')
	new_path = './archive/' + path +'.backup'
	shutil.move(path, new_path)
	print('Import successfully!')

	# import excel raw data rename import files and move to backup
def xlsx2sql(path, table_name):
	print('Import data from file '+ path)
	df = pd.read_excel(path)
	df.to_sql(table_name, conn, if_exists = 'replace')
	new_path = './archive/' + path +'.backup'
	shutil.move(path, new_path)
	print('Import successfully!')


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

def init_terminals_hist():
	cursor.execute('''
		CREATE TABLE if not exists DWH_DIM_terminals_HIST(
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128),
			deleted_flg integer default 0,
			effective_from datetime default current_timestamp,
			effective_to default (datetime('2999-12-31 23:59:59'))
			
			);
	''')
	cursor.execute('''
		CREATE VIEW if not exists v_terminals as
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address				
			from DWH_DIM_terminals_HIST
			where current_timestamp between effective_from and effective_to and deleted_flg = 0
		''')


def createnewRows():
	cursor.execute('''
		CREATE TABLE if not exists STG_new_rows as
			select
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			from STG_terminals t1
			left join v_terminals t2
			on t1.terminal_id = t2.terminal_id
			where t2.terminal_id is null
		''')

def createDeletedRows():
	cursor.execute('''
		CREATE TABLE if not exists STG_deleted_rows as
			select
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			from v_terminals t1
			left join STG_terminals t2
			on t1.terminal_id = t2.terminal_id
			where t2.terminal_id is null
		''')

def createChangedRows():
	cursor.execute('''
		CREATE TABLE if not exists STG_changed_rows as
			select
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			from STG_terminals t1
			inner join v_terminals t2
			on t1.terminal_id = t2.terminal_id
			and(   t1.terminal_id        <> t2.terminal_id 
				or t1.terminal_type      <> t2.terminal_type 
				or t1.terminal_city      <> t2.terminal_city 
				or t1.terminal_address   <> t2.terminal_address )
		''')

def update_terminals_hist():
	cursor.execute('''
		INSERT INTO DWH_DIM_terminals_HIST(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
	) 	select
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		from STG_new_rows
	''')

	cursor.execute('''
		UPDATE DWH_DIM_terminals_HIST
		set effective_to = datetime('now', '-1 second')
		where terminal_id in (select terminal_id from STG_changed_rows)
		and effective_to = datetime('2999-12-31 23:59:59')
		''')
	cursor.execute('''
		INSERT INTO DWH_DIM_terminals_HIST(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
	) 	select
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		from STG_changed_rows
	''')

	cursor.execute('''
		UPDATE DWH_DIM_terminals_HIST
		set effective_to = datetime('now', '-1 second')
		where terminal_id in (select terminal_id from STG_deleted_rows)
		and effective_to = datetime('2999-12-31 23:59:59')
		''')
	cursor.execute('''
		INSERT INTO DWH_DIM_terminals_HIST(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			deleted_flg
	) 	select
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			1
		from STG_deleted_rows
	''')

	conn.commit()

def showTable(table_name):
	cursor.execute('select * from ' + table_name)
	for row in cursor.fetchall():
		print(row)


def init_reports():
	cursor.execute('''
		CREATE TABLE if not exists REP_FRAUD (
		event_dt date,
		passport_num varchar(128),
		FIO varchar(128),
		phone varchar(128),
		event_type varchar(128),
		report_dt date
			);
	''')


def scam_catcher_type_1_1():
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport_num,
			FIO,
			phone,
			event_type,
			report_dt)
		select 
     		min(transaction_date) as event_dt,
     		passport_num,
     		last_name ||' '||first_name||' '||patronymic as FIO,
     		phone,
     		'Overdue passport' as event_type,
    		date() as report_dt
 		from STG_clients t1
 		join STG_accounts t2
     		on t1.client_id = t2.client
 		join STG_cards t3
     		on t3.account = t2.account
 		join DWH_FACT_transactions t4
     		on t4.card_num = t3.card_num
 		left join DWH_FACT_passport_blacklist t5
     		on t5.passport = t1.passport_num
 		where t4.transaction_date > t1.passport_valid_to
 		GROUP BY FIO
 		;
	''')

def scam_catcher_type_1_2():
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport_num,
			FIO,
			phone,
			event_type,
			report_dt)
		select 
     		min(transaction_date) as event_dt,
     		passport_num,
     		last_name ||' '||first_name||' '||patronymic as FIO,
     		phone,
     		'Blocked passport' as event_type,
    		date() as report_dt
 		from STG_clients t1
 		join STG_accounts t2
     		on t1.client_id = t2.client
 		join STG_cards t3
     		on t3.account = t2.account
 		join DWH_FACT_transactions t4
     		on t4.card_num = t3.card_num
 		left join DWH_FACT_passport_blacklist t5
     		on t5.passport = t1.passport_num
 		where t4.transaction_date > t5.date
 		GROUP BY FIO
 		;
	''')


def scam_catcher_type_2():
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport_num,
			FIO,
			phone,
			event_type,
			report_dt
	) 	select
     		min(transaction_date) as event_dt,
     		passport_num as passport,
     		last_name ||' '||first_name||' '||patronymic as FIO,
     		phone,
     		'Bank agreement not valid' as event_type,
     		date() as report_dt
 		from STG_clients t1
 		join STG_accounts t2
     		on t1.client_id = t2.client
 		join STG_cards t3
     		on t3.account = t2.account
 		join DWH_FACT_transactions t4
     		on t4.card_num = t3.card_num
 		left join DWH_FACT_passport_blacklist t5
     		on t5.passport = t1.passport_numcd 
 		where t4.transaction_date > t2.valid_to
 		GROUP BY FIO
 		;
	''')

def scam_catcher_type_3():
	cursor.execute('''
		INSERT INTO REP_FRAUD(
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
		     'Transactions in different cities in less than an hour' as event_type,
		     t4.transaction_date,
		     	case
		         when lag(terminal_city,1,null) over  (partition by t3.card_num order by t4.transaction_date )!= terminal_city
	             and (julianday(t4.transaction_date) - julianday(lag(t4.transaction_date,1,null) over  (partition by t3.card_num order by t4.transaction_date )) ) * 24 < 1   
	             	then 1
	             	else 0 end IS_DIFF_CITY
		 from STG_clients t1
		 join STG_accounts t2
		     on t1.client_id = t2.client
		 join STG_cards t3
		     on t3.account = t2.account
		 join DWH_FACT_transactions t4
		     on t4.card_num = t3.card_num
		 join DWH_DIM_terminals_HIST t6
		    on t6.terminal_id = t4.terminal 
	     	order by 2,1
	         	)
	     	select   
	     		event_dt,
		    	passport,
		    	FIO,
		    	phone,
		    	'Transactions in different cities in less than an hour' as event_type,
		     	date() as report_dt
		    from cte_pre
	     where IS_DIFF_CITY = 1
	     
 		;
	''')

def scam_catcher_type_4():
	cursor.execute('''
		INSERT INTO REP_FRAUD(
			event_dt,
			passport_num,
			FIO,
			phone,
			event_type,
			report_dt
	) 	with cte_pre as(
		select 
    		transaction_date as event_dt,
    		passport_num as passport,
    		last_name ||' '||first_name||' '||patronymic as FIO,
    		phone,
    		'selection of the amount with success in 20 minutes' as event_type,
    		t4.transaction_date,
    		t4.amount,
    		t4.oper_result,
  			lag(t4.oper_result ,1,null) over  (partition by t3.card_num order by t4.transaction_date ) oper_result_prev,
    		lag(t4.oper_result ,2,null) over  (partition by t3.card_num order by t4.transaction_date ) oper_result_prev2,
		    
    		case 
      			when 
        		lag(t4.oper_result ,1,null) over  (partition by t3.card_num order by t4.transaction_date ) = 'REJECT'
        		and lag(t4.oper_result ,2,null) over  (partition by t3.card_num order by t4.transaction_date )='REJECT'
        		and lag(t4.oper_result ,3,null) over  (partition by t3.card_num order by t4.transaction_date )='REJECT'
        		and  t4.oper_result='SUCCESS'
        		and  lag(t4.amount ,1,null) over  (partition by t3.card_num order by t4.transaction_date )-t4.amount>0
        		and lag(t4.amount ,2,null) over  (partition by t3.card_num order by t4.transaction_date )-lag(t4.amount ,1,null) over  (partition by t3.card_num order by t4.transaction_date ) >0
        		and lag(t4.amount ,3,null) over  (partition by t3.card_num order by t4.transaction_date )-lag(t4.amount ,2,null) over  (partition by t3.card_num order by t4.transaction_date ) >0
        		and (julianday(t4.transaction_date) - julianday(lag(t4.transaction_date,3,null) over  (partition by t3.card_num order by t4.transaction_date )) ) * 24*60 <20 
        			then 1
            		else 0 end IS_20_MIN
		from STG_clients t1
		join STG_accounts t2
    		on t1.client_id = t2.client
		join STG_cards t3
    		on t3.account = t2.account
		join DWH_FACT_transactions t4
    		on t4.card_num = t3.card_num
		join DWH_DIM_terminals_HIST t6
    		on t6.terminal_id = t4.terminal 
    		order by 2,1)
      		select   
      			event_dt,
    			passport,
    			FIO,
    			phone,
    			'selection of the amount with success in 20 minutes' as event_type,
    			date() as report_dt
     		from cte_pre
    		where IS_20_MIN = 1
    		
    		;
	''')	

conn.commit()

def delete_tmp_tables():
	cursor.execute('DROP TABLE if exists STG_terminals')
	cursor.execute('DROP TABLE if exists STG_new_rows')
	cursor.execute('DROP TABLE if exists STG_deleted_rows')
	cursor.execute('DROP TABLE if exists STG_changed_rows')
	print('All temp tables deleted successfully.')

# start and test area


init_source_data()
init_transactions()
init_terminals_hist()
init_passport_blacklist()
init_reports()

csv2sql('transactions_01032021.txt','DWH_FACT_transactions',';')
xlsx2sql('passport_blacklist_01032021.xlsx', 'DWH_FACT_passport_blacklist')
xlsx2sql('terminals_01032021.xlsx','STG_terminals')
print(' ')

createnewRows()
createDeletedRows()
createChangedRows()
update_terminals_hist()


print('_-new-_'*5)
showTable('STG_new_rows')
print('_-deleted-_'*5)
showTable('STG_deleted_rows')
print('_-changed-_'*5)
showTable('STG_changed_rows')


scam_catcher_type_1_1()
scam_catcher_type_1_2()
scam_catcher_type_2()
scam_catcher_type_3()
scam_catcher_type_4()
print('_-start report-_'*5)
showTable('REP_FRAUD')
print('_-end report-_'*5)
print(' ')
print('>> start_analize_next_day ' * 5)
print(' ')
delete_tmp_tables()

# add next day data

csv2sql('transactions_02032021.txt','DWH_FACT_transactions',';')
xlsx2sql('passport_blacklist_02032021.xlsx', 'DWH_FACT_passport_blacklist')
xlsx2sql('terminals_02032021.xlsx','STG_terminals')
print(' ')

createnewRows()
createDeletedRows()
createChangedRows()
update_terminals_hist()


print('_-new-_'*5)
showTable('STG_new_rows')
print('_-deleted-_'*5)
showTable('STG_deleted_rows')
print('_-changed-_'*5)
showTable('STG_changed_rows')


scam_catcher_type_1_1()
scam_catcher_type_1_2()
scam_catcher_type_2()
scam_catcher_type_3()
scam_catcher_type_4()
print('_-start report-_'*5)
showTable('REP_FRAUD')
print('_-end report-_'*5)
print(' ')
print('>> start_analize_next_day ' * 5)
print(' ')
delete_tmp_tables()

# add another day data 

csv2sql('transactions_03032021.txt','DWH_FACT_transactions',';')
xlsx2sql('passport_blacklist_03032021.xlsx', 'DWH_FACT_passport_blacklist')
xlsx2sql('terminals_03032021.xlsx','STG_terminals')
print(' ')

createnewRows()
createDeletedRows()
createChangedRows()
update_terminals_hist()


print('_-new-_'*5)
showTable('STG_new_rows')
print('_-deleted-_'*5)
showTable('STG_deleted_rows')
print('_-changed-_'*5)
showTable('STG_changed_rows')

scam_catcher_type_1_1()
scam_catcher_type_1_2()
scam_catcher_type_2()
scam_catcher_type_3()
scam_catcher_type_4()
print('_-start report-_'*5)
showTable('REP_FRAUD')
print('_-end report-_'*5)
print(' ')
print(' >< finished_analize_last_day >< ' * 5)

delete_tmp_tables()

# conn.commit()

# showTable('STG_cards')
# showTable('STG_accounts')
# showTable('STG_clients')
# showTable('DWH_FACT_transactions')
# showTable('DWH_FACT_passport_blacklist')
# showTable('DWH_DIM_terminals_HIST')
# showTable('REP_FRAUD')


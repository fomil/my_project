The result of the work.
An ETL process has been developed that receives daily data upload (test data is provided for 3 days), uploads it to the data warehouse and builds a report daily.
Uploading data.
The following files are provided daily:
List of transactions for the current day. Format - CSV.
List of terminals in full cut. The format is XLSX.
The list of passports included in the "black list" - cumulative from the beginning of the month.
The format is XLSX.
Information about cards, accounts and clients is stored in the sqlite database in the BANK schema in the tables STG_cards, STG_accounts and STG_clients
Storage structure.
Initial data:
STG_cards, STG_accounts and STG_clients
Fact tables:
DWH_FACT_transactions, DWH_FACT_passport_blacklist
SCD2 measurement tables:
DWH_DIM_terminals_HIST
Temporary DIM calculation tables:
STG_new_rows, STG_deleted_rows, STG_changed_rows
Report table:
REP_FRAUD
The report table is created according to the following requirements.
Based on the results of uploading new data, it is necessary to build a reporting mart on fraudulent transactions daily. The show-window is under construction accumulation, each new report keeps within the same table with new report_dt.
The window contains the following fields:
event_dt
The time of the event. If the event happened after
the result of several actions - the duration of the action is indicated,
for which fraud has been established.
Passport
The passport number of the client who committed the fraudulent
operation.
FIO
Surname First name Patronymic of the client who committed the fraudulent transaction.
phone
The phone number of the client who committed the fraudulent
operation.
event_type
Description of the type of fraud.
report_dt
The date and time the report was generated.

My algorithm identifies five types of potentially fraudulent transactions:
1_1 Performing an operation with an expired passport.
1_2 Performing an operation with a blocked passport.
2 Making a transaction with an invalid agreement with the bank.
3 Transactions in different cities within one hour.
4 Attempt to select the amount of the transaction. Within 20 minutes, more than 3 Operations take place with the following template
 - each subsequent one is less than the previous one, while all but the last one are rejected.
The last operation (successful) in such a chain is considered fraudulent.

16/08/2022 created by fomil
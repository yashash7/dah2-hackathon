import csv
import pandas as pd
import mysql.connector as my
from mysql.connector import Error

class database():
    def __init__(self):
        self.__host = "bzfznrwpgwfvpw8huacz-mysql.services.clever-cloud.com"
        self.__database="bzfznrwpgwfvpw8huacz"
        self.__user="uuia2kmq9adqfx1x"
        self.__password="sxSZi7lMOpcpaAkFDcX9"
        self.risk_h1 = {}
        self.risk_ml = {}
        self.incoming = {}
        self.outgoing = {}
        self.h4 = {}
        try:
            self.conn = my.connect(host = self.__host, database = self.__database, user = self.__user, password = self.__password)
            if self.conn.is_connected():
                self.cursor = self.conn.cursor()
                #cursor.execute("CREATE DATABASE employee")
                print("Database Connected Successfully.")
        except Error as e:
            print("Error While Connecting to MySQL", e)
    def __del__(self):
        
        self.cursor.close()
        self.conn.close()
        print("Connection Closed!")
    def db_retrieve(self):
        #conn = my.connect(host = self.__host, database = self.__database, user = self.__user, password = self.__password)
        #cursor = conn.cursor()
        query = 'select * from countries_info'
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        for i in res:
            print(i)

    def insert_countries_info(self):
        try:
            empdata = pd.read_csv('./csv/countries_info.csv', index_col=False, delimiter = ',')
            count = 0
            sql = "INSERT INTO countries_info VALUES (%s,%s)"
            print("Inserting Countries Info, Please Wait", end = '')
            for i,row in empdata.iterrows():
                #here %S means string values 
                self.cursor.execute(sql, tuple(row))
                count+=1
                print(".", end='')
                # the connection is not auto committed by default, so we must commit to save our changes
                self.conn.commit()
            print("\nInserted ",count, "Rows Successfully!")
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)

    def insert_customer_info(self):
        try:
            empdata = pd.read_csv('./csv/customer_info.csv', index_col=False, delimiter = ',')
            empdata['residential_country_cd'] = empdata['residential_country_cd'].fillna('NULL')
            #print(empdata.to_string())
            sql = "INSERT IGNORE INTO customer_info VALUES (%s,%s,STR_TO_DATE(%s, '%d/%m/%Y'))"
            count = 0
            print("Inserting Customer Info, Please Wait", end = '')
            for i,row in empdata.iterrows():
                #here %S means string values 
                self.cursor.execute(sql,tuple(row))
                count+=1
                print(".", end='')
                # the connection is not auto committed by default, so we must commit to save our changes
                self.conn.commit()
            print("\nInserted ",count, "Rows Successfully!")
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)

    def insert_customer_account_info(self):
        try:
            empdata = pd.read_csv('./csv/customer_account_info.csv', index_col=False, delimiter = ',')
            count = 0
            print("Inserting Customer Account Info, Please Wait", end = '')
            sql = "INSERT IGNORE INTO customer_account_info VALUES (%s,%s,STR_TO_DATE(%s, '%d/%m/%Y'))"
            for i,row in empdata.iterrows():
                #here %S means string values 
                self.cursor.execute(sql,tuple(row))
                count+=1
                print(".", end='')
                # the connection is not auto committed by default, so we must commit to save our changes
                self.conn.commit()
            print("\nInserted ",count, "Rows Successfully!")
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)

    def insert_customer_transactions(self):
        try:
            empdata = pd.read_csv('./csv/customer_transactions.csv', index_col=False, delimiter = ',')
            count = 0
            #print("Inserting Customer Transactions, Please Wait", end = '')
            sql = "INSERT IGNORE INTO customer_transactions(transfer_key, account_key, transaction_amount, transaction_type,transaction_location, transaction_date) VALUES (%s,%s,%f,%s,%s,STR_TO_DATE(%s, '%d/%m/%Y'))"
            ls = []
            for i,row in empdata.iterrows():
                ls.append(tuple(row))
            #print(ls[0])
            self.cursor.executemany(sql,ls)
            self.conn.commit()
        except Error as e:
            print("An Error Occured, Unable to Insert!", e)


    def get_h1(self):
        query = """SELECT account_key,COUNT(account_key)
FROM countries_info ci join customer_transactions ct
on ci.ENTITY_KEY=ct.transaction_location
GROUP BY YEAR(transaction_date),MONTH(transaction_date),account_key"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        #print(type(res))
        for i in res:
            if(self.risk_h1.get(i[0],False)):
                self.risk_h1[i[0]] = max(self.risk_h1[i[0]],i[1])
            else:
                self.risk_h1[i[0]] = i[1]

    def get_mlrisk(self):
        query = """SELECT account_key,COUNT(account_key)
FROM customer_transactions
GROUP BY YEAR(transaction_date),MONTH(transaction_date),account_key"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        #print(type(res))
        for i in res:
            if(self.risk_ml.get(i[0],False)):
                self.risk_ml[i[0]] = max(self.risk_ml[i[0]],i[1])
            else:
                self.risk_ml[i[0]] = i[1]

    def get_h2(self):
        query = """SELECT account_key,SUM(transaction_amount) as sum_amount from customer_transactions WHERE transaction_type='INN'
GROUP BY YEAR(transaction_date),MONTH(transaction_date),account_key"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        for i in res:
            if(self.incoming.get(i[0],False)):
                self.incoming[i[0]] = max(self.incoming[i[0]],i[1])
            else:
                self.incoming[i[0]] = i[1]

    def get_h3(self):
        query = """SELECT account_key,SUM(transaction_amount) as sum_amount from customer_transactions WHERE transaction_type='OUT'
GROUP BY YEAR(transaction_date),MONTH(transaction_date),account_key"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        for i in res:
            if(self.outgoing.get(i[0],False)):
                self.outgoing[i[0]] = max(self.outgoing[i[0]],i[1])
            else:
                self.outgoing[i[0]] = i[1]
    def get_h4(self):
        query = """SELECT account_key,COUNT(account_key) from customer_transactions 
GROUP BY YEAR(transaction_date),MONTH(transaction_date),DATE(transaction_date),account_key"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        for i in res:
            if(self.h4.get(i[0],False)):
                self.h4[i[0]] = max(self.h4[i[0]],i[1])
            else:
                self.h4[i[0]] = i[1]     
        
        

# Driver Code
mydb = database()
#mydb.insert_customer_transactions()
#mydb.db_retrieve()
data = pd.read_csv('./csv_data/customer_account_info.csv', index_col=False, delimiter = ',')
mydb.get_h1()
mydb.get_mlrisk()
mydb.get_h2()
mydb.get_h3()
mydb.get_h4()
#print(mydb.h4)
#print(mydb.incoming)
#print(mydb.outgoing)
#print(mydb.h4)
Risk = []
Remarks = []
for cust_id in data['account_key']:
    if(mydb.risk_h1.get(cust_id,False) and mydb.risk_h1[cust_id]>=10):
        Risk.append('High')
        Remarks.append( 'H1')
    elif(mydb.incoming.get(cust_id,False) and mydb.incoming[cust_id]>=1000):
        Risk.append('High')
        Remarks.append( 'H2')
    elif(mydb.outgoing.get(cust_id,False) and mydb.outgoing[cust_id]>=800):
        Risk.append('High')
        Remarks.append( 'H3')
    elif(mydb.h4.get(cust_id,False) and mydb.h4[cust_id]>=20):
        Risk.append('High')
        Remarks.append( 'H4')
    elif(mydb.risk_ml.get(cust_id,False) and mydb.risk_ml[cust_id]>=6):
        #M1
        Risk.append('Medium')
        Remarks.append( 'M1')
    elif(mydb.incoming.get(cust_id,False) and mydb.incoming[cust_id]>=600):
        Risk.append('Medium')
        Remarks.append( 'M2')
    elif(mydb.outgoing.get(cust_id,False) and mydb.outgoing[cust_id]>=500):
        Risk.append('Medium')
        Remarks.append( 'M3')
    elif(mydb.h4.get(cust_id,False) and mydb.h4[cust_id]>=10):
        Risk.append('Medium')
        Remarks.append( 'M4')
    elif(mydb.risk_ml.get(cust_id,False) and mydb.risk_ml[cust_id]<=10):
        #L1
        Risk.append('Low')
        Remarks.append( 'L1')
    elif(mydb.incoming.get(cust_id,False) and mydb.incoming[cust_id]<600):
        Risk.append('Low')
        Remarks.append( 'L2')
    elif(mydb.outgoing.get(cust_id,False) and mydb.outgoing[cust_id]<500):
        Risk.append('Low')
        Remarks.append( 'L3')
    else:
        Risk.append('Low')
        Remarks.append( 'L4')

data['Risk'] = Risk
data['Remarks'] = Remarks
del mydb
print(data.to_string())
__author__ = 'brucepannaman'

import MySQLdb
import os
import configparser
import csv
from subprocess import call
import psycopg2


config = configparser.ConfigParser()
ini = config.read('conf2.ini')

AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
HOST = config.get('Stat Slave Creds', 'host')
PORT = config.get('Stat Slave Creds', 'port')
USER = config.get('Stat Slave Creds', 'user')
PASSWORD = config.get('Stat Slave Creds', 'password')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')


# ALLPAGO SYMPHONY PAYMENTS

# Set up csv file to write data to
with open('allpago_symphony_payments.csv', 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')


    # Open database connection
    db = MySQLdb.connect(host =HOST, user =USER, passwd= PASSWORD, db ="busuudata")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    cursor.execute("select date(pay.created_at) as received, sub.id as order_id, pay.id as reference,pay.currency,  pay.amount,  sub.billing_period as package,  pay.type as provider,  pay.ip,  sub.user_id as uid,  sub.type as payment_type,  case when sub.cancelled_at is not null then true else false end as cancelled,  sub.cancelled_at as cancelled_at  from sfbusuudata.payment pay  inner join sfbusuudata.subscription sub on sub.id = pay.subscription_id;")
    data = cursor.fetchall()

    for row in data:
        writer.writerow(row)

    db.close()


# QIWI WALLET SYMPHONY PAYMENTS

# Set up csv file to write data to
with open('qiwi_symphony_payments.csv', 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')


    # Open database connection
    db = MySQLdb.connect(host =HOST, user =USER, passwd= PASSWORD, db ="busuudata")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    cursor.execute("select date(pay.created_at) as received, substring(sub.shopper_reference,12,15) as order_id, pay.psp_reference as reference, pay.currency, round(pay.amount/100),sub.billing_period as package, sub.selected_brand as provider, pay.ip as ip, sub.user_id as uid, 'Qiwi Wallet' as payment_type, case when sub.cancelled_at is not null then true else false end as cancelled,null as cancelled_at from sfbusuudata.adyen_payment pay inner join sfbusuudata.adyen_subscription sub on sub.id = pay.subscription_id where sub.status != 'unpaid_signup';")
    data = cursor.fetchall()

    for row in data:
        writer.writerow(row)

    db.close()

# UPLOAD SYMPHONY PAYMENTS TO REDSHIFT
print 'Uploading to s3'
call(["s3cmd", "put", 'qiwi_symphony_payments.csv',  "s3://bibusuu/symphony_payments/"])

print 'Uploading to s3'
call(["s3cmd", "put", 'allpago_symphony_payments.csv',  "s3://bibusuu/symphony_payments/"])

# deleting old payments
os.remove('allpago_symphony_payments.csv')
os.remove('qiwi_symphony_payments.csv')


# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

print "Deleting old table symphony_payments2"
cursor.execute("Drop table if exists symphony_payments2;")
print "Creating table symphony_payments2"
cursor.execute("create table symphony_payments2( received varchar(15), order_id varchar(100), reference varchar(100), currency varchar(5), amount decimal, billing_period varchar(30), provider varchar(20), ip varchar(30), uid int, payment_method varchar(50), cancelled boolean, cancelled_at timestamp ); ")
print "Adding Qiwi Data to symphony_payments2"
cursor.execute("COPY symphony_payments2  FROM 's3://bibusuu/symphony_payments/qiwi_symphony_payments.csv'  CREDENTIALS 'aws_access_key_id=AKIAITPOBFF7K7ZPLIRQ;aws_secret_access_key=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' CSV;")
print "Adding Allpago Data to symphony_payments2"
cursor.execute("COPY symphony_payments2  FROM 's3://bibusuu/symphony_payments/allpago_symphony_payments.csv'  CREDENTIALS 'aws_access_key_id=AKIAITPOBFF7K7ZPLIRQ;aws_secret_access_key=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' CSV;")
print "Deleting old table symphony_payments"
cursor.execute("Drop table if exists symphony_payments;")
print "Renaming table symphony_payments2 to symphony_payments"
cursor.execute("create table symphony_payments as select sym.* from symphony_payments2 sym left join ab_tests.allpago_boleto_failures_Jul_2016 hack on hack.order_id = sym.order_id where hack.order_id is null;")

# AGGREGATE SYMPHONY PAYMENTS WITH RECEIPTS AGGREGATED
print "Aggregating receipts_aggregated"
cursor.execute("drop table if exists receipts_aggregated2")
cursor.execute("create table receipts_aggregated2 as select * from receipts_aggregated union all select distinct MD5(reference) as receipt_id, order_id as order_id, uid, provider, payment_method as method, reference, amount, sp.currency, fx.rate as rate, round((amount/fx.rate),2) as eur_amount, (rank() over (partition by uid order by received asc)) -1 as recurring, received from symphony_payments sp inner join bs_exchange_rates fx on date((TIMESTAMP 'epoch' + fx.timestamp * INTERVAL '1 Second ')) = sp.received and fx.currency = sp.currency;")
cursor.execute("drop table if exists receipts_aggregated")
cursor.execute("alter table receipts_aggregated2 rename to receipts_aggregated;")

conn.commit()
conn.close()
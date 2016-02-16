__author__ = 'brucepannaman'

import MySQLdb
import datetime
import calendar
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

    cursor.execute("select date(pay.created_at), pay.currency, pay.amount, pay.type, pay.ip, sub.user_id, sub.type from sfbusuudata.payment pay inner join sfbusuudata.subscription sub on sub.id = pay.subscription_id;")
    data = cursor.fetchall()
    print data

    for row in data:
        writer.writerow(row)

    db.close()
    

# QIWI WALLET SYMPHONY PAYMENTS

# Set up csv file to write data to
with open('allpago_symphony_payments.csv', 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')


    # Open database connection
    db = MySQLdb.connect(host =HOST, user =USER, passwd= PASSWORD, db ="busuudata")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    cursor.execute("select date(pay.created_at), pay.currency, pay.amount, pay.type, pay.ip, sub.user_id, sub.type from sfbusuudata.payment pay inner join sfbusuudata.subscription sub on sub.id = pay.subscription_id;")
    data = cursor.fetchall()
    print data

    for row in data:
        writer.writerow(row)

    db.close()
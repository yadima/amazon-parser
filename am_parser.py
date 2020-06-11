#!/usr/bin/python3

import pandas as pd
import bs4 
import requests
import re
import locale
import datetime
import sys
from sqlalchemy import Column, Integer, String, MetaData, Table, DateTime, ForeignKey, Float, create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.sql import text


VERSION = 'v0.1 BETA'
LOGIN = "YOUR LOGIN"
PASSWORD = "YOUR PASSWORD"
API_KEY = "YOUR API KEY"
BASE_URL = "https://www.amazon.com/product-reviews/"
TAIL_URL = "/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"


locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' ) 

#Getting a file name as a parameter
inputfile = ''
try:
    if sys.argv[1] == '-i':
        inputfile = sys.argv[2]
    else:
        raise Exception("Invalid parameter!")
except Exception as e:
        print("Invalid parameter!")
        print("Usage: am_parser.py -i <inputfile.csv>")
        print(VERSION)
        sys.exit(2)

#Getting asins from a file
print("Getting asins from a file:", inputfile)
asins_df = pd.read_csv(inputfile, header=None, index_col=None)

print("Creating engine...")
engine = create_engine("postgres://" + LOGIN + ":" + PASSWORD + "@localhost:5432/amazon_parser")

#Create amazon_parser DB
print("Creating amazon_parser DB...")
if not database_exists(engine.url):
    create_database(engine.url)
print('DB amazon_parser exists:', database_exists(engine.url))

#Creating tables in the DB
print("Creating tables in the DB...")
metadata = MetaData(engine)
asins = Table('asins', metadata,
        Column('asin', String(15), primary_key=True, nullable=False),
        Column('created_at', DateTime, default=datetime.datetime.utcnow(), index=True),)
# If exists - implement the creation
asins.create(checkfirst=True)

product_info = Table('product_info', metadata,
        Column('id', Integer, primary_key=True, nullable=False),
        Column('asin_id', ForeignKey('asins.asin'), nullable=False),
        Column('created_at', DateTime, default=datetime.datetime.utcnow(), index=True),
        Column('title', String, nullable=False),
        Column('ratings', Integer, nullable=False),
        Column('average_rating', Float, nullable=False),)
# If exists - implement the creation
product_info.create(checkfirst=True)

reviews = Table('reviews', metadata,
        Column('id', Integer, primary_key=True, nullable=False),
        Column('asin_id', ForeignKey('asins.asin'), nullable=False),
        Column('created_at', DateTime, default=datetime.datetime.utcnow(), index=True),
        Column('review_number', Integer, nullable=False),
        Column('positive_reviews', Integer, nullable=False),
        Column('answered_questions', Integer, nullable=False),)
# If exists - implement the creation
reviews.create(checkfirst=True)

#Initializing variables
print("Initializing variables...")
asins_df.columns = ['asin']
asins_uniq = asins_df['asin'].unique()
db_connection = engine.connect()
current_datetime = datetime.datetime.utcnow()
asins_rows_amount = 0
product_info_rows_amount = 0
reviews_rows_amount = 0

#Processing asins
print("Processing asins...")
for asin in asins_uniq:
    print("*****************************")
    #Inserting rows into the "asin" table
    try:
        ins = asins.insert().values(asin=asin, created_at=current_datetime)
        result = db_connection.execute(ins)
        print('Inserted asin:', asin)
        asins_rows_amount += 1
    except IntegrityError:
        print('WARNING: asin already exists in the table, or something went wrong. Asin:', asin)
    except SQLAlchemyError as e:
        print('ERROR:', e, 'Asin:', asin)

    #Getting raw data from a page
    headers = { 
    "apikey": API_KEY
    }

    params = (
    ("url", BASE_URL + asin + TAIL_URL),
    )

    try:
        response = requests.get('https://app.zenscrape.com/api/v1/get', headers=headers, params=params)
        print("Getting raw data for asin:", asin, 'Response code:', response.status_code)
    except requests.exceptions.Timeout:
        print("ERROR: Timeout. Raw data for asin:", asin)
        continue
    except requests.exceptions.TooManyRedirects:
        print("ERROR: TooManyRedirects. Raw data for asin:", asin)
        continue
    except requests.exceptions.RequestException as e:
        print("ERROR: Unhandled exception. Raw data for asin:", asin, "Exception:", e)
        raise SystemExit(e)

    soup = bs4.BeautifulSoup(response.text, "lxml")

    #Parsing values for product_info
    title, ratings, average_rating = '0', '0', '0'
    try:
        title = soup.select('a[data-hook="product-link"]')[0].get_text()
        ratings = soup.find(text=re.compile('customer ratings')).split(' ')[0]
        average_rating = soup.select('i[data-hook="average-star-rating"]')[0].get_text().split(' ')[0]
    except Exception as e:
        print("ERROR: parsing product_info values for asin:", asin, "Exception:", e)
        print("Please, see the last response in the response.txt file.")
        open('response.txt', 'w').write(response.text)

    #Inserting rows into the "product_info" table
    try:
        ins = product_info.insert().values(asin_id=asin, title=title, ratings=locale.atoi(ratings), average_rating=average_rating, created_at=current_datetime)
        result = db_connection.execute(ins)
        print('Inserted product_info values, asin_id:', asin, '; title:', title, '; ratings:', ratings, '; average_rating:', average_rating, '; created_at:', current_datetime)
        product_info_rows_amount += 1
    except SQLAlchemyError as e:
        print('ERROR:', e, 'Asin:', asin)
    except Exception as e:
        print("ERROR: unhandled exception for asin:", asin, "Exception:", e)

    #Parsing values for reviews
    review_number, positive_reviews, answered_questions = '0', '0', '0'
    try:
        review_number = soup.select('span[data-hook="cr-filter-info-review-count"]')[0].get_text().split('of ')[1].split(' ')[0]
        positive_reviews = soup.find(text=re.compile('positive reviews')).split(' ')[2]
        answered_questions = soup.find(text=re.compile('answered questions')).split('See all ')[1].split(' ')[0].rstrip('+')
    except Exception as e:
        print("ERROR: parsing reviews values for asin:", asin, "Exception:", e)
        print("Please, see the last response in the response.txt file.")
        open('response.txt', 'w').write(response.text)

    #Inserting rows into the "reviews" table
    try:
        ins = reviews.insert().values(asin_id=asin, review_number=locale.atoi(review_number), positive_reviews=locale.atoi(positive_reviews), answered_questions=locale.atoi(answered_questions), created_at=current_datetime)
        result = db_connection.execute(ins)
        print('Inserted reviews values, asin_id:', asin, '; review_number:', review_number, '; positive_reviews:', positive_reviews, '; answered_questions:', answered_questions, '; created_at:', current_datetime)
        reviews_rows_amount += 1
    except SQLAlchemyError as e:
        print('ERROR:', e, 'Asin:', asin)
    except Exception as e:
         print("ERROR: unhandled exception for asin:", asin, "Exception:", e)

print('{} row(s) were inserted into asins table.'.format(asins_rows_amount))
print('{} row(s) were inserted into product_info table.'.format(product_info_rows_amount))
print('{} row(s) were inserted into reviews table.'.format(reviews_rows_amount))

db_connection.close()
engine.dispose()

print("Done!")
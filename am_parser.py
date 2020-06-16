#!/usr/bin/python3

import pandas as pd
import bs4 
import requests
import re
import locale
import datetime
import sys
import logging
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker, relationship


VERSION = 'v0.2 BETA'
LOGIN = "YOUR LOGIN"
PASSWORD = "YOUR PASSWORD"
API_KEY = "YOUR API KEY"
BASE_URL = "https://www.amazon.com/product-reviews/"
TAIL_URL = "/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"
SCHEMA_NAME = "amparser"
MAX_RETRY = 5


def log_productinfo_and_review_details(det_type, asin, e, response_text):
            print("ERROR: parsing", det_type, "for asin:", asin, "Exception:", e)
            print("Please, see the DEBUG level logging in the am_parser.log for the details.")
            logging.error('%s, parsing %s for asin: %s', e, det_type, asin)
            logging.debug('Response text: %s', response_text)


locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
logging.basicConfig(filename='am_parser.log', format='%(levelname)s:%(asctime)s %(message)s', level=logging.DEBUG)
logging.info('Started am_parser!')

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

if not engine.dialect.has_schema(engine, SCHEMA_NAME):
    engine.execute(CreateSchema(SCHEMA_NAME))

#Creating models and tables
Base = declarative_base()

class Asin(Base):
    """Data model for asins."""
    __tablename__ = "asins"
    __table_args__ = {"schema": SCHEMA_NAME}

    asin = Column(String(15),
                primary_key=True,
                unique=True,
                nullable=False)
    created_at = Column(DateTime, 
                default=datetime.datetime.utcnow(), 
                index=True)

    def __repr__(self):
        return "<Asin(asin='%s', created_at='%s')>" % (
                                self.asin, self.created_at)


class ProductInfo(Base):
    """Data model for product info."""
    __tablename__ = "product_info"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer,
                primary_key=True, 
                nullable=False)
    asin = Column(String(15),
                ForeignKey('amparser.asins.asin'),
                nullable=False)
    created_at = Column(DateTime, 
                default=datetime.datetime.utcnow(), 
                index=True)
    title = Column(String(255), 
                nullable=False)
    ratings = Column(Integer, 
                nullable=False)
    average_rating = Column(Float, 
                nullable=False)
    
    asin_rel = relationship("Asin", backref="productinfo_rel")

    def __repr__(self):
        return "<ProductInfo(asin='%s', created_at='%s', title='%s', ratings='%s', average_rating='%s')>" % (
                                self.asin, self.created_at, self.title, self.ratings, self.average_rating)


class Review(Base):
    """Data model for previews."""
    __tablename__ = "reviews"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer,
                primary_key=True, 
                nullable=False)
    asin = Column(String(15),
                ForeignKey('amparser.asins.asin'),
                nullable=False)
    created_at = Column(DateTime, 
                default=datetime.datetime.utcnow(), 
                index=True)
    review_number = Column(Integer, 
                nullable=False)
    positive_reviews = Column(Integer, 
                nullable=False)
    answered_questions = Column(Integer, 
                nullable=False)

    asin_rel = relationship("Asin", backref="reviews_rel")

    def __repr__(self):
        return "<Review(asin='%s', created_at='%s', review_number='%s', positive_reviews='%s', answered_questions='%s')>" % (
                                self.asin, self.created_at, self.review_number, self.positive_reviews, self.answered_questions)


#Initializing connection and variables
Base.metadata.create_all(engine)
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

print("Initializing variables...")
asins_df.columns = ['asin']
asins_uniq = asins_df['asin'].unique()
current_datetime = datetime.datetime.utcnow()
asins_rows_amount = 0
product_info_rows_amount = 0
reviews_rows_amount = 0

#Processing asins
print("Processing asins...")
for asin in asins_uniq:
    print("*****************************")
    retry = 0
    is_retry = True
    #Inserting rows into the "asin" table
    try:
        ins_asin = Asin(asin=asin, created_at=current_datetime)
        session.add(ins_asin)
        session.commit()
        print('Inserted asin:', asin)
        asins_rows_amount += 1
    except IntegrityError:
        print('WARNING: asin already exists in the table, or something went wrong. Asin:', asin)
        session.rollback()
        logging.warning('asin already exists in the table, or something went wrong. Asin: %s', asin)
    except SQLAlchemyError as e:
        print('ERROR:', e, 'Asin:', asin)
        session.rollback()
        logging.error('%s, Asin: %s', e, asin)

    #Getting raw data from a page
    headers = { 
    "apikey": API_KEY
    }

    params = (
    ("url", BASE_URL + asin + TAIL_URL),
    )

    while is_retry and retry <= MAX_RETRY:
        is_retry = False
        retry += 1
        try:
            print("Getting raw data for asin:", asin)
            response = requests.get('https://app.zenscrape.com/api/v1/get', headers=headers, params=params)
            print("Response code:", response.status_code)
            logging.info('Getting raw data for asin: %s. Response code: %s. Reason: %s.', asin, response.status_code, response.reason)
        except requests.exceptions.Timeout as e:
            print("ERROR: Timeout. Raw data for asin:", asin, "Retry attempt:", retry)
            is_retry = True
            logging.error('%s, Asin: %s. Retry attempt: %s', e, asin, retry)
            continue
        except requests.exceptions.RequestException as e:
            print("ERROR: Unhandled exception. Raw data for asin:", asin, "Exception:", e)
            print("Retry attempt:", retry)
            is_retry = True
            logging.error('%s, Asin: %s. Retry attempt: %s', e, asin, retry)
            logging.debug('Response text: %s', response.text)
            continue

        soup = bs4.BeautifulSoup(response.text, "lxml")

        #Parsing values for product_info
        title, ratings, average_rating = '0', '0', '0'
        try:
            title = soup.select('a[data-hook="product-link"]')[0].get_text()
        except Exception as e:
            log_productinfo_and_review_details('TITLE', asin, e, response.text)
            is_retry = True
            print('Retry attempt:', retry)
            logging.error('Retry attempt: %s', retry)
            continue

        try:
            ratings = soup.find(text=re.compile('customer ratings')).split(' ')[0]
        except Exception as e:
            log_productinfo_and_review_details('ratings', asin, e, response.text)

        try:
            average_rating = soup.select('i[data-hook="average-star-rating"]')[0].get_text().split(' ')[0]
        except Exception as e:
            log_productinfo_and_review_details('average_rating', asin, e, response.text)

        #Inserting rows into the "product_info" table
        try:
            ins_product_info = ProductInfo(asin=asin, title=title, ratings=locale.atoi(ratings), average_rating=average_rating, created_at=current_datetime)
            session.add(ins_product_info)
            session.commit()
            print('Inserted product_info values, asin_id:', asin, '; title:', title, '; ratings:', ratings, '; average_rating:', average_rating, '; created_at:', current_datetime)
            product_info_rows_amount += 1
        except SQLAlchemyError as e:
            print('ERROR:', e, 'Asin:', asin)
            session.rollback()
            logging.error('%s, Asin: %s.', e, asin)
        except Exception as e:
            print("ERROR: unhandled exception for asin:", asin, "Exception:", e)
            session.rollback()
            logging.error('unhandled exception %s, Asin: %s.', e, asin)

        #Parsing values for reviews
        review_number, positive_reviews, answered_questions = '0', '0', '0'
        try:
            review_number = soup.select('span[data-hook="cr-filter-info-review-count"]')[0].get_text().split('of ')[1].split(' ')[0]
        except Exception as e:
            log_productinfo_and_review_details('review_number', asin, e, response.text)

        try:
            positive_reviews = soup.find(text=re.compile('positive reviews')).split(' ')[2]
        except Exception as e:
            log_productinfo_and_review_details('positive_reviews', asin, e, response.text)

        try:
            answered_questions = soup.find(text=re.compile('answered questions')).split('See all ')[1].split(' ')[0].rstrip('+')
        except Exception as e:
            log_productinfo_and_review_details('answered_questions', asin, e, response.text)

        #Inserting rows into the "reviews" table
        try:
            ins_review = Review(asin=asin, review_number=locale.atoi(review_number), positive_reviews=locale.atoi(positive_reviews), answered_questions=locale.atoi(answered_questions), created_at=current_datetime)
            session.add(ins_review)
            session.commit()
            print('Inserted reviews values, asin_id:', asin, '; review_number:', review_number, '; positive_reviews:', positive_reviews, '; answered_questions:', answered_questions, '; created_at:', current_datetime)
            reviews_rows_amount += 1
        except SQLAlchemyError as e:
            print('ERROR:', e, 'Asin:', asin)
            session.rollback()
            logging.error('%s, Asin: %s.', e, asin)
        except Exception as e:
            print("ERROR: unhandled exception for asin:", asin, "Exception:", e)
            session.rollback()
            logging.error('unhandled exception %s, Asin: %s.', e, asin)

print('{} row(s) were inserted into asins table.'.format(asins_rows_amount))
print('{} row(s) were inserted into product_info table.'.format(product_info_rows_amount))
print('{} row(s) were inserted into reviews table.'.format(reviews_rows_amount))
logging.info('%s row(s) were inserted into asins table.', asins_rows_amount)
logging.info('%s row(s) were inserted into product_info table.', product_info_rows_amount)
logging.info('%s row(s) were inserted into reviews table.', reviews_rows_amount)

session.close()
engine.dispose()

logging.info('Finished am_parser! Done!')
print("Done!")
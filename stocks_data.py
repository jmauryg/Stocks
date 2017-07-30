#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spyder Editor

Use for inserting/updating stock data in mysql database

"""


from yahoo_finance import Share
import yahoo_finance
import mysql.connector
import csv
import urllib
from getpass import getpass

                
def get_stocks(filename):
    stocks = []
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)
        for row in reader:
            if row[0] not in stocks:
                stocks.append(row[0])
        return stocks
            
def db_login():
    
    username = input("Please type your username: ")
    pwd = getpass("Please type your password: ")
    cnx = mysql.connector.connect(user=username, password=pwd, host='localhost',database='stocks')

    cursor = cnx.cursor()
    
    return cnx, cursor

def db_login_credentials(username, pwd):
    
    cnx = mysql.connector.connect(user=username, password=pwd, host='localhost',database='stocks')

    cursor = cnx.cursor()
    
    return cnx, cursor
    
def db_logout(cnx, cursor):
    cursor.close()
    cnx.close()
    

def insert_into_db(cnx, cursor, stocks):
    add_stocks = ("INSERT INTO fundamentals "
               "(symbol, name, price, book_value, market_cap, ebitda, div_yield, earnings, moving_average_50, moving_average_200, earnings_ratio, earnings_ratio_growth, price_book_ratio, EPS_estimate_next_year, EPS_estimate_this_year, EPS_ratio_estimate_this_year, EPS_ratio_estimate_next_year) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )")

    for stock in stocks:
        #print(stock)
        try:
            stock_1 = Share(stock)    
            data = get_stock_data(stock, update=False)
            if stock_1.get_name() is not None and data is not None and stock_1.get_price() is not None:
                cursor.execute(add_stocks, data)
        except (RuntimeError, TypeError, NameError, urllib.error.HTTPError, yahoo_finance.YQLResponseMalformedError):
            continue
    cnx.commit()
    
def update_db(cnx, cursor, stocks):
    update_stocks = ("UPDATE fundamentals "
                    "SET name=%s, price=%s, book_value=%s, market_cap=%s, ebitda=%s, div_yield=%s, earnings=%s, moving_average_50=%s, moving_average_200=%s, earnings_ratio=%s, earnings_ratio_growth=%s, price_book_ratio=%s, EPS_estimate_next_year=%s, EPS_estimate_this_year=%s, EPS_ratio_estimate_this_year=%s, EPS_ratio_estimate_next_year=%s "
                    "WHERE symbol=%s")
    
    for stock in stocks:
        print(stock)
        try:
            stock_1 = Share(stock)
            data = get_stock_data(stock, update=True)
            if stock_1.get_name() is not None and data is not None and stock_1.get_price() is not None:
                cursor.execute(update_stocks, data)
        except (RuntimeError, TypeError, NameError, urllib.error.HTTPError, yahoo_finance.YQLResponseMalformedError):
            continue
    cnx.commit()

def get_stock_data(stock_symbol, update=True):
    stock = Share(stock_symbol)
    price = float(stock.get_price()) if stock.get_price() is not None and stock.get_price() != 'nan' else None
    book = float(stock.get_book_value()) if stock.get_book_value() is not None and stock.get_book_value() != 'nan' else None
    dividend = float(stock.get_dividend_yield()) if stock.get_dividend_yield() is not None and stock.get_dividend_yield() != 'nan' else None
    earnings = float(stock.get_earnings_share()) if stock.get_earnings_share() is not None and stock.get_earnings_share() != 'nan' else None
    moving50 = float(stock.get_50day_moving_avg()) if stock.get_50day_moving_avg() is not None and stock.get_50day_moving_avg() != 'nan' else None
    moving200 = float(stock.get_200day_moving_avg()) if stock.get_200day_moving_avg() is not None and stock.get_200day_moving_avg() != 'nan' else None
    earnings_ratio = float(stock.get_price_earnings_ratio()) if stock.get_price_earnings_ratio() is not None and stock.get_price_earnings_ratio() != 'nan'else None
    earnings_growth = float(stock.get_price_earnings_growth_ratio()) if stock.get_price_earnings_growth_ratio() is not None and stock.get_price_earnings_growth_ratio() != 'nan' else None
    book_ratio = float(stock.get_price_book()) if stock.get_price_book() is not None and stock.get_price_book() != 'nan' else None
    earnings_estimate_next = float(stock.get_EPS_estimate_next_year()) if stock.get_EPS_estimate_next_year() is not None and stock.get_EPS_estimate_next_year() != 'nan' else None
    earnings_estimate_this = float(stock.get_EPS_estimate_current_year()) if stock.get_EPS_estimate_current_year() is not None and stock.get_EPS_estimate_current_year() != 'nan' else None
    earnings_estimate_ratio_this = float(stock.get_price_EPS_estimate_current_year()) if stock.get_price_EPS_estimate_current_year() is not None and stock.get_price_EPS_estimate_current_year() != 'nan' else None
    earnings_estimate_ratio_next = float(stock.get_price_EPS_estimate_next_year()) if stock.get_price_EPS_estimate_next_year() is not None and stock.get_price_EPS_estimate_next_year() != 'nan' else None
    
    if update and stock.get_name() is not None:
        return (stock.get_name(), price, book, stock.get_market_cap(), stock.get_ebitda(), dividend, earnings, moving50, moving200, earnings_ratio, earnings_growth, book_ratio, earnings_estimate_next, earnings_estimate_this, earnings_estimate_ratio_this, earnings_estimate_ratio_next, stock_symbol)
    elif not update  and stock.get_name() is not None:
        return (stock_symbol, stock.get_name(), price, book, stock.get_market_cap(), stock.get_ebitda(), dividend, earnings, moving50, moving200, earnings_ratio, earnings_growth, book_ratio, earnings_estimate_next, earnings_estimate_this, earnings_estimate_ratio_this, earnings_estimate_ratio_next)
    
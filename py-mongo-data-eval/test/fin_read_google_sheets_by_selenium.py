import json

from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.firefox.options import Options
import pickle
import pandas
import datetime
from datetime import timedelta
import cx_Oracle
'''
gmail : dp-data@mydigipay.com
pass : D!G!P@Y1
'''
print('Selenium Starting....')
op = Options()
op.headless = True
##################op.headless = True
driver = webdriver.Firefox(options=op)
time.sleep(3)
driver.get('https://www.google.com')
# time.sleep(30)
# pickle.dump( driver.get_cookies() , open("fpa_gsheets.pkl","wb"))

cookie = pickle.load(open("../../finance/fpa_gsheets.pkl", "rb"))
print('Sucessfully Loading Cookies ...')
for cook in cookie:
    driver.add_cookie(cook)
driver.refresh()
time.sleep(3)
# driver.get('https://docs.google.com/spreadsheets/d/1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1-8/edit#gid=0')
# iframe contains emmbeded content
driver.get('https://docs.google.com/spreadsheets/u/0/d/1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1-8/preview/sheet?gid=0')
print('Successfully login to google account...')
page = driver.page_source
soup = BeautifulSoup(page, 'html.parser')

f = open("D:/tmp/a.html", "wb")
f.write(soup.prettify("utf-8"))
f.close()

tables = soup.findAll("table")
tableMatrix = []
for table in tables:
    #Here you can do whatever you want with the data! You can findAll table row headers, etc...
    list_of_rows = []
    for row in table.findAll('tr')[1:]:
        list_of_cells = []
        for cell in row.findAll('td'):
            text = cell.text.replace('&nbsp;', '')
            list_of_cells.append(text)
            print(text)
        list_of_rows.append(list_of_cells)
    tableMatrix.append((list_of_rows, list_of_cells))
print(tableMatrix[0][0][0])
print(json.dumps( tableMatrix))
print('Scraping data ...')




#for i,j in enumerate(tableMatrix[0][0][3:]):
#    print(j[11].replace(',','').replace('-',''))

# connection = cx_Oracle.connect(user="S_keyvanmehr", password='s_keyvanmehr1400',
#                                dsn="10.198.31.51/DGPORCLW",
#                                encoding="UTF-8")
# cursor = connection.cursor()
# SCRAP_DATE = datetime.datetime.now().strftime('%Y%m%d %H')
# scrap_gdate = datetime.datetime.now().strptime(SCRAP_DATE,'%Y%m%d %H')
# cursor.execute('delete mongodb.stg_bank_credit where scrap_date = \'{}\' '.format(SCRAP_DATE))
# for i,j in enumerate(tableMatrix[0][0][3:]):
#     print(scrap_gdate,
#           j[0].replace(',','').replace('-','').strip(),
#           j[2].replace(',','').replace('-','').strip(),
#           j[3].replace(',','').replace('-','').strip(),
#           j[4].replace(',','').replace('-','').strip(),
#           j[5].replace(',','').replace('-','').strip(),
#           j[6].replace(',','').replace('-','').strip()
#           )
#
#
#
# for i,j in enumerate(tableMatrix[0][0][3:]):
#     cursor.execute("insert into mongodb.stg_bank_credit values (:1, :2, :3, :4,:5,:6,:7,:8)" \
#                    ,(SCRAP_DATE,
#                      scrap_gdate,
#                      j[0].replace(',','').replace('-','').strip(),
#                      j[2].replace(',','').replace('-','').strip(),
#                      j[3].replace(',','').replace('-','').strip().replace('\u200f',''),
#                      j[4].replace(',','').replace('-','').strip().replace('\u200f',''),
#                      j[5].replace(',','').replace('-','').strip().replace('\u200f',''),
#                      j[6].replace(',','').replace('-','').strip().replace('\u200f','')
#                      ))



print('Successfully loding data to database...')
# connection.commit()
# connection.close()
driver.close()

import mysql.connector
import requests
import json
from datetime import datetime
import logging
import time

# get now time
times = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

# use logging
logfile = 'LOG_' + datetime.today().strftime('%d_%m_%Y') + '.log'
logging.basicConfig(filename="../"+logfile, format='%(asctime)s %(message)s', level=logging.DEBUG)

while(True) :

    # database connect
    print(times, ' - Database connecting...')
    logging.info(' - Database connecting...')
    ENDPOINT="ls-90ab2b006c3baed894f9e24c1274ec64317e4832.cf8xnfszniyg.us-east-1.rds.amazonaws.com"
    PORT="3306"
    USER="kic_dba"
    PASSWORD="Futalaufquen2022"
    DBNAME="dbmaster"
    try:
        conn =  mysql.connector.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, port=PORT, database=DBNAME) #, ssl_ca='SSLCERTIFICATE'
        cur = conn.cursor()
        print(cur)
    except mysql.connector.Error as e:
        print(times, " - Database connection failed due to {}".format(e))
        logging.info(' - Database connection failed due to {}'.format(e))
        exit()
    # search pending jobs
    print(times, " - Searching for pending jobs.")
    logging.info(" - Searching for pending jobs.")
    query_results = []
    i = 0
    while(len(query_results) == 0):
        i = i + 1
        try:
            cur.execute("SELECT ScraperJob_ID, Sitemap_ID, Sitemap_Name FROM KIC_TB_Scraper_Jobs WHERE ScraperJob_Status = 'Finished' AND ScraperJob_Action = 1 ORDER BY Datetime ASC LIMIT 1")
            query_results = cur.fetchall()
            ScraperJob_ID = str(query_results[0][0])
            Sitemap_ID = str(query_results[0][1])
            Sitemap_Name = str(query_results[0][2])
        except:# done!
            print("Search "+ str(i)+"th failed, will search again.")
            logging.info("Search ", str(i), "th failed, will search again.")
            query_results = []
            continue

    # get json data
    print(times, ' - processing ScraperJob_ID : ', ScraperJob_ID,'.')
    logging.info(' - processing ScraperJob_ID : ' + ScraperJob_ID + '.')
    URL= "https://api.webscraper.io/api/v1/scraping-job/"+ScraperJob_ID+"/json"
    PARAMS = { "api_token":"XIYXn0oxef5A0pLEZYPBvaFyNnIGswFr3v4bM2i4OQpoozIC8R7finbEjCCM" }
    json_array = requests.get( URL, PARAMS ).text.replace('\n', '').replace('}{', '},{')
    json_array = "[" + json_array + "]"
    json_array = json.loads(json_array)

    # insert json data into database
    total_i = len(json_array)
    if(total_i != 1):
        try:
            i = 0
            for json_item in json_array:
                i = i + 1
                cur.callproc('KIC_SP_Property_Temp_Readings_Add2', (
                    json_item['web-scraper-order'], json_item['time-scraped'], ScraperJob_ID, Sitemap_ID, Sitemap_Name, json_item['propertyID'], json_item['type'],json_item['URL'],json_item['imagesAll'],  json_item['latitude'], json_item['longitude'], json_item['addressCity'], json_item['addressPostcode']  , json_item['addressStreet'], json_item['addressStreetNumber'], json_item['yearOfConstruction'], json_item['salePrice'], json_item['roomsQty'], json_item['habitableSquareMeters'], json_item['landSquareMeters'], json_item['usefulSquareMeters'], json_item['cubicMeters'], json_item['description'], json_item['features'], json_item['publishSince'], json_item['availableSince'])
                )
                print(times, " - propertyID(", json_item['propertyID'],") inserted correctly in temp readings table. Record ", i, "/", total_i, ".")
                logging.info(" - propertyID(" + json_item['propertyID'] + ") inserted correctly in temp readings table. Record " + str(i) + "/" + str(total_i) + ".")
            cur.execute("UPDATE KIC_TB_Scraper_Jobs SET ScraperJob_Action = '2' WHERE ScraperJob_ID = '" + ScraperJob_ID + "'")
            print(times, " - ScraperJob_ID(", ScraperJob_ID, ") processed correctly.")
            logging.info(" - ScraperJob_ID(" + ScraperJob_ID + ") processed correctly.")
            conn.commit()
        except mysql.connector.Error as e:
            cur.execute("UPDATE KIC_TB_Scraper_Jobs SET ScraperJob_Action = '4' WHERE ScraperJob_ID = '" + ScraperJob_ID + "'")
            conn.rollback()
            print("Failed to update record to database rollback")
            print(times, ' - Json data of ScraperJob_ID(', ScraperJob_ID, ') is not exist' )
            logging.info(' - Json data of ScraperJob_ID(' + ScraperJob_ID + ') is not exist')
        finally:
            # closing database connection.
            if conn.is_connected():
                cur.close()
                conn.close()
                print(times, " - database connection is closed")
                logging.info(" -  database connection is closed")
    else:
        print(times, ' - Get Json data of ScraperJob_ID(', ScraperJob_ID, ') Failed.')
        logging.info(' - Get Json data of ScraperJob_ID(' + ScraperJob_ID + ') Failed.')

    print(times, " - Waiting ...\n after 30min, it will working.....")
    time.sleep(1800)

    
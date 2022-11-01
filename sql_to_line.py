# sql_to_line.py
# segment to line but only in postgis skipping geopandas entirely.
# right now this operates in NAD83 BC Albers (CRS 3005). Probably best practice for this local area, however if we want to scale this up we'll need to revisit this.

# currently using a temp database-- this is important because data is coming from lat/long so we need to create the temp as crs 4326, create the geometry then convert back to 3005 and insert into the main databse

# LAST UPDATED 2022-11-01 --> Moved to public repo thereafter

import logging
from datetime import datetime
import psycopg2 as pg

# auth_class contains the database info so alter this depending on the target database
import auth_class

#starting time of script
start_time = datetime.now()

# database connection
conn = pg.connect(host=auth_class.login.host,
                  port=auth_class.login.port,
                  dbname=auth_class.login.db,
                  user=auth_class.login.user,
                  password=auth_class.login.pw,
                  options='-c search_path=dbo,' + str(auth_class.login.schem))  # sets schema to public

def main():
    global conn

    # this is an assumption that all new or needing updating segments will not have any geom at this point --> must be careful here because this won't realize if segments need to be deleted
    cursor = conn.cursor()
    query = "SELECT segmentid FROM " + auth_class.login.inputDb + " WHERE geom IS NULL"
    cursor.execute(query)
    # Maybe alter the above query to also check for lenm and sogkt being 'null' as the assumption those would have to be updated as well?

    result = list(row[0] for row in cursor.fetchall())
    segList = []
    x = 0
    for i in result:
        segList.append(int(result[x]))
        x += 1

    # if there's nothing to update the program shuts off here
    if not segList:
        logger(False, 'empty')

    else:
        # select dates that occur within the segments being updated
        cursor = conn.cursor()
        query = "SELECT starttime FROM " + auth_class.login.inputDb + " WHERE segmentid IN ({})".format(str(segList)[1:-1])
        cursor.execute(query)
        result = cursor.fetchall()

        # loop to convert datetime to date in a list
        dateList = []
        x = 0
        for i in result:
            dateList.append(result[x][0].strftime("'%Y-%m-%d'"))
            x += 1

        # only keeps unique dates to prevent unnecessary loops
        dateList = set(dateList)

        # creates dictionary that stores pairings of dates and their associated segmentid's
        resultsDict = {}

        for dates in dateList:
            # deletes previous temp database if exists and creates new one
            temper(conn)

            # inserts the queried data into the temp table
            temp_inserter(conn, dates, segList)

            # where the geographic magic happens -- change this to sql
            geoger(conn)

            # inserts data back into sql and then cleans temp
            sql_tabler(conn)

            # adds the days that have been processed to a list to be logged
            cursor = conn.cursor()
            query = 'SELECT segmentid FROM ' + auth_class.login.tempDb
            cursor.execute(query)
            result = cursor.fetchall()

            theDate = datetime.strptime(dates, "'%Y-%m-%d'")

            dailyList = []
            for row in result:
                dailyList.append(row[0])
            resultsDict[str(theDate.date())] = dailyList

            # commits work to database between each day in case there is interruptions not all work is lost
            conn.commit()

        # logs the line id's that were updated during this run of the program
        logger(True, resultsDict)

    # close connection to database which also saves changes
    conn.close()


def temp_inserter(conn, date, segList):
    # moves the selected data from the main database into the temp database
    cursor = conn.cursor()

    sql = 'INSERT INTO ' + auth_class.login.tempDb + \
            ' SELECT * FROM ' + auth_class.login.inputDb + \
            ' WHERE CAST(starttime AS DATE)  = ' + date + \
            ' AND segmentid IN ({})'.format(str(segList)[1:-1])

    cursor.execute(sql)


def temper(conn):
    # this functionality checks if a temp folder already exists and drops the existing one if it does -- regardless the script will create a blank temp folder
    exists = False
    try:
        cursor = conn.cursor()
        cursor.execute("select exists(select relname from pg_class where relname='" + auth_class.login.tempDb + "')")
        exists = cursor.fetchone()[0]
        cursor.close()
    finally:
        pass

    # deletes existing temp table if exists is TRUE
    if exists == True:
        cursor = conn.cursor()
        sql = 'DROP TABLE ' + auth_class.login.tempDb
        cursor.execute(sql)
    else:
        pass

    # create temp table
    cursor = conn.cursor()
    sql =   ('CREATE TABLE ' + auth_class.login.tempDb + ' ' +
            '(segmentId BIGINT PRIMARY KEY,' +
            'uid BIGINT NOT NULL,' +
            'mmsi INT NOT NULL,' +
            'startTime TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
            'duration INT NOT NULL,' +
            'startLat FLOAT NOT NULL,' +
            'startLon FLOAT NOT NULL,' +
            'endLat FLOAT NOT NULL,' +
            'endLon FLOAT NOT NULL,' +
            'isClassA BOOL NOT NULL,' +
            'classAIS SMALLINT NOT NULL,' +
            'classGen SMALLINT NOT NULL,' +
            'name VARCHAR(20),' +
            'isUnique BOOL NOT NULL,' +
            'lastChange TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
            'geom GEOMETRY (LineString, 4326),' +
            'lenM FLOAT,' +
            'sogKt FLOAT)')

    cursor.execute(sql)


def geoger(conn):
    cursor = conn.cursor()

    # creates geometry in the temporary database
    sql = 'UPDATE ' + auth_class.login.tempDb + ' SET geom = ST_SetSRID(ST_MakeLine(ST_MakePoint(startlon, startlat), ST_MakePoint(endlon, endlat)), 4326);'
    cursor.execute(sql)

    # converts data to 3005
    sql = 'ALTER TABLE ' + auth_class.login.tempDb + ' ALTER COLUMN geom TYPE Geometry(LineString, 3005) USING ST_Transform(geom, 3005)'
    cursor.execute(sql)

    # calculates the length
    sql = 'UPDATE ' + auth_class.login.tempDb + ' network SET lenm = ST_length(geom)'
    cursor.execute(sql)

    # calculates the speed over ground by taking the length in meters, dividing by the duration in seconds and then multiplying by 1.94384 to get knots.
    sql = 'UPDATE ' + auth_class.login.tempDb + ' SET sogkt = (lenm / duration) * 1.94384'
    cursor.execute(sql)


def sql_tabler(conn):
    cursor = conn.cursor()

    cursor.execute('UPDATE ' + auth_class.login.inputDb + ' AS a SET ' +
                'geom = b.geom, ' +
                'lenm = b.lenm, ' +
                'sogkt = b.sogkt ' +
                'FROM ' + auth_class.login.tempDb + ' AS b WHERE a.segmentid = b.segmentid')


def logger(happenings, resultsList): # this still needs lot of work.......
    global start_time
    global conn

    # configure logger --> this configures for the log file but the console output is handled separately
    logger = logging.getLogger('log')
    logging.root.setLevel(logging.NOTSET)
    # logging.basicConfig(filename='seg_to_line_LOG.log', format="%(asctime)s;%(levelname)s;%(message)s",datefmt='%Y-%m-%d %H:%M:%S')
    logging.basicConfig(filename='seg_to_line_LOG.log')

    now = datetime.now()
    duration = (now - start_time)

    if happenings == False:
        logger.info("Nothing to update.. \nCompleted: " + now.strftime("%d/%m/%Y %H:%M:%S") + "\nRuntime = " + str(duration))

        print('\n--------------------------------------------------------------------------------\n\nNothing to update..',
              "\nCompleted: ", now.strftime("%d/%m/%Y %H:%M:%S"), "\nRuntime = ", duration,"\n\n--------------------------------------------------------------------------------\n")

    else:
        logger.info("--------------------------------------------------------------------------------")
        logger.info('%s', resultsList)
        logger.info("   Completed: "+ now.strftime("%d/%m/%Y %H:%M:%S")+ "  Runtime = "+ str(duration))

        print('\n--------------------------------------------------------------------------------\n')
        for key, value in resultsList.items():
            print(key, ' : ', value)
        print("\n* listed line segments have been added or updated\nCompleted: ", now.strftime("%d/%m/%Y %H:%M:%S"),
              "\nRuntime = ", duration,
              "\n\n--------------------------------------------------------------------------------\n")


if __name__ == "__main__":
    main()

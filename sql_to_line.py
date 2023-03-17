# sql_to_line.py
# segment to line but only in postgis skipping geopandas entirely.
# right now this operates in NAD83 BC Albers (CRS 3005). Probably best practice for this local area, however if we want to scale this up we'll need to revisit this.

# currently using a temp database-- this is important because data is coming from lat/long so we need to create the temp as crs 4326, create the geometry then convert back to 3005 and insert into the main database

# LAST UPDATED 2022-11-02 --> Removing logger method and instead including it in the main. This allows for a better log that can see the progress in the event the script shuts down prematurely

import logging
from datetime import datetime
import psycopg2 as pg

# auth_class contains the database info so alter this depending on the target database
import auth_class

# establish database connection
conn = pg.connect(host=auth_class.login.host,
                  port=auth_class.login.port,
                  dbname=auth_class.login.db,
                  user=auth_class.login.user,
                  password=auth_class.login.pw,
                  options='-c search_path=dbo,' + str(auth_class.login.schem))  # sets schema to public

def main():
    global conn

    # starting time of script
    start_time = datetime.now()

    # initialize logger
    logger = logging.getLogger('log')
    logging.root.setLevel(logging.NOTSET)
    logging.basicConfig(filename='seg_to_line_LOG.log', format="%(asctime)s;%(levelname)s;%(message)s",datefmt='%Y-%m-%d %H:%M:%S')

    # first quick logger entry to determine start time of logger for records
    logger.debug('Program Start..')

    # this is an assumption that all new or needing updating segments will not have any geom at this point --> must be careful here because this won't realize if segments need to be deleted
    cursor = conn.cursor()

    sql = "SELECT segmentid FROM " + auth_class.login.inputDb + " WHERE geom IS NULL"
    cursor.execute(sql)
    # Maybe alter the above query to also check for lenm and sogkt being 'null' as the assumption those would have to be updated as well?

    result = list(row[0] for row in cursor.fetchall())
    segList = []
    x = 0
    for i in result:
        segList.append(int(result[x]))
        x += 1

    # if there's nothing to update the program shuts off here
    if not segList:
        # Just logging info to know when things were last run
        now = datetime.now()
        duration = (now - start_time)

        logger.info(
            "Nothing to update.. \nCompleted: " + now.strftime("%d/%m/%Y %H:%M:%S") + "\nRuntime = " + str(duration))

        print('\n--------------------------------------------------------------------------------\n\nNothing to update..',
            "\nCompleted: ", now.strftime("%d/%m/%Y %H:%M:%S"), "\nRuntime = ", duration,
            "\n\n--------------------------------------------------------------------------------\n")

    else:
        # start logging and console
        now = datetime.now()
        duration = (now - start_time)
        print('\n--------------------------------------------------------------------------------\n')

        filter_data(conn)

        # select dates that occur within the segments being updated
        cursor = conn.cursor()
        sql = "SELECT starttime FROM " + auth_class.login.inputDb + " WHERE segmentid IN ({})".format(str(segList)[1:-1])

        cursor.execute(sql)
        result = cursor.fetchall()

        # loop to convert datetime to date in a list
        dateList = []
        x = 0
        for i in result:
            dateList.append(result[x][0].strftime("'%Y-%m-%d'"))
            x += 1

        # only keeps unique dates to prevent unnecessary loops
        dateList = set(dateList)


        for dates in dateList:
            # creates dictionary that stores pairings of dates and their associated segmentid's
            # -- note that this has been moved into this loop as the dictionary would be completely gone otherwise in the event of a premature program exit. This way we can see progress in the log.
            resultsDict = {}

            # deletes previous temp database if exists and creates new one
            print("temper")
            temper(conn)

            # inserts the queried data into the temp table
            print("inserter")
            temp_inserter(conn, dates, segList)

            # where the geographic magic happens -- change this to sql
            print("geoger")
            geoger(conn)

            # inserts data back into sql and then cleans temp
            print("tabler")
            sql_tabler(conn)

            # adds the days that have been processed to a list to be logged
            cursor = conn.cursor()
            sql = 'SELECT segmentid FROM ' + auth_class.login.tempDb
            cursor.execute(sql)
            result = cursor.fetchall()

            theDate = datetime.strptime(dates, "'%Y-%m-%d'")

            dailyList = []
            for row in result:
                dailyList.append(row[0])
            resultsDict[str(theDate.date())] = dailyList

            # commits work to database between each day in case there is interruptions not all work is lost
            conn.commit()

            print(theDate.date(), '  - - -  ', len(dailyList))

            # logs the line id's that were updated during this run of the program
            logger.info(resultsDict)

        # end print statement and end time + duration log
        logger.info("   Completed: " + now.strftime("%d/%m/%Y %H:%M:%S") + "  Runtime = " + str(duration))

        print("\n* listed dates and segment counts have been added or updated\nCompleted: ", now.strftime("%d/%m/%Y %H:%M:%S"),
              "\nRuntime = ", duration,
              "\n\n--------------------------------------------------------------------------------\n")

    # closes connection to database which also saves changes
    conn.close()


def filter_data(conn):
    # method to filter out any unwanted data moving forward that is missed earlier. In this case there is some zero duration ais lines which messes with SOG and is clearly an error.
    cursor=conn.cursor()
    sql = "DELETE FROM " + auth_class.login.inputDb + " WHERE duration = 0"
    cursor.execute(sql)
    conn.commit()

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
        # turns exists to True if a temp table already exists in the database
        exists = cursor.fetchone()[0]
    finally:
        pass

    # deletes existing temp table if exists is TRUE
    if exists == True:
        cursor = conn.cursor()
        sql = 'DROP TABLE ' + auth_class.login.tempDb
        cursor.execute(sql)
    else:
        pass

    # create temp table in database
    # -- note that this create table statement is slightly different from the main databases create table statement as the geom/geometry column is in crs 4326 instead of 3005. Alter this to 3005 for input table create statement.
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
    # creates geometry from start/end lat/long and converts into 3005, then determines length and speed over ground.

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
    # updates the main database with the data from the temp database on segmentid
    cursor = conn.cursor()
    cursor.execute('UPDATE ' + auth_class.login.inputDb + ' AS a SET ' +
                'geom = b.geom, ' +
                'lenm = b.lenm, ' +
                'sogkt = b.sogkt ' +
                'FROM ' + auth_class.login.tempDb + ' AS b WHERE a.segmentid = b.segmentid')

    sql = ('DROP INDEX IF EXISTS idx')
    cursor.execute(sql)
    sql = ('CREATE INDEX idx ON temp USING gist (geom)')
    cursor.execute(sql)


if __name__ == "__main__":
    main()

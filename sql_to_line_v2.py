# sql_to_line_V2.py
# vastly simplified- no logging no looping etc. etc.

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

    # deletes previous temp database if exists and creates new one
    print("temper")
    temper(conn)

    # inserts the queried data into the temp table
    print("inserter")
    temp_inserter(conn)

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

    # commits work to database between each day in case there is interruptions not all work is lost
    conn.commit()

    # closes connection to database which also saves changes
    conn.close()


def filter_data(conn):
    # method to filter out any unwanted data moving forward that is missed earlier. In this case there is some zero duration ais lines which messes with SOG and is clearly an error.
    cursor=conn.cursor()
    sql = "DELETE FROM " + auth_class.login.inputDb + " WHERE duration = 0"
    cursor.execute(sql)
    conn.commit()

def temp_inserter(conn):
    # moves the selected data from the main database into the temp database
    cursor = conn.cursor()
    sql = 'INSERT INTO ' + auth_class.login.tempDb + ' SELECT * FROM ' + auth_class.login.inputDb + ' WHERE geom is NULL'

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

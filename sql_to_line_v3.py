# sql_to_line_v3.py
# 2025-08-25

# redone for 3rd iteration of data

# going to start this one without auth_class

# must be run AFTER 'decoder_v3_to_segments_2025-10-14.py'

from datetime import datetime
import psycopg2 as pg

# --- DATABASE CONFIGURATION ---
# fill in database info below
dbhost = "localhost"
dbport = 5433
dbname = "ais_data"
dbuser = "postgres"
dbpass = "postgres"

# --- TABLE SELECT ---
# select schema year and then months within year to process
year = 2024
months = ["07"]
#months = ["01","02","03","04","05","06","07","08","09","10","11","12"]

# --- PYTHON CONNECT TO POSTGRESQL CURSOR ---
conn = pg.connect(host=dbhost,
                  port=dbport,
                  dbname=dbname,
                  user=dbuser,
                  password=dbpass)

# --- OTHER OPTIONS ---
desired_crs = 3005



def main():
    start_time = datetime.now()

    schema_tables = tableLister(year, months)

    for t in schema_tables:
        table_start_time = datetime.now()

        geoger(t, desired_crs)
        create_spatial_index(t)

        table_finish_time = datetime.now()
        table_run_duration = (table_finish_time - table_start_time)
        print(f"""{t} run time: {table_run_duration}""")

    now = datetime.now()
    duration = (now - start_time)
    print(f"""Total run time: {duration}""")


def tableLister(year, months):
    schema = f"year{year}"
    month_strs = [f"{int(m):02d}" for m in months]
    #table_names = [f"segments_{year}_{m}_day" for m in month_strs]
    table_names = [f"segments_{year}_{m}" for m in month_strs]
    schema_table_name = [f"{schema}.{t}" for t in table_names]

    return schema_table_name


def geoger(table, crs):
    print(f"""Starting: {table}""")
    cursor = conn.cursor()

    # delete duration 0 segments as they shouldn't exist (also messes with SOG)
    cursor.execute(f"DELETE FROM {table} WHERE duration = 0")

    # compute geometry column and convert to EPSG 3005 NAD 83 BC Albers
    cursor.execute(f"""
            UPDATE {table}
            SET geom = ST_Transform(
                          ST_SetSRID(
                            ST_MakeLine(
                              ST_MakePoint(startlon, startlat),
                              ST_MakePoint(endlon, endlat)
                            ),
                            4326
                          ),
                          {crs}
                       )
            WHERE geom IS NULL
        """)

    # calculate segment length
    cursor.execute(f"""UPDATE {table} SET lenm = ST_length(geom)""")

    # calculate SOG (speed over ground)
    cursor.execute(f"""UPDATE {table} SET sogkt = (lenm / duration) * 1.94384""")

    # commit to db after each table
    conn.commit()


def create_spatial_index(table):
    cursor = conn.cursor()
    index_name = f"{table.replace('.', '_')}_geom_idx"
    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} USING GIST (geom)")
    conn.commit()


if __name__ == "__main__":
    main()
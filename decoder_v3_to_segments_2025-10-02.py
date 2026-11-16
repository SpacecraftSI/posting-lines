import os.path

import psycopg2

from decoder_v3_helpers import *

# -------------------------------------------------
# *** update these global variables ***

# database username and password
databaseLogin = ('postgres', 'postgres')
# If the databasePort is None, it will be excluded from the connection statement
databasePort = 5433

# Database names. Add more lines as more years are added.
# The "default" value is used if the year being uploaded isn't otherwise specified.
# Removing the default value will cause it to throw an error if the year isn't specified, which might be desirable.
databaseNames = {
	2025: 'ais_data',
	'default': 'ais_data'
}

# Folder that contains all pickle files. Doesn't matter if other stuff is also in this folder.
# targetFolder = r"D:\workspace\ais processing v3 data folder\cached data"
targetFolder = r"K:\Data\ECCC\decoded ais data from Greg 2025-10-01 FIXED CLASSES\2025_q1"

# Date range to attempt to upload. Range is (inclusive, exclusive). Prints a warning if any days are missing.
dateRange = (datetime.datetime(2025, 1, 1), datetime.datetime(2026, 1, 1))

# An override to skip safety checks before deleting more than the threshold number of segments at once (i.e. per day).
# Leave this as "False" unless you are very intentionally overriding it. Adjusting the threshold is much safer
skipDeleteWarnings = False
abortDeleteThreshold = 5000

# Sets whether to print out the dates for the warnings, or just the presence of them. Mostly just for debugging
verboseErrors = True

# -------------------------------------------------


def main():
	print("Welcome to Greg's ais database to sql table converter.")
	print("This script will print updates as it goes as well as a summary at the end.")
	print("Any warnings that are printed out as the script goes are also noted in the summary at the end, so if the summary doesn't mention any errors, then the script ran successfully!")
	print("Starting the data upload now...\n")
	upload_files()


def upload_files():
	global targetFolder, dateRange, verboseErrors
	# print(datetime.datetime.now(), "Starting data upload. Target date range: {}".format(dateRange[0]))
	dt = dateRange[0]
	summaryStats = [0, 0, 0, 0, 0, []]
	filesNotFound = []
	while dt < dateRange[1]:
		dynamicFilepath = os.path.join(targetFolder, create_filename(dt, "_dynamic_points.txt"))
		staticFilepath = os.path.join(targetFolder, create_filename(dt, "_unique_vessels.txt"))

		if not os.path.exists(dynamicFilepath) or not os.path.exists(staticFilepath):
			print(datetime.datetime.now(), "Warning: file(s) not found for {}. Skipping.".format(dt.strftime("%Y-%m-%d")))
			dt += datetime.timedelta(days=1)
			filesNotFound.append(dt)
			continue

		added, updatedCleared, updatedInPlace, skipped, removed, triggeredAbort = upload_one_file(dynamicFilepath, staticFilepath, dt)
		print(datetime.datetime.now(), "Finished day {}. {} segments added, {} segments updated, {} segments static updated, {} segments already present, {} segments removed".format(dt.strftime("%Y-%m-%d"), added, updatedCleared, updatedInPlace, skipped, removed))
		for i, val in enumerate([added, updatedCleared, updatedInPlace, skipped, removed]):
			summaryStats[i] += val
		if triggeredAbort:
			summaryStats[5].append(dt)

		dt += datetime.timedelta(days=1)

	print("\n-------------- Complete -------------\n")
	print("{} segments added, {} segments updated, {} segments static updated, {} segments already present, {} segments removed.".format(*summaryStats[0:5]))

	if len(summaryStats[5]) > 0:
		print("Warning: abort condition triggered on the following {} days".format(len(summaryStats[5])))
		if verboseErrors:
			for date in summaryStats[5]:
				print(date)
	else:
		print("All segment deletes successful.")

	if len(filesNotFound) > 0:
		print("Warning: {} days not found.".format(len(filesNotFound)))
		if verboseErrors:
			for date in filesNotFound:
				print(date)
	else:
		print("Data found for all days.")


def upload_one_file(tracksFilename, uidFilename, dt):
	global skipDeleteWarnings, abortDeleteThreshold

	tracks = Tracks()
	tracks.load_from_file(tracksFilename)
	uids = UniqueVessels()
	uids.load_from_file(uidFilename)

	tablename = get_table_name(dt)
	schemaName = get_schema_name(dt)
	ensure_table_exists(dt)
	sqlGet = "SELECT * FROM {}.{} WHERE segmentId=%s".format(schemaName, tablename)
	sqlInsert = "INSERT INTO {}.{} VALUES ({})".format(schemaName, tablename, "%s, " * 18 + "%s")
	sqlDelete = "DELETE FROM {}.{} WHERE segmentId=%s".format(schemaName, tablename)
	sqlUpdateStatic = "UPDATE {}.{} SET mmsi=%s, isClassA=%s, classAIS=%s, classGen=%s, name=%s, isUnique=%s, vesDrt=%s, vesLen=%s, vesWidth=%s, vesDestination=%s, lastChange=%s WHERE segmentId=%s".format(schemaName, tablename)
	sqlFindDailySegs = "SELECT segmentId FROM {}.{} WHERE startTime >= %s AND startTime < %s".format(schemaName, tablename)

	segmentIdHash = {}

	segmentsAdded = 0
	segmentsUpdatedCleared = 0
	segmentsUpdatedInPlace = 0
	segmentsSkipped = 0
	segmentsRemoved = 0
	triggeredAbort = False
	with psycopg2.connect(get_conn_string(dt)) as conn:
		with conn.cursor() as cur:
			for vesselId in tracks.tracks.keys():
				if vesselId in uids.vessels:
					segments = convert_track_to_segments(tracks.tracks[vesselId].points, uids.vessels[vesselId])
					for segment in segments:
						segmentIdHash[segment[0]] = True

						cur.execute(sqlGet, (segment[0],))
						res = cur.fetchall()
						# _add_ the segment if not already present
						if len(res) == 0:
							cur.execute(sqlInsert, segment)
							segmentsAdded += 1

						# _replace_ the segment if dynamic needs to be updated
						elif not (len(res) == 1 and res[0][4] == segment[4] and res[0][5] == segment[5] and res[0][6] == segment[6] \
								and res[0][7] == segment[7] and res[0][8] == segment[8]):
							cur.execute(sqlDelete, (segment[0],))
							cur.execute(sqlInsert, segment)
							segmentsUpdatedCleared += 1

						# _update_ the segment if just the static needs to be updated
						elif not (res[0][2] == segment[2] and res[0][9] == segment[9] and res[0][10] == segment[10] and \
								res[0][11] == segment[11] and res[0][12] == segment[12] and res[0][13] == segment[13] and \
								res[0][14] == segment[14] and res[0][15] == segment[15] and res[0][16] == segment[16] and \
								res[0][17] == segment[17]):
							abbreviated = [segment[2]]
							abbreviated.extend(segment[9:19])
							abbreviated.append(segment[0])
							cur.execute(sqlUpdateStatic, abbreviated)
							segmentsUpdatedInPlace += 1

						# no update needed
						else:
							segmentsSkipped += 1

			if len(segmentIdHash.keys()) < abortDeleteThreshold and not skipDeleteWarnings:
				print("Warning: remaining segment count too low, skipping segment delete")
				triggeredAbort = True
			else:
				segsToRemove = []
				cur.execute(sqlFindDailySegs, [dt, dt + datetime.timedelta(days=1)])
				res = cur.fetchall()
				for line in res:
					segId = line[0]
					if segId not in segmentIdHash:
						segsToRemove.append(segId)
				if len(segsToRemove) > abortDeleteThreshold and not skipDeleteWarnings:
					print("Warning: segments to remove count too high ({}), skipping segment delete".format(len(segsToRemove)))
					triggeredAbort = True
				else:
					for segId in segsToRemove:
						cur.execute(sqlDelete, (segId,))
						segmentsRemoved += 1

	return segmentsAdded, segmentsUpdatedCleared, segmentsUpdatedInPlace, segmentsSkipped, segmentsRemoved, triggeredAbort


# TODO: currently, maximum duration, velocity, and distance are set in several places. It should be standardized here instead.
def convert_track_to_segments(track, vesselInfo):
	mmsi = vesselInfo.mmsi
	isA = vesselInfo.isA
	uid = get_vessel_id(mmsi, isA)
	# Catch 10 digit mmsi's before converting to sql
	if len(str(uid)) > 10:
		uid = int(str(uid)[-10:])

	classAIS = vesselInfo.classAIS
	classGen = get_class_gen(classAIS)
	vesselName = vesselInfo.name
	isUnique = vesselInfo.isUnique
	lastChange = datetime.datetime.now()

	length = vesselInfo.length
	width = vesselInfo.width

	segments = []
	for i in range(len(track) - 1):
		startTime = track[i].dt
		segmentId = str(uid) + '.' + str(int((startTime - datetime.datetime(1970, 1, 1)).total_seconds()))
		duration = (track[i+1].dt - track[i].dt).total_seconds()
		startLat, startLon = track[i].lat, track[i].lon
		endLat, endLon = track[i+1].lat, track[i+1].lon

		timeSeconds = startTime.hour * 3600 + startTime.minute * 60 + startTime.second
		draught = vesselInfo.draughts.get_val(timeSeconds)
		destination = vesselInfo.destinations.get_val(timeSeconds)

		# Check for safety, but this theoretically isn't possible.
		if len(destination) > 20:
			destination = destination[20:]

		segments.append((segmentId, uid, mmsi, startTime, duration, startLat, startLon, endLat, endLon, isA, classAIS, classGen, vesselName, isUnique, draught, length, width, destination, lastChange))

	return segments


# -------


def get_table_name(dt):
	return "segments_{}".format(dt.strftime('%Y_%m'))


def get_schema_name(dt):
	return f"year{dt.year}"


def get_conn_string(dt):
	global databasePort
	if databasePort:
		return "dbname={} user={} password={} port={}".format(*get_database_info(dt.year), databasePort)
	return "dbname={} user={} password={}".format(*get_database_info(dt.year))


def ensure_table_exists(dt):
	global databasePort
	schemaName = get_schema_name(dt)
	tableName = get_table_name(dt)

	with psycopg2.connect(get_conn_string(dt)) as conn:
		with conn.cursor() as cur:
			# create the schema if needed
			cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schemaName};")

			# If the table already exists, no work is necessary
			sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{}'".format(tableName)
			cur.execute(sql)
			res = cur.fetchall()
			if len(res) > 0:
				return

			sql = """CREATE TABLE {}.{} (
				segmentId VARCHAR(21) PRIMARY KEY,
				uid BIGINT NOT NULL,
				mmsi INT NOT NULL,
				startTime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
				duration INT NOT NULL,
				startLat FLOAT NOT NULL,
				startLon FLOAT NOT NULL,
				endLat FLOAT NOT NULL,
				endLon FLOAT NOT NULL,
				isClassA BOOL NOT NULL,
				classAIS SMALLINT NOT NULL,
				classGen SMALLINT NOT NULL,
				name VARCHAR(20),
				isUnique BOOL NOT NULL,
				vesDrt FLOAT NOT NULL,
				vesLen FLOAT NOT NULL,
				vesWidth FLOAT NOT NULL,
				vesDestination VARCHAR(20) NOT NULL,
				lastChange TIMESTAMP WITHOUT TIME ZONE NOT NULL,
				geom GEOMETRY (LineString, 3005),
				lenM FLOAT,
				sogKt FLOAT
				);
			""".format(schemaName, tableName)
			cur.execute(sql)
			print(datetime.datetime.now(), "Created table {}.".format(tableName))


def get_database_info(year):
	global databaseLogin, databaseNames
	if year in databaseNames:
		return databaseNames[year], *databaseLogin
	else:
		return databaseNames['default'], *databaseLogin


# Groups AIS vessel classes into the classes used for the current study.
def get_class_gen(classAIS):
	# Null case, to differentiate from other "misc"
	# This is because "null" needs to be freely overwritten, while "misc" needs to not be overwritten
	if classAIS == -1:
		return -1

	if 70 <= classAIS < 80:
		return 1
	if 80 <= classAIS < 90:
		return 2
	if classAIS == 30:
		return 3
	if classAIS in (31, 32, 52):
		return 4
	if classAIS in (35, 50, 51, 55, 58):
		return 5
	if classAIS == 36:
		return 6
	if classAIS == 37:
		return 7
	if 60 <= classAIS < 70:
		return 8
	return 0


if __name__ == '__main__':
	main()

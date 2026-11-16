import datetime

from math import sin, cos, sqrt, atan2, radians
import ast


# --------------------------

# One parsed static message
class StaticDetection:
	def __init__(self, message, dt):
		# Lots of these allow 'None' columns because of class B messages not always containing them
		# AIS reference https://www.navcen.uscg.gov/ais-class-a-static-voyage-message-5
		self.mmsi = int(message['mmsi'])
		self.imo = message['imo'] if 'imo' in message else -1
		self.callsign = message['callsign'] if 'callsign' in message else ''
		self.name = message['shipname'] if 'shipname' in message else ''
		self.classAIS = int(message['ship_type']) if 'ship_type' in message else -1
		self.messageType = message['msg_type']

		self.dt = dt
		self.isA = self.messageType == 5

		self.toBow = message['to_bow'] if 'to_bow' in message else 0
		self.toStern = message['to_stern'] if 'to_stern' in message else 0
		self.toPort = message['to_port'] if 'to_port' in message else 0
		self.toStarboard = message['to_starboard'] if 'to_starboard' in message else 0
		self.draught = message['draught'] if 'draught' in message else -1

		self.positionFixingDevice = message['epfd'] if 'epfd' in message else -1
		self.destination = message['destination'] if 'destination' in message else ""
		self.eta = (None, None, None, None)
		if 'month' in message and 'day' in message and 'hour' in message and 'minute' in message:
			self.eta = (message['month'], message['day'], message['hour'], message['minute'])

	@classmethod
	def from_str(cls, s):
		li = ast.literal_eval(s)
		if len(li) != 15:
			raise ValueError("Wrong number of list items")

		data = {
			'mmsi': li[0],
			'imo': li[1],
			'callsign': li[2],
			'shipname': li[3],
			'shiptype': li[4],
			'msg_type': li[5],
			'to_bow': li[7],
			'to_stern': li[8],
			'to_port': li[9],
			'to_starboard': li[10],
			'draught': li[11],
			'epfd': li[12],
			'destination': li[13],
			'month': li[14][0],
			'day': li[14][1],
			'hour': li[14][2],
			'minute': li[14][3],
		}
		dt = datetime.datetime.strptime(li[6], '%Y%m%d%H%M%S')
		return cls(data, dt)

	def __str__(self):
		dtStr = self.dt.strftime('%Y%m%d%H%M%S')
		return str((self.mmsi, self.imo, self.callsign, self.name, self.classAIS, self.messageType,
				dtStr, self.toBow, self.toStern, self.toPort, self.toStarboard,
				self.draught, self.positionFixingDevice, self.destination, self.eta))


# One parsed dynamic message
# Messages "<" compare mmsi, isA, and datetime
class DynamicDetection:
	def __init__(self, message, dt):
		self.mmsi = int(message['mmsi'])
		self.lat = message['lat']
		self.lon = message['lon']

		self.dt = dt

		self.navStat = int(message['status']) if 'status' in message else -1
		self.sog = message['speed']
		self.cog = message['course']
		self.reportedTimestamp = message['second'] if 'second' in message else -1
		self.reportedPositionalAccuracy = True if message['accuracy'] else False
		self.messageType = message['msg_type']
		# TODO: message type 27 can be a or b
		self.isA = self.messageType in (1, 2, 3)

		self.turn = int(message['turn']) if 'turn' in message else -1
		self.heading = message['heading']
		self.maneuver = int(message['maneuver']) if 'maneuver' in message else -1

	@classmethod
	def from_str(cls, s):
		li = ast.literal_eval(s)
		if len(li) != 13:
			raise ValueError("Wrong number of list items")

		message = {
			'mmsi': li[0],
			'lat': li[1],
			'lon': li[2],
			'status': li[4],
			'speed': li[5],
			'course': li[6],
			'second': li[7],
			'accuracy': li[8],
			'msg_type': li[9],
			'turn': li[10],
			'heading': li[11],
			'maneuver': li[12],
		}
		dt = datetime.datetime.strptime(li[3], '%Y%m%d%H%M%S')
		return cls(message, dt)

	def __lt__(self, other):
		if not isinstance(other, DynamicDetection):
			raise TypeError()

		if self.mmsi < other.mmsi:
			return True
		elif self.mmsi == other.mmsi:
			if self.isA < other.isA:
				return True
			elif self.isA == other.isA:
				return self.dt < other.dt

		return False

	def __str__(self):
		dtStr = self.dt.strftime('%Y%m%d%H%M%S')
		return str((self.mmsi, self.lat, self.lon, dtStr, self.navStat, self.sog, self.cog, self.reportedTimestamp,
				self.reportedPositionalAccuracy, self.messageType, self.turn, self.heading, self.maneuver))


class Detections:
	def __init__(self):
		self.filenamesDecoded = {}
		self.dynamicPoints = {}
		self.staticPoints = {}
		self.newDynamicPoints = []
		self.newStaticPoints = []

	def add_filename(self, filename):
		self.filenamesDecoded[filename] = True

	def is_filename_included(self, filename):
		return filename in self.filenamesDecoded

	# returns True if the point is new, or False otherwise
	def add_dynamic(self, detection, replace=False):
		dId = self.build_dynamic_id(detection.mmsi, detection.dt, detection.lat)
		if dId in self.dynamicPoints:
			if replace:
				self.dynamicPoints[dId] = detection
				self.newDynamicPoints.append(detection)
			return False

		self.dynamicPoints[dId] = detection
		self.newDynamicPoints.append(detection)
		return True

	def add_static(self, detection, replace=False):
		sId = self.build_static_id(detection.mmsi, detection.dt, detection.classAIS, detection.name, detection.callsign)
		if sId in self.staticPoints:
			if replace:
				self.staticPoints[sId] = detection
				self.newStaticPoints.append(detection)
			return False

		self.staticPoints[sId] = detection
		self.newStaticPoints.append(detection)
		return True

	def has_new(self):
		return len(self.newDynamicPoints) > 0 or len(self.newStaticPoints) > 0

	# gets the new dynamic messages, in mmsi, then time-sorted order
	def get_sorted_new_dynamic(self):
		self.newDynamicPoints.sort()
		return self.newDynamicPoints

	# gets the new static messages
	def get_new_static(self):
		return self.newStaticPoints

	# clears the new points arrays.
	def clear_new(self):
		self.newDynamicPoints = []
		self.newStaticPoints = []

	def save(self, path, dt):
		pass

	def load(self, path, dt):
		pass

	# statistics/testing method for comparing Detections objects
	def generate_diff_dynamic(self, other):
		uniqueSelf = []
		uniqueOther = []
		both = []
		for pId in self.dynamicPoints.keys():
			if pId in other.dynamicPoints:
				both.append(self.dynamicPoints[pId])
			else:
				uniqueSelf.append(self.dynamicPoints[pId])

		for pId in other.dynamicPoints.keys():
			if pId not in self.dynamicPoints:
				uniqueOther.append(other.dynamicPoints[pId])

		return uniqueSelf, uniqueOther, both

	# statistics/testing method
	def generate_diff_dynamic_with_margin(self, other, margin=5):
		uniqueSelf = []
		uniqueOther = []
		both = []
		for pId in self.dynamicPoints.keys():
			if other.does_dynamic_point_exists_with_margin(self.dynamicPoints[pId][1], self.dynamicPoints[pId][2], self.dynamicPoints[pId][4], margin):
				both.append(self.dynamicPoints[pId])
			else:
				uniqueSelf.append(self.dynamicPoints[pId])

		for pId in other.dynamicPoints.keys():
			if not self.does_dynamic_point_exists_with_margin(other.dynamicPoints[pId][1], other.dynamicPoints[pId][2], other.dynamicPoints[pId][4], margin):
				uniqueOther.append(other.dynamicPoints[pId])

		return uniqueSelf, uniqueOther, both

	# statistics/testing method
	def does_dynamic_point_exists_with_margin(self, mmsi, dt, lat, margin_secs):
		for i in range(0 - margin_secs, margin_secs + 1):
			if self.build_dynamic_id(mmsi, dt + datetime.timedelta(seconds=i), lat) in self.dynamicPoints:
				return True
		return False

	# Get a dictionary of filenames processed to create this object
	def get_files_dict(self):
		return self.filenamesDecoded.copy()

	@staticmethod
	def build_dynamic_id(mmsi, dt, lat):
		return guarantee_mmsi_length(mmsi) * pow(10, 16) + \
			int((dt - datetime.datetime(1970, 1, 1)).total_seconds()) * pow(10, 6) + \
			abs(int(lat * pow(10, 4)))

	# Note: currently ignores name and callsign uniqueness
	@staticmethod
	def build_static_id(mmsi, dt, classAIS, name, callsign):
		return guarantee_mmsi_length(mmsi) * pow(10, 16) \
			   + int((dt - datetime.datetime(1970, 1, 1)).total_seconds()) * pow(10, 6) \
			   + (classAIS * pow(10, 3) if classAIS else 0)


# --------------------------


class VesselInfo:
	# Data accepts both dictionaries and static messages
	def __init__(self, data):
		# These two values make up the id, and such can never change
		self.mmsi = data['mmsi']
		self.isA = data['isA']

		# The rest of the info can change.

		# Base case is true because it's being generated from one static message
		self.isUnique = data['isUnique'] if 'isUnique' in data else True

		self.imo = data['imo']
		self.callsign = data['callsign']
		self.name = data['name']
		self.classAIS = data['classAIS']

		self.length = data['length']
		self.width = data['width']

		self.draughts = MyHashTable(-1)
		self.destinations = MyHashTable("")
		self.etas = MyHashTable([None, None, None, None])

	@classmethod
	def from_static(cls, static, dt):
		data = {
			'mmsi': static.mmsi,
			'isA': static.isA,
			'imo': static.imo,
			'callsign': static.callsign,
			'name': static.name,
			'classAIS': static.classAIS,
			'length': static.toBow + static.toStern,
			'width': static.toPort + static.toStarboard,
		}
		obj = cls(data)

		timestamp = dt_to_seconds_of_day(dt)
		if static.draught and static.draught > 0:
			obj.draughts.add_value(timestamp, static.draught)
		if static.destination and len(static.destination) > 0:
			obj.destinations.add_value(timestamp, static.destination)
		if static.eta[0] is not None:
			obj.etas.add_value(timestamp, static.eta)

		return obj


	@classmethod
	def from_string(cls, text):
		li = ast.literal_eval(text)
		if len(li) != 12:
			raise ValueError("Wrong number of list items")
		data = {
			'mmsi': li[0],
			'isA': li[1],
			'isUnique': li[2],
			'imo': li[3],
			'callsign': li[4],
			'name': li[5],
			'classAIS': li[6],
			'length': li[7],
			'width': li[8]
		}
		obj = cls(data)

		obj.draughts = MyHashTable.from_str(li[9])
		obj.destinations = MyHashTable.from_str(li[10])
		obj.etas = MyHashTable.from_str(li[11])

		return obj

	@classmethod
	def from_static_mocked(cls, static, dt):
		obj = cls(static)

		timestamp = dt_to_seconds_of_day(dt)
		obj.draughts.add_value(timestamp, static['draught'])
		obj.destinations.add_value(timestamp, static['destination'])
		obj.etas.add_value(timestamp, static['eta'])

		return obj

	def add_detection(self, static, dt):
		length = static.toBow + static.toStern
		width = static.toPort + static.toStarboard

		if static.imo != self.imo:
			self.imo = -1
			self.isUnique = False
		if self.callsign and static.callsign and static.callsign != self.callsign:
			self.callsign = "ambiguous"
			self.isUnique = False
		if self.name and static.name and static.name != self.name:
			self.name = "ambiguous"
			self.isUnique = False
		if static.classAIS > 0 and static.classAIS != self.classAIS:
			self.classAIS = -1
			self.isUnique = False

		if length > 0 and self.length > 0 and self.length != length:
			self.length = -1
		elif self.length == 0 and length:
			self.length = length

		if width > 0 and self.width > 0 and self.width != width:
			self.width = -1
		elif self.width == 0 and width:
			self.width = width

		timestamp = dt_to_seconds_of_day(dt)
		if static.draught and static.draught > 0:
			self.draughts.add_value(timestamp, static.draught)
		if static.destination and len(static.destination) > 0:
			self.destinations.add_value(timestamp, static.destination)
		if static.eta[0] is not None:
			self.etas.add_value(timestamp, static.eta)

	def __str__(self):
		return str([self.mmsi, self.isA, self.isUnique, self.imo, self.callsign, self.name, self.classAIS, self.length,
					self.width, str(self.draughts), str(self.destinations), str(self.etas)])


class UniqueVessels:
	def __init__(self):
		self.vessels = {}

	def add_static_message(self, static, dt=None, usingMocked=False):
		if not usingMocked:
			vesselId = get_vessel_id(static.mmsi, static.isA)
			dt = static.dt
			if vesselId in self.vessels:
				self.vessels[vesselId].add_detection(static, dt)
			else:
				self.vessels[vesselId] = VesselInfo.from_static(static, dt)
		else:
			vesselId = get_vessel_id(static['mmsi'], static['isA'])
			if vesselId in self.vessels:
				self.vessels[vesselId].add_detection(static, dt)
			else:
				self.vessels[vesselId] = VesselInfo.from_static_mocked(static, dt)


	def is_mmsi_a(self, mmsi):
		id = get_vessel_id(mmsi, True)
		return id in self.vessels

	def is_mmsi_b(self, mmsi):
		id = get_vessel_id(mmsi, False)
		return id in self.vessels

	def write_to_file(self, filepath):
		with open(filepath, 'w+') as outfile:
			for vesselId in self.vessels.keys():
				# safeguard against newlines in the string.
				# Since this object is always saved and reloaded before further processing, this is ok.
				text = str(self.vessels[vesselId])
				text.replace("\n", "")
				outfile.write(text + "\n")

	def load_from_file(self, filepath):
		with open(filepath, 'r') as infile:
			for line in infile:
				obj = VesselInfo.from_string(line)
				vesselId = get_vessel_id(obj.mmsi, obj.isA)
				self.vessels[vesselId] = obj

	def __str__(self):
		return "\n".join([str(self.vessels[x]) for x in self.vessels.keys()])


# --------------------------

class Track:
	def __init__(self, vId):
		self.points = []
		self.id = vId

	@classmethod
	def from_str(cls, s):
		li = ast.literal_eval(s)
		res = cls(li[0])
		for val in li[1:]:
			res.points.append(DynamicDetection.from_str(val))

		return res

	def add_points(self, newPoints):
		# determine which new points are both valid and add meaningful information
		i = 0
		for newPoint in newPoints:
			# Out of possible bounds check:
			lat, lon = newPoint.lat, newPoint.lon
			if not (-90 < lat < 90 and -180 < lon < 180):
				continue

			# Always keep the first point for a given vessel
			if len(self.points) == 0:
				self.points.insert(i, newPoint)
				i += 1
				continue

			# Find the correct potential position for the new point
			while i < len(self.points) and self.points[i].dt < newPoint.dt:
				i += 1

			# Determine whether to keep the new point
			if self.keep_or_drop_point(None if i == 0 else self.points[i - 1], newPoint):
				if self.keep_or_drop_point(None if i == len(self.points) else self.points[i], newPoint):
					self.points.insert(i, newPoint)
					i += 1

	# returns the interpolated position at the requested time, or None if none can be generated
	def get_interpolated_position(self, dt):
		if len(self.points) < 2 or dt < self.points[0][2] or dt > self.points[-1][2]:
			return None

		prevPoint = self.points[0]
		for point in self.points[1:]:
			if point[2] == dt:
				return point[4:6]
			if point[2] > dt:
				totalDelta = point[2] - prevPoint[2]
				# Max segment duration check
				if totalDelta.total_seconds() > 6 * 3600:
					return None

				# interpolate the current position
				interpolationFactor = (dt - prevPoint[2]) / totalDelta
				lat = prevPoint[4] + (point[4] - prevPoint[4]) * interpolationFactor
				lon = prevPoint[5] + (point[5] - prevPoint[5]) * interpolationFactor
				return lat, lon

			# increment points counter and check next set again
			prevPoint = point

		raise Exception("unexpected state reached - interpolation math should have been exhaustive")

	# "Fixer" method: not generally used unless the data gets messed up somehow
	# returns:
	# 1. A boolean, whether the track needed fixing
	# 2. the number of points removed during the fix (if done)
	# 3. a list of points that have been mis-attributed to this track
	def check_and_fix_point_order(self):
		if self.is_points_ordered():
			return False, 0

		points = self.points
		self.points = []
		points.sort()
		self.add_points(points)
		return True, len(points) - len(self.points)

	def is_points_ordered(self):
		if len(self.points) < 2:
			return True
		# if self.id == 3160195651:
		# 	print(self.points[12])
		# 	print(self.points[13])
		# 	print(self.points[12] < self.points[13])
		# 	print(self.points[12].mmsi == self.points[13].mmsi)
		# 	print(self.points[12].isA == self.points[13].isA)
		for i in range(len(self.points) - 1):
			if self.points[i] > self.points[i+1]:
				return False
		return True

	# "Fixer" method: not generally used unless the data gets messed up somehow
	# Removes and returns a list of all dynamic points mistakenly attributed to this track
	def get_misattributed_points(self):
		isA = self.id % 2 == 0
		i = 0
		res = []
		while i < len(self.points):
			if self.points[i].isA != isA:
				res.append(self.points.pop(i))
			else:
				i += 1

		return res


	def __str__(self):
		res = [self.id]
		res.extend([str(x) for x in self.points])
		return str(res)

	# Returns True if both should be kept, or False if one should be dropped
	@staticmethod
	def keep_or_drop_point(p1, p2):
		if p1 is None or p2 is None:
			return True

		lat1, lon1 = p1.lat, p1.lon
		lat2, lon2 = p2.lat, p2.lon
		dist = get_geo_dist((lat1, lon1), (lat2, lon2))

		# small initial distance. This helps eliminate vessels at anchor
		if dist < 30:
			return False

		dt1 = p1.dt
		dt2 = p2.dt
		time = abs((dt2 - dt1).total_seconds())

		# Standard filter
		if dist < 500 and time < 60:
			return False

		# prevents two dynamic points with the same timestamp from being added multiple times across multiple runs
		if time == 0:
			return False

		return True


class Tracks:
	def __init__(self):
		self.tracks = {}

	# Splits a big sorted (mmsi then datetime) list of dynamic points into their appropriate tracks
	def addPoints(self, pointsToAdd):
		while len(pointsToAdd) > 0:
			oneSet, pointsToAdd = self._pop_one_point_set(pointsToAdd)
			mmsi = oneSet[0].mmsi
			isA = oneSet[0].isA
			id = get_vessel_id(mmsi, isA)
			if id not in self.tracks:
				self.tracks[id] = Track(id)
			self.tracks[id].add_points(oneSet)

	# Splits a sorted list of dynamic points into two
	# The first has all points for one mmsi, and the latter is everything else
	@staticmethod
	def _pop_one_point_set(li):
		mmsi = li[0].mmsi
		isA = li[0].isA
		# Base case: only one mmsi
		if li[-1].mmsi == mmsi and li[-1].isA == isA:
			return li, []

		# Otherwise, find a break. No end-of-list condition is needed in this loop due to base case above
		i = 1
		while li[i].mmsi == mmsi and li[i].isA == isA:
			i += 1

		return li[:i], li[i:]

	def __str__(self):
		return "\n".join([str(self.tracks[x]) for x in self.tracks.keys()])

	def write_to_file(self, filepath):
		with open(filepath, 'w+') as outfile:
			for trackId in self.tracks.keys():
				# safeguard against newlines in the string.
				# Since this object is always saved and reloaded before further processing, this is ok.
				text = str(self.tracks[trackId])
				text.replace("\n", "")
				outfile.write(text + "\n")

	def load_from_file(self, filepath):
		with open(filepath, 'r') as infile:
			for line in infile:
				obj = Track.from_str(line)
				self.tracks[obj.id] = obj



# --------------------------


# An object optimized for looking up the closest past value for a given key
# linear memory performance, and if the lookups tend to cluster, fixed lookup performance too
# Worst-case lookup performance is linear to the number of keys
class MyHashTable:
	def __init__(self, defaultVal=None):
		self.values = {}
		self.keys = []
		self.index = 0

		if defaultVal is not None:
			self.add_value(-1, defaultVal)

	@classmethod
	def from_str(cls, s):
		res = cls()
		res.values = ast.literal_eval(s)
		return res

	# 'key' must be an integer for the json to work properly
	def add_value(self, key, val):
		self.values[key] = val

	def get_val(self, key):
		# Ensure the key table is up to date.
		if len(self.keys) != len(self.values.keys()):
			self._optimize()

		if len(self.keys) == 0:
			return 0

		# see if we need to move forward first
		while self.index < len(self.keys) - 1 and self.keys[self.index + 1] <= key:
			self.index += 1

		# see if we need to move backward next
		while self.index > 0 and self.keys[self.index - 1] >= key:
			self.index -= 1

		return self.values[self.keys[self.index]]

	def _optimize(self):
		self.keys = list(self.values.keys())
		self.keys.sort()

	def __str__(self):
		return str(self.values)

# --------------------------

def get_vessel_id(mmsi, isA):
	return mmsi * 10 + (0 if isA else 1)


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


# Calculates the distance between two lat/lon points, using earth-as-a-sphere model
# Modified from code originally written by Michael0x2a, which can be found at:
# https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
def get_geo_dist(p1, p2):
	R = 6373000

	lat1 = radians(p1[0])
	lon1 = radians(p1[1])
	lat2 = radians(p2[0])
	lon2 = radians(p2[1])

	dlon = lon2 - lon1
	dlat = lat2 - lat1

	a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
	c = 2 * atan2(sqrt(a), sqrt(1 - a))

	return R * c


def create_filename(datetime, suffix):
	return datetime.strftime("%Y%m%dT%H%M%S") + suffix


# Gets a datetime from a string. Mostly here for future-proofing.
# If other time formats are used by other files, they only need to be added here
def dt_from_raw(s):
	return datetime.datetime.strptime(s.split('.')[0], "%Y%m%dT%H%M%S")


def dt_to_seconds_of_day(dt):
	return dt.hour * 3600 + dt.minute * 60 + dt.second


# Input: an integer (typically an mmsi)
# Output: that integer having no more than 9 digits. Useful for creating id's that have length restrictions
def guarantee_mmsi_length(mmsi):
	return mmsi % 1000000000
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import col

import numpy as np
from sklearn.neighbors import DistanceMetric
from datetime import timedelta

EARTH_RADIUS = 6372.795 * 1000


def sklearn_haversine(latlon, latlon2=None):
	haversine = DistanceMetric.get_metric('haversine')
	dists = haversine.pairwise(latlon, latlon2)
	return EARTH_RADIUS * dists


def diameter(dataframe, i, j_star, cached_matrix=None):
	"""
	Return max distance between two points in dataframe.
	:param dataframe: [[latitude, longitude]] matrix
	:param i: row index start
	:param j_star: row index end
	:return: (float) maximum distance between two points.
	"""
	if cached_matrix is not None:
		point_to_add = dataframe[j_star].reshape(1, 2)
		other_points = dataframe[i:j_star + 1]

		distance_to_add = sklearn_haversine(point_to_add, other_points)

		dist_mat = np.pad(cached_matrix, [(0, 1), (0, 1)], mode='constant')
		dist_mat[-1, :] = distance_to_add
		dist_mat[:, -1] = distance_to_add
	else:
		points = dataframe[i:j_star + 1]
		'''
		if points.shape[0] > 20:
			# Convex Hull
			# http://people.scs.carleton.ca/~michiel/lecturenotes/ALGGEOM/diameter.pdf
			cartesian_points = spherical_to_cartesian(points[:, [1, 0]])
			hull = ConvexHull(cartesian_points)
	
			points = points[hull.vertices]
		'''
		# Calculate distance matrix
		dist_mat = sklearn_haversine(points)
	return np.max(dist_mat), dist_mat


def get_medoid_index(dist_mat):
	"""
	Given a vector of distances it returns the index of the medoid of those points.
	The medoid is the point closest to all others.
	:param dist_mat: vector of distances
	:return: index of the medoid.
	"""
	return np.argmin(np.sum(dist_mat, axis=0))


def calculate_centroid(X):
	"""
	Calculates the centroid plain and simple.
	:param X:
	:return:
	"""
	return np.mean(X, axis=0)


def get_stop_location(df, min_stay_duration, roaming_distance):
	"""
	Given a numpy array of time-ordered gps locations detect the stop locations
	and return them in a pandas dataframe.
	Hariharan, Toyama. 2004. Project Lachesis: Parsing and Modeling Location
	Histories

	[[timestamp, latitude, longitude, t_start, t_end]]
	"""

	# Inizialise variables
	i = 0
	medoids_set = []

	df = np.array(df)
	time_array = df[:, 0]
	df_xyz = df[:, [1, 2]].astype(np.float32)

	while i < df.shape[0]:
		# Get the first item that is at least min_stay_duration away from
		# point i
		time_cutoff = time_array[i] + timedelta(minutes=min_stay_duration)
		idxs_after_time_cutoff = np.where(time_array >= time_cutoff)
		# Break out of while loop if there are no more items that satisfy the
		# time_cutoff criterium
		if len(idxs_after_time_cutoff[0]) == 0:
			break

		# This is the first item after that satisfies time_cutoff
		j_star = idxs_after_time_cutoff[0][0]

		# Check whether roaming_distance criterium is satisfied.
		dist, cached_matrix = diameter(df_xyz, i, j_star)
		if dist > roaming_distance:
			i += 1
		else:
			for y in range(j_star + 1, df.shape[0]):
				dist, temp_cached_matrix = diameter(df_xyz, i, y, cached_matrix)
				if dist > roaming_distance:
					break
				cached_matrix = temp_cached_matrix
				j_star = y

			# Get medoid, if there are only 2 points just take the first one.
			if (j_star - i) == 1:
				medoid_idx = 0
			else:
				medoid_idx = get_medoid_index(cached_matrix)

			# Add medoid to list and increment i-index
			m = df_xyz[i + medoid_idx]
			medoids_set.append([float(m[0]), float(m[1]), time_array[i], time_array[j_star]])
			i = j_star + 1

	# Convert to dataframe and return as result columns=["timestamp", "latitude", "longitude", "t_start", "t_end"]
	return medoids_set


def main():
	# Parameters for the algorithm
	roam_dist = 100  # meters
	min_stay = 10  # minutes
	# Parameters for the paths
	input_path = '/path_to_parquet'  # parquet file
	output_path = '/path_to_parquet_out'  # parquet file

	spark = SparkSession \
		.builder \
		.appName("Stop locations") \
		.getOrCreate()

	# Read data
	source_df = spark.read.parquet(input_path)
	source_df = source_df.select('user_id', 'timestamp', F.radians('latitude').alias('lat'),
								 F.radians('longitude').alias("lon")).orderBy('timestamp')
	source_df.cache()

	# Filter out all the data that is not necessary (e.g. positions equals to others in a time-distance less than min_stay
	w = Window.partitionBy(['user_id']).orderBy('timestamp')

	source_df = source_df.select("user_id", "timestamp", "lat", "lon",
								 F.lead("lat", 1).over(w).alias("next_lat"), F.lead("lon", 1).over(w).alias("next_lon"))

	dist_df = source_df.withColumn("distance_next", EARTH_RADIUS * 2 * F.asin(F.sqrt(
		F.pow(F.sin((col("next_lat") - col("lat")) / 2.0), 2) + F.cos("lat") * F.cos("next_lat") * F.pow(
			F.sin((col("next_lon") - col("lon")) / 2.0), 2))))

	dist_df = dist_df.withColumn("distance_prev", F.lag("distance_next").over(w))
	exclude_df = dist_df.where(((col("distance_next") < 5) & (col("distance_prev") < 5)) | (
			(col("distance_next") > roam_dist) & (col("distance_prev") > roam_dist)))

	df = source_df.join(exclude_df, ['user_id', 'timestamp'], "left_anti").select("user_id", "timestamp", "lat", "lon")

	# Transform to RDD, in order to apply the function get_stop_location
	# RDD that contains: (user_id, [timestamp, lat, lon, lat_degrees, lon_degrees])
	df_rdd = df.orderBy(['user_id', 'timestamp']).rdd.map(tuple)
	df_rdd = df_rdd.map(lambda x: (x[0], [[x[1], x[2], x[3]]]))
	# RDD that contains: (user_id, [[timestamp, lat, lon], ..., [timestamp, lat, lon]]), sorted by timestamp
	grouped_rdd = df_rdd.reduceByKey(lambda x, y: x + y)

	stop_locations_rdd = grouped_rdd.map(
		lambda x: (x[0], get_stop_location(x[1], min_stay_duration=min_stay, roaming_distance=roam_dist)))

	stop_locations_rdd = stop_locations_rdd.flatMapValues(lambda x: x).map(
		lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3]))

	# Output schema
	schema = StructType([
		StructField('user_id', StringType(), False),
		StructField('lat', DoubleType(), False),
		StructField('lon', DoubleType(), False),
		StructField('from', TimestampType(), False),
		StructField('to', TimestampType(), False)
	])
	result_df = spark.createDataFrame(stop_locations_rdd, schema)

	result_df = result_df.withColumn('lat', F.degrees('lat'))
	result_df = result_df.withColumn('lon', F.degrees('lon'))

	result_df.write.save(output_path)


if __name__ == '__main__':
	main()


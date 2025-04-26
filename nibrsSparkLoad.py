from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PostgresToMongo") \
    .config("spark.mongodb.output.uri", "mongodb://10.166.128.67:27017/nibrs.incident") \
    .getOrCreate()

# PostgreSQL connection parameters
pg_url = "jdbc:postgresql://10.12.0.3/nibrs"
pg_properties = {
    "user": "completely-epic-seahorse",
    "password": "O!6LHcsgjVq63Zdl",
    "driver": "org.postgresql.Driver"
}

# Query to fetch data
pg_query = """
SELECT
    nibrs_incident.incident_id,
    nibrs_incident.incident_date,
    json_agg(
        json_build_object(
            'offender_id', nibrs_offender.offender_id,
            'age_num', nibrs_offender.age_num
        )
    ) AS offenders,
    json_agg(
        json_build_object(
            'offense', nibrs_offense_type.offense_name,
            'weapon', json_build_object(
                'weapon', nibrs_weapon_type.weapon_name
            )
        )
    ) AS offenses
FROM 
    nibrs_incident
JOIN 
    nibrs_offender ON nibrs_incident.incident_id = nibrs_offender.incident_id
JOIN 
    nibrs_offense ON nibrs_incident.incident_id = nibrs_offense.incident_id
JOIN
    nibrs_offense_type ON nibrs_offense.offense_code = nibrs_offense_type.offense_code
JOIN 
    nibrs_weapon ON nibrs_offense.offense_id = nibrs_weapon.offense_id
JOIN
    nibrs_weapon_type ON nibrs_weapon.weapon_id = nibrs_weapon_type.weapon_id
WHERE 
    nibrs_incident.incident_date BETWEEN '2023-01-01' AND '2023-01-31'
GROUP BY 
    nibrs_incident.incident_id, nibrs_incident.incident_date
"""
    
# Load data from PostgreSQL into a Spark DataFrame
postgres_df = spark.read \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("query", pg_query) \
    .option("user", pg_properties["user"]) \
    .option("password", pg_properties["password"]) \
    .option("driver", pg_properties["driver"]) \
    .load()

# Convert columns to JSON format for MongoDB
mongo_ready_df = postgres_df.withColumn("incident_json", to_json(struct([col(c) for c in postgres_df.columns])))

# Write data to MongoDB
mongo_ready_df.write \
    .format("mongo") \
    .mode("append") \
    .save()

# Stop Spark session
spark.stop()

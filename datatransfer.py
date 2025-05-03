import json
import psycopg2
from pymongo import MongoClient

# Connect to PostgreSQL
conn = psycopg2.connect("dbname=<DBNAME> user=<USERNAME> password=<PASSWORD> host=<HOST>")
cur = conn.cursor()

# Query to gather incident summary
query = """
SELECT row_to_json(incident_summary) as incident_json
FROM (
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
) incident_summary;
"""
# Execute the query
cur.execute(query)

# MongoDB connection details
mongo_uri = "<HOST>:<PORT>"
mongo_database = "<DBNAME>"
mongo_collection = "<COLLECTION>"

client = MongoClient(mongo_uri)
db = client[mongo_database]
collection = db[mongo_collection]

# Loop through query results
results = cur.fetchall()
for incident in results:
    document = json.dumps(incident[0])
    collection.insert_one(json.loads(document))

# Close connection
conn.close()
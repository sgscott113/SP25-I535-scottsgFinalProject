import psycopg2
import subprocess
import pandas as pd
import csv

# Download file from GCS
state = "NJ"
files_to_load = [
    {"file_path": f"gs://fbi_nibrs/{state}-2023/agencies.csv", "local_path": "./agencies.csv", "table_name": "agencies"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ACTIVITY_TYPE.csv", "local_path": "./NIBRS_ACTIVITY_TYPE.csv", "table_name": "nibrs_activity_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_AGE.csv", "local_path": "./NIBRS_AGE.csv", "table_name": "nibrs_age"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ARRESTEE.csv", "local_path": "./NIBRS_ARRESTEE.csv", "table_name": "nibrs_arrestee"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ARRESTEE_GROUPB.csv", "local_path": "./NIBRS_ARRESTEE_GROUPB.csv", "table_name": "nibrs_arrestee_groupb"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ARRESTEE_GROUPB_WEAPON.csv", "local_path": "./NIBRS_ARRESTEE_GROUPB_WEAPON.csv", "table_name": "nibrs_arrestee_groupb_weapon"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ARRESTEE_WEAPON.csv", "local_path": "./NIBRS_ARRESTEE_WEAPON.csv", "table_name": "nibrs_arrestee_weapon"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ARREST_TYPE.csv", "local_path": "./NIBRS_ARREST_TYPE.csv", "table_name": "nibrs_arrest_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ASSIGNMENT_TYPE.csv", "local_path": "./NIBRS_ASSIGNMENT_TYPE.csv", "table_name": "nibrs_assignment_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_BIAS_LIST.csv", "local_path": "./NIBRS_BIAS_LIST.csv", "table_name": "nibrs_bias_list"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_BIAS_MOTIVATION.csv", "local_path": "./NIBRS_BIAS_MOTIVATION.csv", "table_name": "nibrs_bias_motivation"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_CIRCUMSTANCES.csv", "local_path": "./NIBRS_CIRCUMSTANCES.csv", "table_name": "nibrs_circumstances"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_CLEARED_EXCEPT.csv", "local_path": "./NIBRS_CLEARED_EXCEPT.csv", "table_name": "nibrs_cleared_except"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_CRIMINAL_ACT.csv", "local_path": "./NIBRS_CRIMINAL_ACT.csv", "table_name": "nibrs_criminal_act"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_CRIMINAL_ACT_TYPE.csv", "local_path": "./NIBRS_CRIMINAL_ACT_TYPE.csv", "table_name": "nibrs_criminal_act_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_DRUG_MEASURE_TYPE.csv", "local_path": "./NIBRS_DRUG_MEASURE_TYPE.csv", "table_name": "nibrs_drug_measure_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_ETHNICITY.csv", "local_path": "./NIBRS_ETHNICITY.csv", "table_name": "nibrs_ethnicity"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_INJURY.csv", "local_path": "./NIBRS_INJURY.csv", "table_name": "nibrs_injury"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_JUSTIFIABLE_FORCE.csv", "local_path": "./NIBRS_JUSTIFIABLE_FORCE.csv", "table_name": "nibrs_justifiable_force"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_LOCATION_TYPE.csv", "local_path": "./NIBRS_LOCATION_TYPE.csv", "table_name": "nibrs_location_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_OFFENDER.csv", "local_path": "./NIBRS_OFFENDER.csv", "table_name": "nibrs_offender"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_OFFENSE.csv", "local_path": "./NIBRS_OFFENSE.csv", "table_name": "nibrs_offense"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_OFFENSE_TYPE.csv", "local_path": "./NIBRS_OFFENSE_TYPE.csv", "table_name": "nibrs_offense_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_PROPERTY.csv", "local_path": "./NIBRS_PROPERTY.csv", "table_name": "nibrs_property"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_PROPERTY_DESC.csv", "local_path": "./NIBRS_PROPERTY_DESC.csv", "table_name": "nibrs_property_desc"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_PROP_DESC_TYPE.csv", "local_path": "./NIBRS_PROP_DESC_TYPE.csv", "table_name": "nibrs_prop_desc_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_PROP_LOSS_TYPE.csv", "local_path": "./NIBRS_PROP_LOSS_TYPE.csv", "table_name": "nibrs_prop_loss_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_RELATIONSHIP.csv", "local_path": "./NIBRS_RELATIONSHIP.csv", "table_name": "nibrs_relationship"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_SUSPECTED_DRUG.csv", "local_path": "./NIBRS_SUSPECTED_DRUG.csv", "table_name": "nibrs_suspected_drug"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_SUSPECTED_DRUG_TYPE.csv", "local_path": "./NIBRS_SUSPECTED_DRUG_TYPE.csv", "table_name": "nibrs_suspected_drug_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_SUSPECT_USING.csv", "local_path": "./NIBRS_SUSPECT_USING.csv", "table_name": "nibrs_suspect_using"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_USING_LIST.csv", "local_path": "./NIBRS_USING_LIST.csv", "table_name": "nibrs_using_list"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_VICTIM.csv", "local_path": "./NIBRS_VICTIM.csv", "table_name": "nibrs_victim"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_VICTIM_CIRCUMSTANCES.csv", "local_path": "./NIBRS_VICTIM_CIRCUMSTANCES.csv", "table_name": "nibrs_victim_circumstances"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_VICTIM_INJURY.csv", "local_path": "./NIBRS_VICTIM_INJURY.csv", "table_name": "nibrs_victim_injury"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_VICTIM_OFFENDER_REL.csv", "local_path": "./NIBRS_VICTIM_OFFENDER_REL.csv", "table_name": "nibrs_victim_offender_rel"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_VICTIM_OFFENSE.csv", "local_path": "./NIBRS_VICTIM_OFFENSE.csv", "table_name": "nibrs_victim_offense"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_VICTIM_TYPE.csv", "local_path": "./NIBRS_VICTIM_TYPE.csv", "table_name": "nibrs_victim_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_WEAPON.csv", "local_path": "./NIBRS_WEAPON.csv", "table_name": "nibrs_weapon"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_WEAPON_TYPE.csv", "local_path": "./NIBRS_WEAPON_TYPE.csv", "table_name": "nibrs_weapon_type"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_incident.csv", "local_path": "./NIBRS_incident.csv", "table_name": "nibrs_incident"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/NIBRS_month.csv", "local_path": "./NIBRS_month.csv", "table_name": "nibrs_month"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/REF_RACE.csv", "local_path": "./REF_RACE.csv", "table_name": "ref_race"},
    {"file_path": f"gs://fbi_nibrs/{state}-2023/REF_STATE.csv", "local_path": "./REF_STATE.csv", "table_name": "ref_state"}
]

# Connect to PostgreSQL
conn = psycopg2.connect("dbname=nibrs user=completely-epic-seahorse password=g%_9LeR%U|NlZo@7 host=10.12.0.3")
cur = conn.cursor()

# Loop through each file and load data
for item in files_to_load:
    file_path = item["file_path"]
    local_path = item["local_path"]
    table_name = item["table_name"]
    
    subprocess.run(['gsutil', 'cp', file_path, local_path])  # Download file from GCS
    print(f"Downloaded {file_path} to {local_path}.")

    if table_name == "nibrs_victim":
        # Load the CSV into a DataFrame
        df = pd.read_csv(local_path, dtype={'age_range_low_num': 'Int64', 'age_code_range_high': 'Int64',
                                            'assignment_type_id': 'string', 'activity_type_id': 'string',
                                            'outside_agency_id': 'string'})
        
        # Clean the DataFrame
        df['age_num'] = df['age_num'].replace({'BB': 0, 'NB': 0, 'NN': 0, 'NS': 0})
        #df['age_range_low_num'] = df['age_range_low_num'].fillna(0).astype(int)
        
        # Write the cleaned data back to a new CSV
        cleaned_file = "./cleaned_" + table_name + ".csv"
        df.to_csv(cleaned_file, index=False)
        file_to_load = cleaned_file
    else:
        file_to_load = local_path

    with open(file_to_load, 'r') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        c = ', '.join(header)
        if (table_name == "agencies"):
            c1 = c.replace("officer+male_civilian", "total")
            columns = c1.replace("officer+female_civilian", "total")
        elif (table_name == "nibrs_bias_list"):
            columns = c.replace("_category", "_name")
        elif (table_name == "nibrs_circumstances"):
            columns = c.replace("circumstance_", "circumstances_")
        elif (table_name == "nibrs_justifiable_force"):
            columns = c.replace("fore_", "force_")
        elif (table_name == "nibrs_victim"):
            columns = c.replace("age_code_range_high", "age_range_high_num")
        else:
            columns = c

        cur.copy_expert(f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER, NULL '')", f)
        print(f"Data from {file_path} loaded into {table_name}.")

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()
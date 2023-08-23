import json
import logging
import pandas as pd
import io
import boto3
import urllib3
import psycopg2
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Initialize S3 client and HTTP client
    s3 = boto3.client('s3')
    http = urllib3.PoolManager()

    # Create an empty list to store Pokemon information
    pokemon_info = []

    # Set source bucket and key
    source_bucket = 'apprentice-training-aavash-raw-ml-dev'
    source_key = 'raw_data.csv'

    # Get raw CSV content from source bucket
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    csv_content = response['Body'].read()

    # Read raw data into a DataFrame
    pokemon_df = pd.read_csv(io.BytesIO(csv_content))

    # Iterate through each row in the DataFrame
    for index, row in pokemon_df.iterrows():
        name = row['Name']
        url = row['URL']

        # Make a request to the URL to get detailed information
        response = http.request('GET', url)
        data = json.loads(response.data.decode('utf-8'))

        # Extract the desired information and append to the list
        pokemon_info.append({
            'Name': name,
            'Height': data['height'],
            'Weight': data['weight'],
            'Base Experience': data['base_experience'],
            'Abilities': [ability['ability']['name'] for ability in data['abilities']],
            'Types': [type_entry['type']['name'] for type_entry in data['types']]
            # Add more fields as needed
        })

    # ... (Continued from the previous response)

    # Create a DataFrame from the collected Pokemon information
    pokemon_info_df = pd.DataFrame(pokemon_info)

    # Apply filters to the DataFrame
    filtered_pokemon_info_df = pokemon_info_df[
        (pokemon_info_df['Base Experience'] >= 100) &
        (pokemon_info_df['Weight'] > 50) &
        (pokemon_info_df['Height'] > 10)
    ]

    # Set destination bucket and key
    destination_bucket = 'apprentice-training-aavash-cleaned-ml-dev'
    destination_key = 'cleaned_file.csv'

    # Convert the filtered DataFrame to CSV content
    cleaned_csv_content = filtered_pokemon_info_df.to_csv(index=False)

    # Upload cleaned CSV to destination bucket
    s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=cleaned_csv_content)
    
    # Insert Data into AWS RD 
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        
        cur = conn.cursor()

        # Insert data into the table
        for _, row in filtered_pokemon_info_df.iterrows():
            insert_query = """
            INSERT INTO aavash (name, height, weight, base_experience, abilities, types)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            values = (
                row['Name'],
                row['Height'],
                row['Weight'],
                row['Base Experience'],
                json.dumps(row['Abilities']),
                json.dumps(row['Types'])
            )
            cur.execute(insert_query, values)
        
        conn.commit()
        cur.close()

    except psycopg2.Error as e:
        logger.error("Error while inserting data into the database: %s", e)


    finally:
        if conn:
            conn.close()

    # Log successful completion
    logger.info("Data cleaning and storage completed successfully.")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data cleaning and storage completed successfully.'})
    }


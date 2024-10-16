import boto3
import pandas as pd
import urllib.parse
import os
import io

# AWS SDK clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# S3 and Glue catalog environment variables
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']

def get_glue_table_schema(database_name, table_name):
    """
    Get the schema of the Glue table to ensure the DataFrame matches the updated schema.
    """
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        columns = response['Table']['StorageDescriptor']['Columns']
        return {col['Name']: col['Type'] for col in columns}
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Table {table_name} not found in database {database_name}.")
        raise

def cast_dataframe_to_glue_schema(df, glue_schema):
    """
    Cast the DataFrame columns to match the types in the Glue table schema.
    """
    for column, dtype in glue_schema.items():
        if column in df.columns:
            if dtype == 'bigint':
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
            elif dtype == 'double':
                df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
            elif dtype == 'boolean':
                df[column] = df[column].astype(bool)
            elif dtype == 'string':
                df[column] = df[column].astype(str)
            elif dtype == 'timestamp':
                df[column] = pd.to_datetime(df[column], errors='coerce')
    return df

def infer_glue_schema_from_dataframe(df):
    """
    Infer the schema for AWS Glue based on the DataFrame's columns and data types.
    """
    glue_schema = []
    for column, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            glue_type = 'int'
        elif pd.api.types.is_float_dtype(dtype):
            glue_type = 'double'
        elif pd.api.types.is_bool_dtype(dtype):
            glue_type = 'boolean'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            glue_type = 'timestamp'
        else:
            glue_type = 'string'  # Default to string if no match

        glue_schema.append({'Name': column, 'Type': glue_type})
    
    return glue_schema

def create_glue_table_if_not_exists(database_name, table_name, s3_location, glue_schema):
    """
    Create the AWS Glue table dynamically if it doesn't exist.
    """
    try:
        # Attempt to get the table, if it doesn't exist, an EntityNotFoundException will be raised
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
        print(f"Table {table_name} already exists in database {database_name}.")
    except glue_client.exceptions.EntityNotFoundException:
        # If table not found, create it
        print(f"Table {table_name} does not exist. Creating with inferred schema...")
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': glue_schema,  # Dynamically inferred schema
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'Compressed': False,
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        print(f"Table {table_name} created successfully with schema: {glue_schema}")

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        # Fetching file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()

        # Load JSON into DataFrame
        df_raw = pd.read_json(io.BytesIO(content))

        # Normalize JSON data
        df_step_1 = pd.json_normalize(df_raw['items'])

        try:
            # Fetch the updated Glue table schema if the table exists
            glue_schema = get_glue_table_schema(os_input_glue_catalog_db_name, os_input_glue_catalog_table_name)
            # Cast the DataFrame columns to match the Glue schema
            df_step_1 = cast_dataframe_to_glue_schema(df_step_1, glue_schema)
        except glue_client.exceptions.EntityNotFoundException:
            # If Glue table doesn't exist, infer schema from DataFrame and create table
            glue_schema = infer_glue_schema_from_dataframe(df_step_1)
            create_glue_table_if_not_exists(
                os_input_glue_catalog_db_name,
                os_input_glue_catalog_table_name,
                f"s3://{os_input_s3_cleansed_layer}/",
                glue_schema
            )

        # Convert DataFrame to Parquet and upload to S3
        parquet_buffer = io.BytesIO()
        df_step_1.to_parquet(parquet_buffer, index=False)

        # Define S3 key for the Parquet file
        s3_key = f"{key.replace('.json', '.parquet')}"
        s3_client.put_object(
            Bucket=os_input_s3_cleansed_layer,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )

        return {
            'statusCode': 200,
            'body': f"File {key} processed and saved as Parquet."
        }

    except Exception as e:
        print(e)
        print(f"Error processing object {key} from bucket {bucket}.")
        raise e
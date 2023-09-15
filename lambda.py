import datetime
import boto3

clientS3 = boto3.client('s3')
clientAthena = boto3.client('athena')

def lambda_handler(event, context):
    
    utc_time = datetime.datetime.utcnow()
    utc_minus_3_offset = datetime.timedelta(hours=-3)
    saopaulo_time = utc_time + utc_minus_3_offset

    bucket_name = "dummy-bucket"
    folder_prefix = "prefix/"
    folder_path = f"year={saopaulo_time.year}/month={saopaulo_time.month:02d}/day={saopaulo_time.day:02d}"

    try:
        clientS3.head_object(Bucket=bucket_name, Key=f"{folder_prefix}{folder_path}/")
        print(f"The folder '{folder_prefix}{folder_path}' already exists.")
    except Exception as e:
        if e.response['Error']['Code'] == '404':
            #Let there be folder 🙏
            clientS3.put_object(Bucket=bucket_name, Key=f"{folder_prefix}{folder_path}/")
            print(f"The folder '{folder_prefix}{folder_path}' has been created.")

            #Cuz folder was created, call Athena to load said folder as partition.
            sql = 'MSCK REPAIR TABLE db_database.tb_table'
            context = {'Database': 'db_database'}

            responseAthena = clientAthena.start_query_execution(QueryString = sql, 
                                                                QueryExecutionContext = context,
                                                                WorkGroup = "primary")
            print(f"Athena response: {responseAthena}")      
        else:
            print(f"An error occurred: {e}")
    
    return 0

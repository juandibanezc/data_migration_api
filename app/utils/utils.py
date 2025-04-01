from datetime import datetime
from typing import Optional

import fastavro
import boto3

from app.core.config import settings

def read_avro_file(file_path: str):
    with open(file_path, "rb") as avro_file:
      reader = fastavro.reader(avro_file)
      records = list(reader)
    return records

def write_avro_file(file_path: str, schema: dict, records: list):
    """Writes data records to an AVRO file."""
    with open(file_path, "wb") as avro_file:
        fastavro.writer(avro_file, schema, records)

def deserialize_record(record, model):
    """
    Convert ISO 8601 datetime string back into a datetime object.
    """
    deserialized_record = {}
    for column in model.__table__.columns:
        column_name = column.name
        if column_name in record:
            value = record[column_name]
            if column_name == "datetime":  # Check for ISO datetime format
                try:
                    deserialized_record[column_name] = datetime.fromisoformat(value)
                except (TypeError,ValueError):
                    deserialized_record[column_name] = value  # Keep as string if conversion fails
            else:
                deserialized_record[column_name] = value
    return deserialized_record

def fetch_csv_from_s3(file_path: str) -> Optional[str]:
  """
  Fetches the content of a CSV file from an S3 bucket as a string.

  Args:
    file_name (str): The name of the file to fetch from the S3 bucket.

  Returns:
    Optional[str]: The content of the CSV file as a decoded string, or None if the file is empty.

  Raises:
    Exception: If there is an error fetching the file from S3.
  """
  # Initialize S3 Client
  s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.S3_REGION
  )

  try:
    s3_object = s3_client.get_object(Bucket=settings.S3_BUCKET_NAME, Key=file_path)
    content = s3_object["Body"].read().decode("utf-8")
    return content if content else None
  
  except Exception as e:
    raise Exception(f"Error fetching file {file_path} from S3: {str(e)}")
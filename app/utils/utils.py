from typing import Optional

import boto3

from app.core.config import settings

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
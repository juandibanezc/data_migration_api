import traceback
from io import StringIO
from typing import Dict, Any

import pandas as pd

from app.core.logging_config import logger
from app.core.database import engine
from app.utils.utils import fetch_csv_from_s3


CSV_FILES = {
    "departments.csv": "departments",
    "jobs.csv": "jobs",
    "hired_employees.csv": "hired_employees"
}
  
def migrate_historical_data() -> Dict[str, Any]:
  """
  Load historical data from CSV files and insert it into th target database.
  
  Returns:
    dict: A dictionary containing a success message upon completion.
  
  Raises:
    FileNotFoundError: If a CSV file for a specific table is not found.
    Exception: For any unexpected errors during the migration process.
  """
  for file_name, table in CSV_FILES.items():
    try:
      file_path = f"raw_data/{file_name}"
      csv_content = fetch_csv_from_s3(file_path)
      file_content = StringIO(csv_content)
      df = pd.read_csv(file_content, sep=',', header=None)
      df = df.where(pd.notnull(df), None)

      # Process Departments
      if table == "departments":
        df.columns = ["id", "name"]
        df = df.astype({"id": int, "name": str})

      # Process Jobs
      elif table == "jobs":
        df.columns = ["id", "name"]
        df = df.astype({"id": int, "name": str})

      # Process Hired Employees
      elif table == "hired_employees":
        df.columns = ["id", "name", "datetime", "department_id", "job_id"]
        df = df.astype({"id": int, "name": str, "department_id": int, "job_id": int}, errors="ignore")
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%dT%H:%M:%SZ", errors="coerce")

      # Bulk insert
      df.to_sql(table, con=engine, if_exists="append", index=False, method="multi", chunksize=10000)
      logger.info(f"Successfully migrated {len(df)} records into {table}")

    except FileNotFoundError as e:
      logger.error(f"File not found: {file_path}. Error: {e}")
      raise FileNotFoundError(f"CSV file for table '{table}' not found.")
    except Exception as e:
      logger.error(f"Unexpected error occurred while migrating data for table {table}. Error: {e}")
      logger.error(traceback.format_exc())
      raise Exception(f"Unexpected error: {str(e)}")

  return {"message": "Historical data migration completed successfully."}
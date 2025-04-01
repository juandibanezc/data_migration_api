import os
import traceback
from typing import Type

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta

from app.models.department import Department
from app.models.job import Job
from app.models.hired_employee import HiredEmployee
from app.core.logging_config import logger
from app.utils.utils import write_avro_file, deserialize_record, read_avro_file

# Directory where backup files will be stored
BACKUP_DIR = "backups"
os.makedirs(BACKUP_DIR, exist_ok=True)

# Mapping table names to ORM models
TABLE_MODELS = {
    "hired_employees": HiredEmployee,
    "departments": Department,
    "jobs": Job,
}

def backup_table(db: Session, model: Type[DeclarativeMeta], table_name: str, schema: dict) -> None:
  """
  Backs up a table to an AVRO file.

  Args:
    db (Session): The SQLAlchemy database session.
    model (Type[DeclarativeMeta]): The SQLAlchemy ORM model representing the table.
    table_name (str): The name of the table to back up.
    schema (dict): The AVRO schema for the table.

  Returns:
    None
  """
  records = db.query(model).all()
  
  if not records:
    logger.info(f"No records found for table {table_name}. Skipping backup.")
    return

  # Convert ORM objects to dictionaries
  records_list = [record.__dict__ for record in records]
  for record in records_list:
    record.pop("_sa_instance_state", None)  # Remove SQLAlchemy metadata
    if "datetime" in record:
      record["datetime"] = record["datetime"].strftime("%Y-%m-%dT%H:%M:%SZ") if record["datetime"] else None

  # Generate file path
  file_path = os.path.join(BACKUP_DIR, f"{table_name}.avro")

  # Write to AVRO
  write_avro_file(file_path, schema, records_list)
  logger.info(f"Backup for {table_name} saved at {file_path}")


def backup_database(db: Session) -> dict:
  """
  Backs up all tables in the database to AVRO format.

  Args:
    db (Session): The SQLAlchemy database session.

  Returns:
    dict: A dictionary containing a success message if the backup is completed successfully.

  Raises:
    HTTPException: If an unexpected error occurs during the backup process.
  """
  logger.info("Starting database backup...")

  # Define AVRO schemas for each table
  department_schema = {
    "type": "record",
    "name": "Department",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
    ]
  }

  job_schema = {
    "type": "record",
    "name": "Job",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
    ]
  }

  hired_employee_schema = {
    "type": "record",
    "name": "HiredEmployee",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "datetime", "type": ["null", "string"]},  # Store as ISO format
      {"name": "department_id", "type": ["null", "int"]},
      {"name": "job_id", "type": ["null", "int"]},
    ]
  }

  try:
    # Backup each table
    backup_table(db, Department, "departments", department_schema)
    backup_table(db, Job, "jobs", job_schema)
    backup_table(db, HiredEmployee, "hired_employees", hired_employee_schema)

    result = "Database backup completed successfully!"
    logger.info(result)
    return {"message": result}

  except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
    logger.error(traceback.format_exc())
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create a new backup for the database."
    )

def restore_table(db: Session, table_name: str) -> dict:
  """
  Restores a table from its Avro backup file using fastavro.

  Args:
    db (Session): The SQLAlchemy database session.
    table_name (str): The name of the table to restore.

  Returns:
    dict: A dictionary containing a success message if the restore is completed successfully.

  Raises:
    ValueError: If the table name is not recognized or if there is an integrity error during restore.
    FileNotFoundError: If the backup file for the specified table is not found.
  """
  logger.info(f"Starting restore for table: {table_name}")

  if table_name not in TABLE_MODELS:
    raise ValueError(f"Table '{table_name}' is not recognized.")

  model = TABLE_MODELS[table_name]
  backup_file = os.path.join(BACKUP_DIR, f"{table_name}.avro")

  if not os.path.exists(backup_file):
    raise FileNotFoundError(f"Backup file for table '{table_name}' not found.")

  # Read Avro file
  records = read_avro_file(file_path=backup_file)

  # Deserialize records (convert datetime fields)
  deserialized_records = [deserialize_record(record, model) for record in records]

  # Truncate table before restore
  db.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
  db.commit()
  logger.info(f"Table {table_name} truncated before restore.")

  # Restore records into database
  try:
    db.bulk_insert_mappings(model, deserialized_records)
    db.commit()
    logger.info(f"Successfully restored {len(deserialized_records)} records to {table_name}.")
    return {"message": f"Successfully restored {len(deserialized_records)} records to {table_name}."}
  except IntegrityError as e:
    db.rollback()
    logger.error(f"Integrity error while restoring {table_name}: {str(e)}")
    raise ValueError(f"Integrity error: {str(e)}")
  except Exception as e:
    db.rollback()
    logger.error(f"Unexpected error restoring {table_name}: {str(e)}")
    raise Exception(f"Unexpected error: {str(e)}")

import traceback
from datetime import datetime
from typing import List, Dict, Any

from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

from app.models.hired_employee import HiredEmployee
from app.models.department import Department
from app.models.job import Job
from app.core.logging_config import logger

def validate_transaction_data(
  db: Session, 
  hired_employees: List[Dict[str, Any]], 
  departments: List[Dict[str, Any]], 
  jobs: List[Dict[str, Any]]
) -> Dict[str, str]:
    """
    Validates incoming transaction data to ensure data integrity and consistency.

    Args:
      db (Session): SQLAlchemy database session.
      hired_employees (List[Dict[str, any]]): List of dictionaries representing hired employees.
      departments (List[Dict[str, any]]): List of dictionaries representing departments.
      jobs (List[Dict[str, any]]): List of dictionaries representing jobs.

    Raises:
      HTTPException: If validation fails, raises an HTTPException with status code 422 and error details.
    """
    errors = []

    # Get existing department & job IDs from the database
    existing_department_ids = {row[0] for row in db.execute(text("SELECT id FROM departments")).fetchall()}
    existing_job_ids = {row[0] for row in db.execute(text("SELECT id FROM jobs")).fetchall()}

    # Validate new departments
    new_department_ids = set()
    for row in departments:
        if "id" not in row or not isinstance(row["id"], int):
            errors.append("Each department must have a valid 'id' (integer).")
        if "name" not in row or not isinstance(row["name"], str):
            errors.append("Each department must have a valid 'name' (string).")
        if row["id"] in existing_department_ids:
            errors.append(f"Department ID {row['id']} already exists.")
        new_department_ids.add(row["id"])

    # Validate new jobs
    new_job_ids = set()
    for row in jobs:
        if "id" not in row or not isinstance(row["id"], int):
            errors.append("Each job must have a valid 'id' (integer).")
        if "name" not in row or not isinstance(row["name"], str):
            errors.append("Each job must have a valid 'name' (string).")
        if row["id"] in existing_job_ids:
            errors.append(f"Job ID {row['id']} already exists.")
        new_job_ids.add(row["id"])

    # Validate hired employees
    for row in hired_employees:
        if "id" not in row or not isinstance(row["id"], int):
            errors.append("Each employee must have a valid 'id' (integer).")

        if "name" not in row or not isinstance(row["name"], str):
            errors.append("Each employee must have a valid 'name' (string).")

        if "datetime" not in row or not isinstance(row["datetime"], str):
            errors.append("Each employee must have a valid 'datetime' (ISO format string).")
        else:
            try:
                # Convert datetime string to proper format
                row["datetime"] = datetime.fromisoformat(row["datetime"])
            except ValueError:
                errors.append(f"Invalid datetime format: {row['datetime']}")

        if "department_id" not in row or not isinstance(row["department_id"], int):
            errors.append("Each employee must have a valid 'department_id' (integer).")
        elif row["department_id"] not in existing_department_ids and row["department_id"] not in new_department_ids:
            errors.append(f"Department ID {row['department_id']} does not exist and is not in the new departments list.")

        if "job_id" not in row or not isinstance(row["job_id"], int):
            errors.append("Each employee must have a valid 'job_id' (integer).")
        elif row["job_id"] not in existing_job_ids and row["job_id"] not in new_job_ids:
            errors.append(f"Job ID {row['job_id']} does not exist and is not in the new jobs list.")

    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"errors": errors}
        )

def insert_new_transactions(
  db: Session, 
  hired_employees: List[Dict[str, Any]], 
  departments: List[Dict[str, Any]], 
  jobs: List[Dict[str, Any]]
) -> Dict[str, str]:
  """
  Inserts new transactions into the database, including departments, jobs, and hired employees.

  Args:
    db (Session): SQLAlchemy database session.
    hired_employees (List[Dict[str, Any]]): List of dictionaries representing hired employees.
      Each dictionary must contain:
        - "id" (int): Employee ID.
        - "name" (str): Employee name.
        - "datetime" (str): ISO format datetime string.
        - "department_id" (int): ID of the department.
        - "job_id" (int): ID of the job.
    departments (List[Dict[str, Any]]): List of dictionaries representing departments.
      Each dictionary must contain:
        - "id" (int): Unique department ID.
        - "name" (str): Department name.
    jobs (List[Dict[str, Any]]): List of dictionaries representing jobs.
      Each dictionary must contain:
        - "id" (int): Unique job ID.
        - "name" (str): Job name.

  Returns:
    Dict[str, str]: A dictionary containing a success message with the count of inserted records.

  Raises:
    HTTPException: If validation fails, batch size is invalid, or a database error occurs.
  """
  try:
    # Validate batch size
    total_records = len(hired_employees) + len(departments) + len(jobs)
    if total_records == 0 or total_records > 1000:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Batch size must be between 1 and 1000 rows."
      )

    # Validate data rules
    validate_transaction_data(db, hired_employees, departments, jobs)

    # Insert new departments
    if departments:
      db.bulk_insert_mappings(Department, departments)
      logger.info(f"Inserted {len(departments)} new departments.")

    # Insert new jobs
    if jobs:
      db.bulk_insert_mappings(Job, jobs)
      logger.info(f"Inserted {len(jobs)} new jobs.")

    # Insert hired employees
    if hired_employees:
      db.bulk_insert_mappings(HiredEmployee, hired_employees)
      logger.info(f"Inserted {len(hired_employees)} new hired employees.")

    db.commit()
    return {"message": f"Inserted {len(departments)} departments, {len(jobs)} jobs, {len(hired_employees)} hired employees successfully."}

  except IntegrityError as e:
    db.rollback()
    logger.error(f"Database constraint error: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Database constraint error. Ensure unique department and job IDs."
    )
  except HTTPException as e:
    logger.error(f"Transaction error: {e.detail}")
    raise e  # Forward HTTPException
  
  except Exception as e:
    db.rollback()
    logger.error(f"Unexpected error: {str(e)}")
    logger.error(traceback.format_exc())
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to insert new transactions."
    )

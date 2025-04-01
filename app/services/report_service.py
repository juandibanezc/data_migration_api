from typing import List, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy import func, case

from app.models.hired_employee import HiredEmployee
from app.models.department import Department
from app.models.job import Job


def get_hired_employees_per_quarter(db: Session) -> List[Dict[str, Any]]:
  """
  Retrieves the report of the number of employees hired per job and department in 2021, divided by quarter.

  Args:
    db (Session): The SQLAlchemy database session.

  Returns:
    List[Dict[str, Any]]: A list of dictionaries, each containing the department name, job name,
                and the number of employees hired in each quarter (Q1, Q2, Q3, Q4).
  """
  quarter_case = case(
    (func.extract("month", HiredEmployee.datetime).between(1, 3), "Q1"),
    (func.extract("month", HiredEmployee.datetime).between(4, 6), "Q2"),
    (func.extract("month", HiredEmployee.datetime).between(7, 9), "Q3"),
    (func.extract("month", HiredEmployee.datetime).between(10, 12), "Q4"),
  )

  results = (
    db.query(
      Department.name.label("department"),
      Job.name.label("job"),
      func.count().filter(quarter_case == "Q1").label("Q1"),
      func.count().filter(quarter_case == "Q2").label("Q2"),
      func.count().filter(quarter_case == "Q3").label("Q3"),
      func.count().filter(quarter_case == "Q4").label("Q4"),
    )
    .join(HiredEmployee, Department.id == HiredEmployee.department_id)
    .join(Job, Job.id == HiredEmployee.job_id)
    .filter(func.extract("year", HiredEmployee.datetime) == 2021)
    .group_by(Department.name, Job.name)
    .order_by(Department.name, Job.name)
    .all()
  )

  return [dict(row._asdict()) for row in results]

def get_departments_hiring_above_average(db: Session) -> List[Dict[str, Any]]:
  """
  Retrieves the report of the list of departments that hired more employees than the average number of hires in 2021.

  Args:
    db (Session): The SQLAlchemy database session.

  Returns:
    List[Dict[str, Any]]: A list of dictionaries, each containing the department ID, department name,
                and the number of employees hired, for departments that hired above the average.
  """
  # Common Table Expression (CTE) to calculate hires per department
  cte = (
    db.query(
      Department.id.label("department_id"),
      func.count(HiredEmployee.id).label("hired_count")
    )
    .join(HiredEmployee, Department.id == HiredEmployee.department_id)
    .filter(func.extract("year", HiredEmployee.datetime) == 2021)
    .group_by(Department.id)
    .cte("department_hires")
  )

  # Calculate the average number of hires across all departments
  avg_hires = db.query(func.avg(cte.c.hired_count)).scalar()

  # Query departments that hired more than the average
  results = (
    db.query(
      Department.id,
      Department.name.label("department"),
      func.count(HiredEmployee.id).label("hired"),
    )
    .join(HiredEmployee, Department.id == HiredEmployee.department_id)
    .filter(func.extract("year", HiredEmployee.datetime) == 2021)
    .group_by(Department.id, Department.name)
    .having(func.count(HiredEmployee.id) > avg_hires)
    .order_by(func.count(HiredEmployee.id).desc())
    .all()
  )

  # Convert results to a list of dictionaries
  return [dict(row._asdict()) for row in results]

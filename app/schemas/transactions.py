from typing import Optional, List
from pydantic import BaseModel

from app.schemas.department import DepartmentSchema
from app.schemas.hired_employee import HiredEmployeeSchema
from app.schemas.job import JobSchema

class TransactionRequestSchema(BaseModel):
    departments: Optional[List[DepartmentSchema]] = []
    jobs: Optional[List[JobSchema]] = []
    hired_employees: List[HiredEmployeeSchema]  # Must have at least one employee
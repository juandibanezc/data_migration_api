from pydantic import BaseModel

class HiredEmployeeSchema(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int
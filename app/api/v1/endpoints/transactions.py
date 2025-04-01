from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.security import get_api_key
from app.services.transaction_service import insert_new_transactions
from app.schemas.transactions import TransactionRequestSchema

router = APIRouter()

@router.post("/insert_new_data")
def insert_data_endpoint(request: TransactionRequestSchema, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """API Endpoint to insert new transactions into the database."""
    return insert_new_transactions(
        db=db,
        hired_employees=[employee.dict() for employee in request.hired_employees],
        departments=[dept.dict() for dept in request.departments] if request.departments else [],
        jobs=[job.dict() for job in request.jobs] if request.jobs else []
    )

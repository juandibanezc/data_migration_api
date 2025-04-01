import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.security import get_api_key
from app.services.report_service import *
from app.core.logging_config import logger

router = APIRouter(prefix="/reports", tags=["Reports"])

@router.get("/hired_per_quarter")
def hired_employees_per_quarter(db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    API Endpoint to generate a report of the number of employees hired 
    per job and department in 2021, divided by quarter.
    """
    try:
      return get_hired_employees_per_quarter(db)
    except Exception as e:
      logger.error(traceback.format_exc())
      raise HTTPException(status_code=500, detail="Internal Server Error")
       

@router.get("/departments_above_average")
def departments_above_average(db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    API Endpoint to generate a report of the departments that hired more employees than the mean in 2021.
    """
    return get_departments_hiring_above_average(db)

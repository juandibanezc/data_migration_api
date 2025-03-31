import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import Engine
from typing import Any

from app.core.security import get_api_key
from app.services.migration_service import migrate_historical_data
from app.core.logging_config import logger


router = APIRouter()

@router.post("/migrate_historic_data")
async def migrate_endpoint(api_key: str = Depends(get_api_key)) -> Any:
  """
  API endpoint to trigger the migration of historical data.
  """
  try:
    return migrate_historical_data()
  except FileNotFoundError as e:
    raise HTTPException(status_code=404, detail=str(e))
  except Exception as e:
    logger.error(traceback.format_exc())
    raise HTTPException(status_code=500, detail="Internal Server Error")
from fastapi import FastAPI
from app.core.config import settings
from app.api.v1.endpoints import migrations, transactions, backups, reports

app = FastAPI(
    title="Data Migration API",
    description="A FastAPI service for data migration.",
    version="1.0.0"
)

# API routes
app.include_router(migrations.router, prefix="/migrations", tags=["Migrations"])
app.include_router(transactions.router, prefix="/transactions", tags=["Transactions"])
app.include_router(backups.router, prefix="/backups", tags=["Backups"])
app.include_router(reports.router, prefix="/reports", tags=["Reports"])

@app.get("/", tags=["Health Check"])
def health_check():
    """Check if the API is running."""
    return {"status": "OK", "message": "FastAPI Data Migration is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

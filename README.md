# 🚀 Big Data Data Migration & Reporting API

**A scalable, high-performance API system** engineered for seamless data ingestion, validation, migration, reporting, and backup operations on **PostgreSQL** databases.
Designed with modular architecture and built on modern **Python** tools like **FastAPI**, **AVRO**, and **Docker**, this application ensures secure, efficient, and auditable data pipelines — making it ideal for both real-time transactional inputs and batch historical migrations.

![alt text](data_migration_api/docs/images/api_docs.png)

---

## 🔥 Features

| Category        | Description |
|----------------|-------------|
| ✅ **Data Migration**  | Load historical CSV data (via Pandas) from S3 into normalized PostgreSQL tables |
| ✅ **Transaction API** | Insert new records (1–1000) into the system with validation |
| ✅ **Backup API**      | Persist table snapshots as AVRO files in the filesystem |
| ✅ **Restore API**     | Reload tables from their AVRO backups (with option to truncate or upsert) |
| ✅ **Reports**         | Generate analytics reports via optimized SQL queries |
| ✅ **Authentication**  | Secured with API Key-based access to all endpoints |
| ✅ **Docker Support**  | Fully containerized and ready for deployment |
| ✅ **Logging & Errors**| Centralized logging using FastAPI's logger and Python logging module |

---

## 🛠️ Technologies Used

- **FastAPI**
- **PostgreSQL**
- **SQLAlchemy**
- **Pandas**
- **FastAvro**
- **Uvicorn**
- **Docker**
- **API Key Auth**
- **Pydantic**

---

## 🚀 Getting started

### 1️⃣ Clone and Set Up the Project

```bash
git clone https://github.com/juandibanezc/data_migration_api.git
cd data_migration_api
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2️⃣ Add Environment Variables

```env
DATABASE_URL=postgresql://user:pass@localhost:5432/yourdb
API_KEY=secure-api-key
S3_BUCKET_NAME=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-access-key
```

---

## 🐳 Running the App with Docker

```bash
docker build -t data-migration-app .
docker run -p 8000:8000 --env-file .env -v $(pwd)/backups:/app/backups data-migration-app
```

---

## 🔐 Authentication

Use `X-API-KEY` in request headers:
```http
X-API-KEY: your-secure-api-key
```

---

## 🧪 Testing

Use Swagger at `http://localhost:8000/docs` or Postman with the API key header.
# REDMANE FastAPI

A FastAPI backend for the REDMANE research data management system with support for both SQLite and PostgreSQL databases.

## Quick Start

### 1. Install Dependencies
```bash
pip install fastapi uvicorn psycopg2-binary python-dotenv
```

### 2. Database Setup

#### Option A: PostgreSQL (Recommended)
```bash
# Set PostgreSQL PATH (Windows)
$env:PATH += ";C:\Program Files\PostgreSQL\17\bin"

# Create database
psql -U postgres -c "CREATE DATABASE redmane_db;"

# Import schema from public data repository
psql -U postgres -d redmane_db -f REDMANE_fastapi_public_data\readmedatabase.sql
```

#### Option B: SQLite (Original)
```bash
# Create data directory
mkdir data

# Copy SQLite database from public data repository
copy REDMANE_fastapi_public_data\data_redmane.db data\
```

### 3. Configure Environment
Edit `config.env` with your database password:
```bash
DB_PASSWORD=your_actual_password
```

### 4. Run Application
```bash
# PostgreSQL version (recommended)
python main_postgresql.py

# Or SQLite version
python main.py
```

### 5. Test API
- **Interactive Docs**: http://localhost:8888/docs
- **Projects**: http://localhost:8888/projects/
- **Patients**: http://localhost:8888/patients/0?project_id=1

## Project Structure

```
REDMANE_fastapi/
├── main_postgresql.py          # PostgreSQL version (recommended)
├── main.py                     # SQLite version
├── config.env                  # Database configuration
├── data/                       # SQLite database folder (for SQLite version)
├── sample_data/                # Sample CSV files and import scripts
├── sample_files/               # Sample file tracking data
└── REDMANE_fastapi_public_data/ # Public schema and sample data
```

## Database Migration

For migration details from SQLite to PostgreSQL, see: [MIGRATION_SQLITE_TO_POSTGRESQL.md](MIGRATION_SQLITE_TO_POSTGRESQL.md)

## Related Projects

- **Frontend**: [REDMANE_react.js](https://github.com/WEHI-RCPStudentInternship/REDMANE_react.js/tree/Semester_1_2025)
- **Public Data**: [REDMANE_fastapi_public_data](https://github.com/WEHI-ResearchComputing/REDMANE_fastapi_public_data)

## Troubleshooting

### PostgreSQL Issues
- **Connection failed**: Check password in `config.env`
- **Database not found**: Run database creation commands
- **PATH not set**: Restart terminal after setting PATH

### SQLite Issues
- **File not found**: Ensure `data/data_redmane.db` exists
- **Permission error**: Check file permissions
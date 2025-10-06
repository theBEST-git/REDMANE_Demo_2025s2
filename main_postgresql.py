from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config.env')

app = FastAPI()

# Allow all origins (for development, consider restricting to specific origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# PostgreSQL database configuration from environment variables
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'redmane_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
    'port': int(os.getenv('DB_PORT', 5432))
}

def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        return conn
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

def get_db_cursor(conn):
    """Get PostgreSQL database cursor with RealDictCursor for named access"""
    return conn.cursor(cursor_factory=RealDictCursor)

# Initialize the database and create the tables if they don't exist
def init_db():
    """Initialize PostgreSQL database - tables should already exist from schema import"""
    conn = get_db_connection()
    cur = get_db_cursor(conn)
    
    # Since we imported the schema, tables should already exist
    # We can verify by checking if tables exist
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('projects', 'datasets', 'patients', 'samples', 'files')
    """)
    
    existing_tables = [row['table_name'] for row in cur.fetchall()]
    print(f"Existing tables: {existing_tables}")
    
    conn.close()

# Call the function to initialize the database
init_db()

# Pydantic model for Project
class Project(BaseModel):
    id: int
    name: str
    status: str

# Pydantic model for Dataset
class Dataset(BaseModel):
    id: int
    project_id: int
    name: str

class DatasetMetadata(BaseModel):
    id: int
    dataset_id: int
    key: str
    value: str

class DatasetWithMetadata(Dataset):
    metadata: List[DatasetMetadata] = []


# Pydantic model for Patient
class Patient(BaseModel):
    id: int
    project_id: int
    ext_patient_id: str
    ext_patient_url: str
    public_patient_id: Optional[str]

# Pydantic model for Patient with sample count
class PatientWithSampleCount(Patient):
    sample_count: int

# Pydantic model for PatientMetadata
class PatientMetadata(BaseModel):
    id: int
    patient_id: int
    key: str
    value: str

# Pydantic model for Patient with Metadata
class PatientWithMetadata(Patient):
    metadata: List[PatientMetadata] = []

# Pydantic model for SampleMetadata
class SampleMetadata(BaseModel):
    id: int
    sample_id: int
    key: str
    value: str

# Pydantic model for Sample
class Sample(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []
    patient: Patient

# Pydantic model for SampleWithoutPatient
class SampleWithoutPatient(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []

class RawFileResponse(BaseModel):
    id: int
    path: str
    sample_id: Optional[str] = None
    ext_sample_id: Optional[str] = None
    sample_metadata: Optional[List[SampleMetadata]] = None

# Pydantic model for Patient with Samples
class PatientWithSamples(PatientWithMetadata):
    samples: List[SampleWithoutPatient] = []

# Pydantic model for RawFileMetadata
class RawFileMetadataCreate(BaseModel):
    metadata_key: str
    metadata_value: str

# Updated Pydantic model for RawFile with nested metadata
class RawFileCreate(BaseModel):
    dataset_id: int
    path: str
    metadata: Optional[List[RawFileMetadataCreate]] = []



@app.post("/add_raw_files/")
async def add_raw_files(raw_files: List[RawFileCreate]):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)

        # Insert raw_files and fetch their IDs
        raw_file_ids = []
        for raw_file in raw_files:
            cursor.execute('''
                INSERT INTO files (dataset_id, path, file_type)
                VALUES (%s, %s, 'raw')
                RETURNING id
            ''', (raw_file.dataset_id, raw_file.path))
            raw_file_id = cursor.fetchone()['id']
            raw_file_ids.append(raw_file_id)

            # Insert associated metadata for this raw_file
            if raw_file.metadata:
                for metadata in raw_file.metadata:
                    cursor.execute('''
                        INSERT INTO files_metadata (raw_file_id, metadata_key, metadata_value)
                        VALUES (%s, %s, %s)
                    ''', (raw_file_id, metadata.metadata_key, metadata.metadata_value))

        conn.commit()
        conn.close()
        return {"status": "success", "message": "Raw files and metadata added successfully"}

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all patients and their metadata for a project_id
@app.get("/patients_metadata/{patient_id}", response_model=List[PatientWithSamples])
async def get_patients_metadata(project_id: int, patient_id: int):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)

        if patient_id != 0:
            cursor.execute('''
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = %s AND p.id = %s
                ORDER BY p.id
            ''', (project_id, patient_id,))
        else:
            cursor.execute('''
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = %s
                ORDER BY p.id
            ''', (project_id,))

        rows = cursor.fetchall()

        patients = []
        current_patient = None
        for row in rows:
            if not current_patient or current_patient['id'] != row['id']:
                if current_patient:
                    patients.append(current_patient)

                current_patient = {
                    'id': row['id'],
                    'project_id': row['project_id'],
                    'ext_patient_id': row['ext_patient_id'],
                    'ext_patient_url': row['ext_patient_url'],
                    'public_patient_id': row['public_patient_id'],
                    'samples': [],
                    'metadata': [] 
                }

            if row['pm.id']:
                current_patient['metadata'].append({
                    'id': row['pm.id'],
                    'patient_id': row['id'],
                    'key': row['pm.key'],
                    'value': row['pm.value']
                })

        if current_patient:
            patients.append(current_patient)

        for patient in patients:
            cursor.execute('''
                SELECT s.id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id, sm.key, sm.value
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                WHERE s.patient_id = %s
                ORDER BY s.id
            ''', (patient['id'],))

            sample_rows = cursor.fetchall()
            current_sample = None
            for sample_row in sample_rows:
                if not current_sample or current_sample['id'] != sample_row['id']:
                    if current_sample:
                        patient['samples'].append(current_sample)
                    current_sample = {
                        'id': sample_row['id'],
                        'patient_id': sample_row['patient_id'],
                        'ext_sample_id': sample_row['ext_sample_id'],
                        'ext_sample_url': sample_row['ext_sample_url'],
                        'metadata': []
                    }
                if sample_row['sm.id']:
                    current_sample['metadata'].append({
                        'id': sample_row['sm.id'],
                        'sample_id': sample_row['id'],
                        'key': sample_row['sm.key'],
                        'value': sample_row['sm.value']
                    })
            if current_sample:
                patient['samples'].append(current_sample)

        conn.close()

        return patients

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all samples and metadata for a project_id and include patient information
@app.get("/samples/{sample_id}", response_model=List[Sample])
async def get_samples_per_patient(sample_id: int, project_id: int):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)

        if sample_id != 0:
            cursor.execute('''
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = %s AND s.id = %s
                ORDER BY s.id, sm.id
            ''', (project_id, sample_id,))
        else:
            cursor.execute('''
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = %s
                ORDER BY s.id, sm.id
            ''', (project_id,))

        rows = cursor.fetchall()
        conn.close()

        samples = []
        current_sample = None
        for row in rows:
            if not current_sample or current_sample['id'] != row['sample_id']:
                if current_sample:
                    samples.append(current_sample)
                current_sample = {
                    'id': row['sample_id'],
                    'patient_id': row['patient_id'],
                    'ext_sample_id': row['ext_sample_id'],
                    'ext_sample_url': row['ext_sample_url'],
                    'metadata': [],
                    'patient': {
                        'id': row['patient_id'],
                        'project_id': row['project_id'],
                        'ext_patient_id': row['ext_patient_id'],
                        'ext_patient_url': row['ext_patient_url'],
                        'public_patient_id': row['public_patient_id']
                    }
                }

            if row['metadata_id']:  # Check if metadata exists
                current_sample['metadata'].append({
                    'id': row['metadata_id'],
                    'sample_id': row['sample_id'],
                    'key': row['sm.key'],
                    'value': row['sm.value']
                })

        if current_sample:
            samples.append(current_sample)

        return samples

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all patients with sample counts
@app.get("/patients/{patient_id}", response_model=List[PatientWithSampleCount])
async def get_patients(project_id: int, patient_id: int):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)

        # Query to fetch all patients with sample counts
        cursor.execute('''
            SELECT patients.id, patients.project_id, patients.ext_patient_id, patients.ext_patient_url,
                   patients.public_patient_id, COUNT(samples.id) AS sample_count
            FROM patients
            LEFT JOIN samples ON patients.id = samples.patient_id
            WHERE patients.project_id = %s
            GROUP BY patients.id
            ORDER BY patients.id
        ''', (project_id,))

        rows = cursor.fetchall()
        conn.close()
        
        patients = []
        for row in rows:
            patients.append({
                'id': row['id'],
                'project_id': row['project_id'],
                'ext_patient_id': row['ext_patient_id'],
                'ext_patient_url': row['ext_patient_url'],
                'public_patient_id': row['public_patient_id'],
                'sample_count': row['sample_count']
            })
        
        return patients
    
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all projects and their statuses
@app.get("/projects/", response_model=List[Project])
async def get_projects():
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)
        cursor.execute("SELECT id, name, status FROM projects")
        rows = cursor.fetchall()
        conn.close()
        return [Project(id=row['id'], name=row['name'], status=row['status']) for row in rows]
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Route to fetch all datasets
@app.get("/datasets/{dataset_id}", response_model=List[Dataset])
async def get_datasets(dataset_id: int, project_id: int):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)
        if dataset_id != 0:
            cursor.execute('SELECT id, project_id, name FROM datasets WHERE project_id = %s AND id = %s', (project_id, dataset_id,))
        else:
            cursor.execute('SELECT id, project_id, name FROM datasets WHERE project_id = %s', (project_id,))

        rows = cursor.fetchall()
        conn.close()
        return [Dataset(id=row['id'], project_id=row['project_id'], name=row['name']) for row in rows]
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


# Endpoint to fetch dataset details and metadata by dataset_id
@app.get("/datasets_with_metadata/{dataset_id}", response_model=DatasetWithMetadata)
async def get_dataset_with_metadata(dataset_id: int, project_id: int):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)
        
        # Fetch dataset details
        cursor.execute('''
            SELECT id, project_id, name
            FROM datasets
            WHERE id = %s AND project_id = %s
        ''', (dataset_id, project_id))
        dataset_row = cursor.fetchone()
        
        if not dataset_row:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Fetch dataset metadata
        cursor.execute('''
            SELECT id, dataset_id, key, value
            FROM datasets_metadata
            WHERE dataset_id = %s
        ''', (dataset_id,))
        metadata_rows = cursor.fetchall()
        
        conn.close()
        
        dataset = {
            "id": dataset_row['id'],
            "project_id": dataset_row['project_id'],
            "name": dataset_row['name'],
            "metadata": [{"id": row['id'], "dataset_id": row['dataset_id'], "key": row['key'], "value": row['value']} for row in metadata_rows]
        }

        return dataset

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.get("/raw_files_with_metadata/{dataset_id}", response_model=List[RawFileResponse])
async def get_raw_files_with_metadata(dataset_id: int):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)
        
        # Query to get raw files and their associated metadata
        query = """
        SELECT rf.id, rf.path, rfm.metadata_value AS sample_id, s.ext_sample_id
        FROM files rf
        LEFT JOIN files_metadata rfm ON rf.id = rfm.raw_file_id
        LEFT JOIN samples s ON rfm.metadata_value::integer = s.id
        WHERE rf.dataset_id = %s AND rfm.metadata_key = 'sample_id'
        """
        cursor.execute(query, (dataset_id,))
        raw_files = cursor.fetchall()

        response = []
        
        for raw_file in raw_files:
            raw_file_id, path, sample_id, ext_sample_id = raw_file['id'], raw_file['path'], raw_file['sample_id'], raw_file['ext_sample_id']
            
            # Fetch sample metadata
            if sample_id:
                cursor.execute("SELECT id, sample_id, key, value FROM samples_metadata WHERE sample_id = %s", (int(sample_id),))
                sample_metadata_rows = cursor.fetchall()

                sample_metadata_list = []
                for row in sample_metadata_rows:
                    sample_metadata_list.append({
                        'id': row['id'],
                        'sample_id': row['sample_id'],
                        'key': row['key'],
                        'value': row['value']
                    }) 
                print(sample_metadata_list)

                response.append(RawFileResponse(
                    id=raw_file_id,
                    path=path,
                    sample_id=sample_id,
                    ext_sample_id=ext_sample_id,
                    sample_metadata=sample_metadata_list
                ))

        conn.close()
        return response
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

class MetadataUpdate(BaseModel):
    dataset_id: int
    raw_file_size: str
    last_size_update: str

@app.put("/datasets_metadata/size_update", response_model=MetadataUpdate)
def update_metadata(update: MetadataUpdate):
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)
        
        # Update record with key 'raw_file_extension_size_of_all_files' for the given dataset_id
        if update.raw_file_size:
            cursor.execute(
                "SELECT id, value FROM datasets_metadata WHERE key = 'raw_file_extension_size_of_all_files' AND dataset_id = %s",
                (update.dataset_id,)
            )
            record = cursor.fetchone()
            if record:
                record_id, value_str = record['id'], record['value']
                # Update the metadata with the new value
                cursor.execute(
                    "UPDATE datasets_metadata SET value = %s WHERE id = %s",
                    (update.raw_file_size, record_id)
                )
            else:
                # Insert a new record if it doesn't exist
                cursor.execute(
                    "INSERT INTO datasets_metadata (dataset_id, key, value) VALUES (%s, 'raw_file_extension_size_of_all_files', %s)",
                    (update.dataset_id, update.raw_file_size))
            
        
        # Update record with key 'last_size_update' for the given dataset_id
        if update.last_size_update:
            cursor.execute(
                "SELECT id, value FROM datasets_metadata WHERE key = 'last_size_update' AND dataset_id = %s",
                (update.dataset_id,)
            )
            record = cursor.fetchone()
            if record:
                record_id, value_str = record['id'], record['value']
                # Update the metadata with the new value
                cursor.execute(
                    "UPDATE datasets_metadata SET value = %s WHERE id = %s",
                    (update.last_size_update, record_id)
                )
            else:
                # Insert a new record if it doesn't exist
                cursor.execute(
                    "INSERT INTO datasets_metadata (dataset_id, key, value) VALUES (%s, 'last_size_update', %s)",
                    (update.dataset_id, update.last_size_update))
        
        conn.commit()
        conn.close()

        return update
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Run the app using Uvicorn server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8888)

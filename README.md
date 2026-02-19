# KU Student Data Pipeline

## Project Overview

This project loads student data from multiple sources (SQLite, pipe-delimited file, JSON) into a DuckDB database and generates a comprehensive enrollment report showing each student's term-by-term credit totals and primary department focus.

## Prerequisites

- Python 3.8 or higher
- pip package manager
- venv (Python virtual environment module, usually included with Python)

### Input Files

The project expects the following files in the `KU_Input/` directory:

- `student_info.sqlite3` - SQLite database with student and academic program tables
- `enrollments.dat` - Pipe-delimited file with course enrollment records
- `departments.json` - JSON file with department information

## Installation

### 1. Create and activate a virtual environment (recommended)

**On macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**On Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### 2. Install required dependencies

```bash
pip install -r requirements.txt
```

## Running the Project

Execute the main script from the project root directory:
```bash
python src/load_and_transform.py
```

This will:
1. Create a DuckDB database file (`ku.duckdb`) in the project root
2. Load all data from the three input files into DuckDB tables
3. **Validate data quality** with 11+ comprehensive checks
4. **Automatically clean** common data quality issues
5. **Apply database constraints** (primary keys, foreign keys)
6. Generate `output.csv` with the required student enrollment summary

## Output

### DuckDB Database (`ku.duckdb`)
Contains the following tables:
- `student` - Student demographic and admission information
- `acad_prog` - Academic program/major information
- `enrollments` - Course enrollment records
- `departments` - Department contact and location information

### CSV Report (`output.csv`)
One row per student per term with columns:
- `student_id` - Student identifier
- `last_name` - Student's last name
- `term` - Academic term code
- `total_credits` - Total credits enrolled for that term
- `focused_department_name` - Department where student took most credits (alphabetically first if tied)
- `focused_department_contact` - Contact person for the focused department

## Project Structure

```
KU_Project/
├── KU_Input/                    # Input data files
│   ├── student_info.sqlite3
│   ├── enrollments.dat
│   ├── departments.json
│   ├── data_info.txt
│   └── output_snippet.csv
├── src/
│       └── load_and_transform.py  # Main ETL script
├── docs/
│   └── TakeHomeProgrammingAssignment.pdf
├── requirements.txt             # Python dependencies
├── README.md                    # This file
└── ARCHITECTURE.md              # Detailed architecture documentation
```

## Data Transformations

For a detailed visual representation and technical explanation, see `ARCHITECTURE.md`.

### Step 1: Load SQLite Tables
- Attach SQLite database using DuckDB's native SQLite reader
- Extract `student` and `acad_prog` tables
- Normalize column names to uppercase in SQL
- Create corresponding tables in DuckDB

### Step 2: Load Enrollments
- Read pipe-delimited `enrollments.dat` file using DuckDB's CSV reader
- Clean data and convert types directly in SQL (strip whitespace, convert credit hours to integers)
- Create `enrollments` table in DuckDB

### Step 3: Load Departments
- Parse `departments.json` file using DuckDB's JSON reader
- Normalize column names and clean data in SQL
- Create `departments` table in DuckDB

### Step 4: Generate Report
The report generation uses a multi-step SQL transformation:

1. **Aggregate Total Credits**: Sum credit hours by student and term
2. **Calculate Department Credits**: Sum credit hours by student, term, and department
3. **Rank Departments**: For each student-term combination, rank departments by:
   - Primary: Total credits (descending)
   - Tiebreaker: Department name (ascending alphabetically)
4. **Join and Format**: Combine with student names and department contacts

## Design Decisions

### Database Schema
The DuckDB schema mirrors the source data structure to maintain flexibility for future reporting needs. All source tables are preserved with minimal transformation, allowing for:
- Historical analysis of program changes
- Cross-term enrollment patterns
- Department-level analytics
- Student cohort analysis

### Focused Department Logic
When a student has equal credits in multiple departments for a term, the department that comes first alphabetically is selected. This is implemented using SQL window functions with a compound ORDER BY clause.

### Data Quality Framework
The pipeline includes a comprehensive data quality validation system:

**11+ Validation Checks:**
- Duplicate detection (students, programs, departments, enrollments)
- Referential integrity (orphaned records)
- NULL value checks on required fields
- Range validation (credit hours 0-30)
- Empty string detection
- Business rule validation (students without enrollments)

**Automatic Cleaning:**
- Removes duplicate records (keeps first occurrence)
- Removes orphaned records (invalid foreign keys)
- Fixes invalid credit hours (caps to valid range)
- Removes NULL required fields
- Detailed audit logging of all cleaning actions

**Database Constraints:**
- Primary keys on all tables (including temporal key for `acad_prog`)
- Foreign keys enforcing referential integrity
- Prevents data quality issues at the database level

## Dependencies

See `requirements.txt` for specific versions:
- `duckdb` - In-process SQL database with native readers for SQLite, CSV, and JSON

**Built-in Python modules** (no installation needed):
- `logging` - Professional logging framework
- `time` - Performance metrics tracking
- Standard library modules (`os`, `sys`, `traceback`, `datetime`)

**Note:** This project uses DuckDB's native data readers instead of pandas for better performance and fewer dependencies.

## Additional Tools

### Verify Database Constraints
```bash
python verify_constraints.py
```
Validates that all primary and foreign key constraints are properly applied and checks for data integrity issues.

### Run Tests
```bash
python test_pipeline.py
```
Runs the test suite to verify pipeline functionality.

### View Database Schema
See `DATABASE_SCHEMA.md` for detailed documentation of:
- Entity Relationship Diagram (ERD)
- Table definitions with constraints
- Primary and foreign key relationships
- Temporal data modeling (SCD Type 2)

## Notes

- The script creates/overwrites `ku.duckdb` and `output.csv` on each run
- All paths are calculated relative to the script location for portability
- The DuckDB file can be queried directly using the DuckDB CLI or Python API for ad-hoc analysis
- Data quality validation runs automatically before constraints are applied

# KU Student Data Pipeline

## Project Overview

This project loads student data from multiple sources (SQLite, pipe-delimited file, JSON) into a DuckDB database and generates a comprehensive enrollment report showing each student's term-by-term credit totals and primary department focus.

**Two Implementations Available:**
- **Main Branch**: Python + DuckDB (uses pandas for data loading)
- **SQL-Only Branch** (`sql-only-alt`): Pure SQL (no Python dependencies)

## Prerequisites

### For Main Branch (Python + DuckDB)
- Python 3.8 or higher
- pip package manager
- venv (Python virtual environment module, usually included with Python)

### For SQL-Only Branch (Pure SQL)
- DuckDB CLI (no Python required)
- Install: `brew install duckdb` (macOS) or see [DuckDB installation](https://duckdb.org/docs/installation/)

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

### Main Branch (Python + DuckDB)

Execute the main script from the project root directory:
```bash
python src/load_and_transform.py
```

This will:
1. Create a DuckDB database file (`ku.duckdb`) in the project root
2. Load all data from the three input files into DuckDB tables
3. Generate `output.csv` with the required student enrollment summary

## Output

### DuckDB Database (`ku.duckdb`)
Contains the following tables:
- `student` - Student demographic and admission information
- `acad_prog` - Academic program/major information
- `enrollments` - Course enrollment records
- `departments` - Department contact and location information

### CSV Report (`output.csv`)
One row per student per term (2,986 data rows + 1 header = 2,987 total lines) with columns:
- `student_id` - Student identifier
- `last_name` - Student's last name
- `term` - Academic term code
- `total_credits` - Total credits enrolled for that term (integer)
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
- Connect to `student_info.sqlite3`
- Extract `student` and `acad_prog` tables
- Normalize column names to uppercase
- Create corresponding tables in DuckDB

### Step 2: Load Enrollments
- Read pipe-delimited `enrollments.dat` file
- Parse and clean data (strip whitespace, convert credit hours to integers)
- Create `enrollments` table in DuckDB

### Step 3: Load Departments
- Parse `departments.json` file
- Normalize column names and clean data
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

### Data Quality
- All string fields are trimmed of whitespace
- Credit hours are converted to integers with error handling
- Left joins preserve all student records even if department information is missing
- Column names are normalized to uppercase for consistency

## Dependencies

See `requirements.txt` for specific versions:
- `duckdb` - In-process SQL database
- `pandas` - Data manipulation and CSV I/O
- `sqlite3` - Built-in Python module for SQLite access
- `json` - Built-in Python module for JSON parsing

## Notes

- The script creates/overwrites `ku.duckdb` and `output.csv` on each run
- All paths are calculated relative to the script location for portability
- The DuckDB file can be queried directly using the DuckDB CLI or Python API for ad-hoc analysis

---

## Alternate SQL-Only Implementation

An alternate pure SQL implementation is available in the `sql-only-alt` branch. This approach uses only SQL for all data loading and transformations, with no Python or pandas dependencies.

### Why Created
- Demonstrates database-native data processing with explicit schema constraints
- Eliminates Python dependencies entirely
- Provides a portable, reproducible SQL-only pipeline
- Shows proper relational database design with PRIMARY KEY, FOREIGN KEY, and NOT NULL constraints
- Includes built-in validation checks for data quality assurance

### How to Run
```bash
# Switch to the SQL-only branch
git checkout sql-only-alt

# Ensure you have DuckDB CLI installed
duckdb --version

# Run the SQL pipeline
duckdb ku.duckdb < sql_pipeline/ku_load_and_transform.sql
```

### Key Differences
- **Data Loading**: Uses DuckDB's native `ATTACH`, `read_csv()`, and `read_json()` functions
- **Schema Design**: Explicit table definitions with PRIMARY KEY, FOREIGN KEY, and NOT NULL constraints
- **Transformations**: Pure SQL views and queries (no pandas operations)
- **Validation**: Built-in checks at each stage (source tables, views, final output)
- **Output**: Uses SQL `COPY TO` command instead of pandas `to_csv()`
- **Dependencies**: Only requires DuckDB CLI (no Python packages needed)

### Schema Constraints (SQL-Only Branch)
- **Primary Keys**: student.EMPLID, acad_prog.ID, departments.DEPT_CODE
- **Foreign Keys**: acad_prog.EMPLID → student.EMPLID, enrollments.EMPLID → student.EMPLID
- **NOT NULL**: Critical fields like names, department codes, and enrollment details

### Note
Both implementations produce identical output (2,986 data rows + 1 header = 2,987 total lines) and follow the same logical transformation flow.

# KU Student Data Pipeline

## Project Overview

This project loads student data from multiple sources (SQLite, pipe-delimited file, JSON) into a DuckDB database using pure SQL and generates a comprehensive enrollment report showing each student's term-by-term credit totals and primary department focus.


All data loading, transformations, validation, and export steps are implemented entirely in SQL.


## Prerequisites

### For SQL Branch (Pure SQL)
- DuckDB CLI installed locally
- Install: `brew install duckdb` (macOS) or see [DuckDB installation](https://duckdb.org/docs/installation/)

### Input Files

The project expects the following files in the `KU_Input/` directory:

- `student_info.sqlite3` - SQLite database with student and academic program tables
- `enrollments.dat` - Pipe-delimited file with course enrollment records
- `departments.json` - JSON file with department information

## Running the Project

Execute the SQL pipeline from the project root directory:
```bash
duckdb ku.duckdb < sql_pipeline/load_and_transform.sql
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

Also includes SQL views used for transformation:
- total_credits
- credits_by_dept
- ranked_depts

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
├── KU_Input/                        # Input data files
│   ├── student_info.sqlite3
│   ├── enrollments.dat
│   ├── departments.json
│   ├── data_info.txt
│   └── output_snippet.csv
├── sql_pipeline/
│   └── load_and_transform.sql       # Pure SQL ETL pipeline
├── docs/
│   └── TakeHomeProgrammingAssignment.pdf
├── README.md                        # This file
└── ARCHITECTURE.md               

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

Only requires:
- DuckDB CLI

## Notes

- The script creates/overwrites `ku.duckdb` and `output.csv` on each run
- All processing is performed inside DuckDB using SQL
- The DuckDB file can be queried directly using the DuckDB CLI for ad-hoc analysis

---

### Why Created
- Demonstrates database-native data processing with explicit schema constraints
- Eliminates Python dependencies entirely
- Provides a portable, reproducible SQL-only pipeline
- Shows proper relational database design with PRIMARY KEY, FOREIGN KEY, and NOT NULL constraints
- Includes built-in validation checks for data quality assurance


### Schema Constraints (SQL-Only Branch)
- **Primary Keys**: student.EMPLID, acad_prog.ID, departments.DEPT_CODE
- **Foreign Keys**: acad_prog.EMPLID → student.EMPLID, enrollments.EMPLID → student.EMPLID
- **NOT NULL**: Critical fields like names, department codes, and enrollment details



### Validation & Exit Strategy

The pipeline includes fail-fast validation checks to ensure data integrity at each stage.

Validation queries verify:
- Source tables are not empty after loading
- Aggregated views produce results
- Final output contains records before export

If any validation fails, the script intentionally triggers a runtime error and stops execution. This prevents exporting incomplete or invalid results.

This ensures:
- No silent failures
- No empty output files
- Deterministic and reliable execution
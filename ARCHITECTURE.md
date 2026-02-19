# Architecture and Data Transformation Documentation

## Project Approach

### Main Branch (Python + DuckDB)
This implementation uses Python for data loading and DuckDB for SQL transformations:
- Python with pandas loads data from multiple sources into DuckDB
- DuckDB performs all SQL transformations
- Python exports the final CSV using pandas

### SQL-Only Branch (Pure SQL)
An alternate implementation (`sql-only-alt` branch) uses only SQL:
- DuckDB native functions load data (ATTACH, read_csv, read_json)
- All transformations in pure SQL
- No Python or pandas dependencies required

**Technology Choice**: DuckDB was selected as the database because it is an in-process analytical database that requires no server setup, supports advanced SQL features (window functions), and is optimized for analytical queries.

---

## Data Flow Diagram

```
┌─────────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│ student_info.sqlite3│   │ enrollments.dat  │   │ departments.json │
│  - student table    │   │  (pipe-delimited)│   │  (flat JSON)     │
│  - acad_prog table  │   │                  │   │                  │
└──────────┬──────────┘   └────────┬─────────┘   └────────┬─────────┘
           │                       │                       │
           └───────────────────────┼───────────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │      DuckDB Database         │
                    │  (ku.duckdb)                 │
                    │                              │
                    │  Tables:                     │
                    │  - student                   │
                    │  - acad_prog                 │
                    │  - enrollments               │
                    │  - departments               │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │   SQL Transformations        │
                    │   (Views & Queries)          │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │       output.csv             │
                    │  (one row per student/term)  │
                    └──────────────────────────────┘
```

---

## Data Transformations

### Step 1: Load Data into DuckDB

#### Main Branch (Python + DuckDB)

**SQLite Tables (student_info.sqlite3)**
- Read `student` and `acad_prog` tables using Python's sqlite3 library
- Normalize column names to uppercase
- Load into DuckDB tables

**Enrollments File (enrollments.dat)**
- Parse pipe-delimited file using pandas
- Clean data: strip whitespace, convert credit hours to integers
- Load into DuckDB `enrollments` table

**Departments File (departments.json)**
- Parse JSON file into pandas DataFrame
- Normalize column names and clean data
- Load into DuckDB `departments` table

#### SQL-Only Branch (Pure SQL)

**SQLite Tables**
- Use `ATTACH 'student_info.sqlite3' AS sqlite_db` to access tables
- Create tables with explicit schema and constraints
- Insert data with transformations

**Enrollments File**
- Use `read_csv('enrollments.dat', delim='|')` to read pipe-delimited data
- Create table with schema and load data

**Departments File**
- Use `read_json('departments.json')` to read JSON data
- Create table with schema and load data

### Step 2: Calculate Total Credits per Student-Term

Create a view that sums credit hours for each student in each term:

```sql
CREATE VIEW total_credits AS
SELECT EMPLID AS student_id, 
       STRM AS term, 
       SUM(CREDIT_HOURS) AS total_credits
FROM enrollments
GROUP BY EMPLID, STRM;
```

### Step 3: Calculate Credits by Department

Create a view that breaks down credits by department for each student-term:

```sql
CREATE VIEW credits_by_dept AS
SELECT EMPLID AS student_id, 
       STRM AS term, 
       DEPARTMENT AS dept_code,
       SUM(CREDIT_HOURS) AS dept_credits
FROM enrollments
GROUP BY EMPLID, STRM, DEPARTMENT;
```

### Step 4: Identify Focused Department

Create a view that ranks departments for each student-term combination. The department with the most credits is ranked #1. If there's a tie, the department that comes first alphabetically is selected:

```sql
CREATE VIEW ranked_depts AS
SELECT 
  c.student_id,
  c.term,
  c.dept_code,
  COALESCE(d.DEPT_NAME, c.dept_code) AS dept_name,
  d.CONTACT_PERSON AS dept_contact,
  ROW_NUMBER() OVER (
    PARTITION BY c.student_id, c.term
    ORDER BY c.dept_credits DESC, 
             COALESCE(d.DEPT_NAME, c.dept_code) ASC
  ) AS rn
FROM credits_by_dept c
LEFT JOIN departments d ON c.dept_code = d.DEPT_CODE;
```

**Key Logic**: The `ROW_NUMBER()` window function assigns a rank to each department for a given student-term. The `ORDER BY` clause ensures:
1. Departments with more credits come first (`dept_credits DESC`)
2. In case of a tie, departments are sorted alphabetically (`dept_name ASC`)

### Step 5: Generate Final Output

Join all the data together to create the final report:

```sql
SELECT
  t.student_id,
  s.LAST_NAME AS last_name,
  t.term,
  t.total_credits,
  rd.dept_name AS focused_department_name,
  rd.dept_contact AS focused_department_contact
FROM total_credits t
LEFT JOIN ranked_depts rd 
  ON t.student_id = rd.student_id 
  AND t.term = rd.term 
  AND rd.rn = 1
LEFT JOIN student s 
  ON t.student_id = s.EMPLID
ORDER BY t.student_id, t.term;
```

This produces one row per student per term with:
- Student ID and last name
- Term code
- Total credits for that term
- The focused department (where they took the most credits)
- Contact person for that department

---

## Design Decisions

### Database Schema

#### Main Branch
The DuckDB schema preserves the original source table structures with minimal transformation. This design:
- Maintains data lineage and traceability
- Enables future ad-hoc queries and reporting
- Supports historical analysis (e.g., tracking program changes over time)

#### SQL-Only Branch
The DuckDB schema includes explicit relational constraints to ensure data integrity:

**Primary Keys:**
- `student.EMPLID` - Uniquely identifies each student
- `acad_prog.ID` - Uniquely identifies each program record
- `departments.DEPT_CODE` - Uniquely identifies each department

**Foreign Keys:**
- `acad_prog.EMPLID` → `student.EMPLID` - Ensures every program belongs to a valid student
- `enrollments.EMPLID` → `student.EMPLID` - Ensures every enrollment belongs to a valid student

**NOT NULL Constraints:**
- Critical fields like student names, department names, and enrollment details are required
- Prevents incomplete or invalid data from entering the database

**Why Constraints Matter:**
- Enforce referential integrity at the database level
- Prevent orphaned records (e.g., enrollments without students)
- Document relationships between tables
- Enable database-level validation before data is inserted

### Transformation Approach

SQL views are used for transformations rather than Python code because:
- SQL is declarative and easier to understand
- Views can be tested independently
- DuckDB's query optimizer handles performance
- Window functions cleanly implement the tie-breaking logic

### Data Quality

- All string fields are trimmed of whitespace
- Credit hours are converted to integers with error handling
- LEFT JOINs preserve all student records even if department information is missing
- Column names are normalized to uppercase for consistency (except in SQL-only branch where case is preserved for readability)

### Focused Department Logic

When a student has equal credits in multiple departments for a term, the department that comes first alphabetically is selected. This is implemented using SQL's `ORDER BY` with multiple columns in the window function.

---

## Output Specification

The output CSV file (`output.csv`) contains:
- **Format**: Comma-separated values without quotes
- **Rows**: One row per student per term (2,986 data rows + 1 header = 2,987 total lines)
- **Columns**:
  - `student_id` – Student identifier
  - `last_name` – Student's last name
  - `term` – Term code
  - `total_credits` – Total credits enrolled for that term (integer)
  - `focused_department_name` – Department with most credits (alphabetically first if tied)
  - `focused_department_contact` – Contact person for the focused department

---

## SQL-Only Architecture Variant

An alternate implementation using pure SQL is available in the `sql-only-alt` branch. This approach demonstrates database-native data processing without Python dependencies.

### Architecture Overview
The SQL-only variant follows the same logical flow but implements everything using DuckDB SQL:

1. **Data Loading** (SQL-native):
   - SQLite tables: `ATTACH 'file.sqlite3' AS sqlite_db`
   - Pipe-delimited file: `read_csv('file.dat', delim='|')`
   - JSON file: `read_json('file.json')`

2. **Schema Definition** (Explicit constraints):
   - Tables created with `CREATE TABLE` statements
   - PRIMARY KEY, FOREIGN KEY, and NOT NULL constraints declared
   - Separate `INSERT INTO` statements for data loading

3. **Data Transformations** (SQL views):
   - Same view structure as Python implementation
   - Identical window function logic for focused department
   - All aggregations and joins in pure SQL

4. **Output Generation** (SQL command):
   - `COPY TO 'output.csv'` instead of pandas export
   - Produces identical output format

### Benefits
- **Portability**: Runs anywhere DuckDB CLI is available
- **Reproducibility**: No Python environment setup needed
- **Transparency**: All logic visible in single SQL file
- **Performance**: Database-native operations throughout
- **Data Integrity**: Explicit constraints enforce referential integrity
- **Validation**: Built-in checks ensure data quality at each stage

### Validation Checks
The SQL-only implementation includes lightweight validation at key stages:
- Verifies source tables are not empty after loading
- Confirms transformation views produce results
- Validates final output before export
- Fails fast with clear error messages if data is missing


# Architecture and Data Transformation Documentation

## Project Approach

This project implements an ETL (Extract, Transform, Load) pipeline that:
- Loads student data from three different sources (SQLite, pipe-delimited file, JSON)
- Stores all data in a DuckDB database for flexible querying
- Transforms the data using SQL to generate a term-by-term enrollment report
- Exports the results to a CSV file

**Technology Choice**: DuckDB was selected as the database because it is an in-process analytical database that requires no server setup, supports advanced SQL features (window functions), and is optimized for analytical queries.

**Programming Language**: Python was used with DuckDB's native readers for data loading. This approach eliminates the need for pandas, resulting in better performance, lower memory usage, and fewer dependencies. All data cleaning and transformations are performed in SQL.

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

**SQLite Tables (student_info.sqlite3)**
- Attach SQLite database using DuckDB's native `ATTACH` command
- Read `student` and `acad_prog` tables directly with SQL
- Normalize column names to uppercase using SQL aliases
- Load into DuckDB tables

**Enrollments File (enrollments.dat)**
- Parse pipe-delimited file using DuckDB's `read_csv()` function
- Clean data in SQL: `TRIM()` for whitespace, `TRY_CAST()` for type conversion
- Convert credit hours to integers with error handling
- Load into DuckDB `enrollments` table

**Departments File (departments.json)**
- Parse JSON file using DuckDB's `read_json()` function with auto-detection
- Normalize column names and clean data using SQL transformations
- Load into DuckDB `departments` table

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

**Database Schema**: The DuckDB schema preserves the original source table structures. This design:
- Maintains data lineage and traceability
- Enables future ad-hoc queries and reporting
- Supports historical analysis (e.g., tracking program changes over time)

**Transformation Approach**: SQL views are used for transformations rather than Python code because:
- SQL is declarative and easier to understand
- Views can be tested independently
- DuckDB's query optimizer handles performance
- Window functions cleanly implement the tie-breaking logic

**Data Quality**: 
- All string fields are trimmed of whitespace using SQL `TRIM()` function
- Credit hours are converted to integers with error handling using `TRY_CAST()` and `COALESCE()`
- LEFT JOINs preserve all student records even if department information is missing
- Column names are normalized to uppercase for consistency
- All data cleaning performed in SQL for better performance

**Performance Optimization**:
- Uses DuckDB's native readers (SQLite, CSV, JSON) instead of pandas
- Eliminates intermediate DataFrame conversions
- Reduces memory footprint by streaming data directly
- Leverages DuckDB's columnar processing and parallel execution

**Focused Department Logic**: When a student has equal credits in multiple departments for a term, the department that comes first alphabetically is selected. This is implemented using SQL's `ORDER BY` with multiple columns in the window function.

---

## Output Specification

The output CSV file (`output.csv`) contains:
- **Format**: Comma-separated values without quotes
- **Rows**: One row per student per term (2,986 data rows + 1 header)
- **Columns**:
  - `student_id` – Student identifier
  - `last_name` – Student's last name
  - `term` – Term code
  - `total_credits` – Total credits enrolled for that term (integer)
  - `focused_department_name` – Department with most credits (alphabetically first if tied)
  - `focused_department_contact` – Contact person for the focused department

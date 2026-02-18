# Architecture and Data Transformation Documentation

## Project Approach

This project implements an ETL (Extract, Transform, Load) pipeline that:
- Loads student data from three different sources (SQLite, pipe-delimited file, JSON)
- Stores all data in a DuckDB database for flexible querying
- Transforms the data using SQL to generate a term-by-term enrollment report

  <!-- For each student + term:
  * Calculate total credits
  * Find which department the student took the most credits in
  * If tie → choose alphabetically
  * Join department contact
  * Export to CSV
  That’s it. -->  

- Exports the results to a CSV file

<!-- Format should be simmilar to the given output_snippet.csv -->

**Technology Choice**: DuckDB was selected as the database because it is an in-process analytical database that requires no server setup, supports advanced SQL features (window functions), and is optimized for analytical queries.

**Programming Language**: Python was used with pandas for data loading and cleaning, and DuckDB for SQL transformations.

---

## Data Flow Diagram

<!-- the first data file contains student table and the academic progrm table, the second contains the enrollment data and the third contains the departmen.I load all three sources into a local DuckDB database to centralize the data. Once inside DuckDB, I run SQL transformations queries using views to compute total credits and determine each student’s focused department per term.

The final result is exported as a clean output.csv file with one row per student per term. -->
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
- Read `student` and `acad_prog` tables using Python's sqlite3 library
- Normalize column names to uppercase 
<!-- to avoid case sensiticity and mismatch joins -->
- Load into DuckDB tables

**Enrollments File (enrollments.dat)**
- Parse pipe-delimited file using pandas
- Clean data: strip whitespace, convert credit hours to integers
- Load into DuckDB `enrollments` table

**Departments File (departments.json)**
- Parse JSON file into pandas DataFrame
- Normalize column names and clean data
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
<!-- so we first created a view to store the sql query, creating a view make it easier for debugging and allow the final query to be much cleaner. we then renamed column name emplid to student_id as per the instructions. we did the same with strm as term. we then summed or added all the credit hours per grouping. then from the raw data table, we used group by to group rows together so we can aggregate them. this would produce one row per student -->
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
<!-- we then used another view for the credits by department Because now we need: Student + Term + Department level. we then used group by this time to change the grain to One row per student-term-department. and in the duckdb visualization it would show the headers like: student_id
term
dept_code
dept_credits -->
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
<!-- and then final view uses a window function so it does not collapse rows and can calucalte across partition while still keeping the original row structure. we use select to take all the columns from credits_by_dept and assign ranking first second third, we use PARTITION BY student_id, term  to reset ranking for each student-term. and following the instrcutions given we use ORDER BY dept_credits DESC to obtain Highest credits first. and
dept_code ASC in case  two departments have same credits,
 so we cna obrain Alphabetically to resolve any tie . now to debug, "WHERE rank = 2", we get the focused department. -->
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
- All string fields are trimmed of whitespace
- Credit hours are converted to integers with error handling
- LEFT JOINs preserve all student records even if department information is missing
- Column names are normalized to uppercase for consistency

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

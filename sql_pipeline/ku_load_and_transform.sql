-- ============================================================================
-- KU Student Data Pipeline - Pure SQL Implementation
-- ============================================================================
-- Purpose: Load student data from multiple sources into DuckDB and generate
--          enrollment report using only SQL transformations.
--
-- Usage: duckdb ku.duckdb < sql_pipeline/ku_load_and_transform.sql
-- ============================================================================

-- ============================================================================
-- STEP 1: Load Data from Source Files
-- ============================================================================

-- Load SQLite tables (student and acad_prog)
-- Attach the SQLite database to access its tables
ATTACH 'KU_Input/student_info.sqlite3' AS sqlite_db (TYPE SQLITE);

-- Create student table with proper schema and constraints
-- Primary Key: EMPLID uniquely identifies each student
-- NOT NULL constraints ensure data quality for critical fields
-- This table provides demographic and admission information
CREATE OR REPLACE TABLE student (
    EMPLID VARCHAR PRIMARY KEY,
    FIRST_NAME VARCHAR NOT NULL,
    LAST_NAME VARCHAR NOT NULL,
    NAME VARCHAR NOT NULL,
    EMAIL_ADDR VARCHAR,
    BIRTHDATE DATE,
    ADMIT_TERM VARCHAR,
    ADMIT_TYPE VARCHAR
);

-- Load data into student table with proper transformations
INSERT INTO student
SELECT 
    TRIM(EMPLID) AS EMPLID,
    UPPER(TRIM(FIRST_NAME)) AS FIRST_NAME,
    UPPER(TRIM(LAST_NAME)) AS LAST_NAME,
    UPPER(TRIM(NAME)) AS NAME,
    UPPER(TRIM(EMAIL_ADDR)) AS EMAIL_ADDR,
    TRIM(BIRTHDATE) AS BIRTHDATE,
    TRIM(ADMIT_TERM) AS ADMIT_TERM,
    UPPER(TRIM(ADMIT_TYPE)) AS ADMIT_TYPE
FROM sqlite_db.student;

-- Create acad_prog table with proper schema and constraints
-- Primary Key: ID uniquely identifies each program record
-- Foreign Key: EMPLID references student.EMPLID to maintain referential integrity
-- This ensures every program record belongs to a valid student
-- NOT NULL constraints ensure critical fields are always populated
-- This table tracks academic program enrollments and changes over time
CREATE OR REPLACE TABLE acad_prog (
    ID INTEGER PRIMARY KEY,
    EMPLID VARCHAR NOT NULL,
    ACAD_PROG VARCHAR NOT NULL,
    PROG_STATUS VARCHAR,
    PROG_ACTION VARCHAR,
    EFFDT DATE,
    DEGREE VARCHAR,
    FOREIGN KEY (EMPLID) REFERENCES student(EMPLID)
);

-- Load data into acad_prog table with proper transformations
INSERT INTO acad_prog
SELECT 
    ID,
    TRIM(EMPLID) AS EMPLID,
    UPPER(TRIM(ACAD_PROG)) AS ACAD_PROG,
    UPPER(TRIM(PROG_STATUS)) AS PROG_STATUS,
    UPPER(TRIM(PROG_ACTION)) AS PROG_ACTION,
    TRIM(EFFDT) AS EFFDT,
    UPPER(TRIM(DEGREE)) AS DEGREE
FROM sqlite_db.acad_prog;

-- Detach SQLite database after loading
DETACH sqlite_db;

-- Load enrollments from pipe-delimited file
-- Primary Key: Composite key would be (EMPLID, STRM, COURSE_ID)
-- Foreign Key: EMPLID references student.EMPLID
-- NOT NULL constraints ensure data integrity for critical enrollment fields
-- This table links students to courses with credit hours
-- CREDIT_HOURS is converted to INTEGER to match output requirements
CREATE OR REPLACE TABLE enrollments (
    EMPLID VARCHAR NOT NULL,
    STRM VARCHAR NOT NULL,
    COURSE_ID VARCHAR NOT NULL,
    DEPARTMENT VARCHAR NOT NULL,
    COURSE_NAME VARCHAR,
    CREDIT_HOURS INTEGER NOT NULL,
    FOREIGN KEY (EMPLID) REFERENCES student(EMPLID)
);

-- Load data into enrollments table
INSERT INTO enrollments
SELECT 
    TRIM(EMPLID) AS EMPLID,
    TRIM(STRM) AS STRM,
    UPPER(TRIM(COURSE_ID)) AS COURSE_ID,
    UPPER(TRIM(DEPARTMENT)) AS DEPARTMENT,
    TRIM(COURSE_NAME) AS COURSE_NAME,
    CAST(TRIM(CREDIT_HOURS) AS INTEGER) AS CREDIT_HOURS
FROM read_csv('KU_Input/enrollments.dat', 
              delim='|', 
              header=true,
              columns={
                  'EMPLID': 'VARCHAR',
                  'STRM': 'VARCHAR',
                  'COURSE_ID': 'VARCHAR',
                  'DEPARTMENT': 'VARCHAR',
                  'COURSE_NAME': 'VARCHAR',
                  'CREDIT_HOURS': 'VARCHAR'
              });

-- Load departments from JSON file
-- Primary Key: DEPT_CODE uniquely identifies each department
-- NOT NULL constraints ensure department code and name are always present
-- This table provides department contact information
CREATE OR REPLACE TABLE departments (
    DEPT_CODE VARCHAR PRIMARY KEY,
    DEPT_NAME VARCHAR NOT NULL,
    BUILDING VARCHAR,
    CONTACT_PERSON VARCHAR,
    PHONE VARCHAR,
    EMAIL VARCHAR
);

-- Load data into departments table
INSERT INTO departments
SELECT 
    UPPER(TRIM(DEPT_CODE)) AS DEPT_CODE,
    TRIM(DEPT_NAME) AS DEPT_NAME,
    TRIM(BUILDING) AS BUILDING,
    TRIM(CONTACT_PERSON) AS CONTACT_PERSON,
    TRIM(PHONE) AS PHONE,
    TRIM(EMAIL) AS EMAIL
FROM read_json('KU_Input/departments.json');

-- ============================================================================
-- STEP 2: Calculate Total Credits per Student-Term
-- ============================================================================
-- Aggregation grain: One row per student per term
-- This view sums all credit hours for each student in each term
CREATE OR REPLACE VIEW total_credits AS
SELECT 
    EMPLID AS student_id,
    STRM AS term,
    SUM(CREDIT_HOURS) AS total_credits
FROM enrollments
GROUP BY EMPLID, STRM;

-- ============================================================================
-- STEP 3: Calculate Credits by Department
-- ============================================================================
-- Aggregation grain: One row per student per term per department
-- This view breaks down credits by department for focused department logic
CREATE OR REPLACE VIEW credits_by_dept AS
SELECT 
    EMPLID AS student_id,
    STRM AS term,
    DEPARTMENT AS dept_code,
    SUM(CREDIT_HOURS) AS dept_credits
FROM enrollments
GROUP BY EMPLID, STRM, DEPARTMENT;

-- ============================================================================
-- STEP 4: Rank Departments (Focused Department Logic)
-- ============================================================================
-- Window function explanation:
-- - ROW_NUMBER() assigns a unique rank to each row within a partition
-- - PARTITION BY creates separate ranking groups for each student-term
-- - ORDER BY defines ranking priority:
--   1. dept_credits DESC: Department with most credits ranks first
--   2. dept_name ASC: Alphabetical tiebreaker when credits are equal
-- - Window functions do NOT collapse rows like GROUP BY
-- - Each row retains its identity while gaining a rank column
CREATE OR REPLACE VIEW ranked_depts AS
SELECT 
    c.student_id,
    c.term,
    c.dept_code,
    COALESCE(d.DEPT_NAME, c.dept_code) AS dept_name,
    d.CONTACT_PERSON AS dept_contact,
    ROW_NUMBER() OVER (
        PARTITION BY c.student_id, c.term
        ORDER BY c.dept_credits DESC, COALESCE(d.DEPT_NAME, c.dept_code) ASC
    ) AS rn
FROM credits_by_dept c
LEFT JOIN departments d ON c.dept_code = d.DEPT_CODE;

-- ============================================================================
-- STEP 5: Generate Final Output
-- ============================================================================
-- Export to CSV with exact column specification
-- Only include the focused department (rn = 1) for each student-term
COPY (
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
    ORDER BY t.student_id, t.term
) TO 'output.csv' (HEADER, DELIMITER ',');

-- KU Student Data Pipeline - Pure SQL Implementation
-- Load student data from multiple sources into DuckDB and generate enrollment report
-- Usage: duckdb ku.duckdb < sql_pipeline/ku_load_and_transform.sql

-- Load SQLite tables
ATTACH 'KU_Input/student_info.sqlite3' AS sqlite_db (TYPE SQLITE);

-- Create student table with constraints
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

-- Create acad_prog table with foreign key to student
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

DETACH sqlite_db;

-- Load enrollments from pipe-delimited file
CREATE OR REPLACE TABLE enrollments (
    EMPLID VARCHAR NOT NULL,
    STRM VARCHAR NOT NULL,
    COURSE_ID VARCHAR NOT NULL,
    DEPARTMENT VARCHAR NOT NULL,
    COURSE_NAME VARCHAR,
    CREDIT_HOURS INTEGER NOT NULL,
    FOREIGN KEY (EMPLID) REFERENCES student(EMPLID)
);

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
CREATE OR REPLACE TABLE departments (
    DEPT_CODE VARCHAR PRIMARY KEY,
    DEPT_NAME VARCHAR NOT NULL,
    BUILDING VARCHAR,
    CONTACT_PERSON VARCHAR,
    PHONE VARCHAR,
    EMAIL VARCHAR
);

INSERT INTO departments
SELECT 
    UPPER(TRIM(DEPT_CODE)) AS DEPT_CODE,
    TRIM(DEPT_NAME) AS DEPT_NAME,
    TRIM(BUILDING) AS BUILDING,
    TRIM(CONTACT_PERSON) AS CONTACT_PERSON,
    TRIM(PHONE) AS PHONE,
    TRIM(EMAIL) AS EMAIL
FROM read_json('KU_Input/departments.json');

-- total credits per student-term
CREATE OR REPLACE VIEW total_credits AS
SELECT 
    EMPLID AS student_id,
    STRM AS term,
    SUM(CREDIT_HOURS) AS total_credits
FROM enrollments
GROUP BY EMPLID, STRM;

-- credits per student-term-department
CREATE OR REPLACE VIEW credits_by_dept AS
SELECT 
    EMPLID AS student_id,
    STRM AS term,
    DEPARTMENT AS dept_code,
    SUM(CREDIT_HOURS) AS dept_credits
FROM enrollments
GROUP BY EMPLID, STRM, DEPARTMENT;

-- rank departments per student-term (tie-break: dept name alphabetical)
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

-- final select returns one row per student per term with focused dept (rn=1)
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

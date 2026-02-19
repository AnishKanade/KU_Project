import os
import duckdb
import sys
import traceback
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(ROOT, "KU_Input")
SQLITE_PATH = os.path.join(INPUT_DIR, "student_info.sqlite3")
ENROLLMENTS_PATH = os.path.join(INPUT_DIR, "enrollments.dat")
DEPARTMENTS_PATH = os.path.join(INPUT_DIR, "departments.json")

OUT_DUCKDB = os.path.join(ROOT, "ku.duckdb")
OUT_CSV = os.path.join(ROOT, "output.csv")

# Performance metrics
class PipelineMetrics:
    def __init__(self):
        self.start_time = None
        self.step_times = {}
        self.row_counts = {}
    
    def start(self):
        self.start_time = time.time()
    
    def record_step(self, step_name, duration, row_count=None):
        self.step_times[step_name] = duration
        if row_count is not None:
            self.row_counts[step_name] = row_count
    
    def get_total_time(self):
        return time.time() - self.start_time if self.start_time else 0
    
    def print_summary(self):
        total_time = self.get_total_time()
        logger.info("\n" + "="*60)
        logger.info("PIPELINE PERFORMANCE SUMMARY")
        logger.info("="*60)
        logger.info(f"Total execution time: {total_time:.2f}s")
        logger.info("\nStep-by-step breakdown:")
        for step, duration in self.step_times.items():
            pct = (duration / total_time * 100) if total_time > 0 else 0
            row_info = f" ({self.row_counts[step]:,} rows)" if step in self.row_counts else ""
            logger.info(f"  {step}: {duration:.2f}s ({pct:.1f}%){row_info}")
        
        if self.row_counts:
            logger.info("\nData volume:")
            total_rows = sum(self.row_counts.values())
            logger.info(f"  Total rows processed: {total_rows:,}")
        logger.info("="*60 + "\n")

metrics = PipelineMetrics()

# helper loaders
def load_sqlite_tables_to_duckdb(con_duck, sqlite_path):
    """
    Load tables from SQLite database using DuckDB's native SQLite reader.
    This is faster and more memory-efficient than using pandas.
    """
    # Use DuckDB's sqlite_scan extension to read SQLite database
    # First, get list of tables by scanning the SQLite file
    tables_query = f"""
        SELECT DISTINCT name 
        FROM sqlite_scan('{sqlite_path}', 'sqlite_master')
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
    """
    tables = con_duck.execute(tables_query).fetchall()
    
    for (table_name,) in tables:
        # Get columns by describing the table
        temp_table = con_duck.execute(f"""
            SELECT * FROM sqlite_scan('{sqlite_path}', '{table_name}') LIMIT 0
        """).description
        
        # Build SELECT with uppercase column aliases
        col_mappings = [f'"{col[0]}" AS {col[0].upper()}' for col in temp_table]
        col_list = ", ".join(col_mappings)
        
        # Create DuckDB table with uppercase columns
        con_duck.execute(f"""
            CREATE OR REPLACE TABLE {table_name.lower()} AS 
            SELECT {col_list} FROM sqlite_scan('{sqlite_path}', '{table_name}')
        """)

def load_enrollments_to_duckdb(con_duck, enrollments_path):
    """
    Load enrollments from pipe-delimited file using DuckDB's native CSV reader.
    Performs data cleaning and type conversion in SQL.
    """
    # First, read the CSV to get column names
    temp_data = con_duck.execute(f"""
        SELECT * FROM read_csv('{enrollments_path}', 
            delim='|',
            header=true,
            all_varchar=true
        ) LIMIT 1
    """).fetchall()
    
    # Get column names and create uppercase mappings
    columns = con_duck.execute(f"""
        DESCRIBE SELECT * FROM read_csv('{enrollments_path}', 
            delim='|',
            header=true,
            all_varchar=true
        )
    """).fetchall()
    
    # Build SELECT with uppercase columns and data cleaning
    col_mappings = []
    for col in columns:
        col_name = col[0]
        upper_name = col_name.upper()
        
        if upper_name == 'CREDIT_HOURS':
            # Convert to integer, handling errors
            col_mappings.append(f"CAST(COALESCE(TRY_CAST(TRIM({col_name}) AS INTEGER), 0) AS INTEGER) AS {upper_name}")
        else:
            # Trim whitespace for string columns
            col_mappings.append(f"TRIM({col_name}) AS {upper_name}")
    
    col_list = ",\n            ".join(col_mappings)
    
    # Create table with cleaned data
    con_duck.execute(f"""
        CREATE OR REPLACE TABLE enrollments AS
        SELECT 
            {col_list}
        FROM read_csv('{enrollments_path}', 
            delim='|',
            header=true,
            all_varchar=true
        )
    """)

def load_departments_to_duckdb(con_duck, departments_path):
    """
    Load departments from JSON file using DuckDB's native JSON reader.
    Performs column name normalization and data cleaning in SQL.
    """
    # Get column names from JSON
    columns = con_duck.execute(f"""
        DESCRIBE SELECT * FROM read_json('{departments_path}', auto_detect=true)
    """).fetchall()
    
    # Build SELECT with uppercase columns and trimmed values
    col_mappings = [f"TRIM({col[0]}) AS {col[0].upper()}" for col in columns]
    col_list = ",\n            ".join(col_mappings)
    
    # Create table with cleaned data
    con_duck.execute(f"""
        CREATE OR REPLACE TABLE departments AS
        SELECT 
            {col_list}
        FROM read_json('{departments_path}', auto_detect=true)
    """)

def validate_data_quality(con_duck):
    """
    Validate data quality BEFORE applying constraints.
    Returns (is_valid, issues_dict) where issues_dict contains any problems found.
    """
    logger.info("Validating data quality...")
    issues = {}
    
    # 1. Check for duplicate students
    dup_students = con_duck.execute("""
        SELECT EMPLID, COUNT(*) as cnt 
        FROM student 
        GROUP BY EMPLID 
        HAVING COUNT(*) > 1
    """).fetchall()
    if dup_students:
        issues['duplicate_students'] = {
            'count': len(dup_students),
            'examples': dup_students[:5]
        }
    
    # 2. Check for duplicate academic programs
    # Note: acad_prog can have multiple records per (EMPLID, ACAD_PROG) with different effective dates
    # Only flag as duplicate if ALL fields are identical (true duplicates)
    acad_cols = con_duck.execute("DESCRIBE acad_prog").fetchall()
    col_names = [col[0] for col in acad_cols]
    group_by_clause = ", ".join(col_names)
    
    dup_acad = con_duck.execute(f"""
        SELECT {group_by_clause}, COUNT(*) as cnt 
        FROM acad_prog 
        GROUP BY {group_by_clause}
        HAVING COUNT(*) > 1
    """).fetchall()
    if dup_acad:
        issues['duplicate_acad_prog'] = {
            'count': len(dup_acad),
            'examples': dup_acad[:5]
        }
    
    # 3. Check for duplicate departments
    dup_depts = con_duck.execute("""
        SELECT DEPT_CODE, COUNT(*) as cnt 
        FROM departments 
        GROUP BY DEPT_CODE 
        HAVING COUNT(*) > 1
    """).fetchall()
    if dup_depts:
        issues['duplicate_departments'] = {
            'count': len(dup_depts),
            'examples': dup_depts[:5]
        }
    
    # 4. Check for duplicate enrollments
    columns = [row[0] for row in con_duck.execute("DESCRIBE enrollments").fetchall()]
    pk_columns = ["EMPLID", "STRM"]
    if "COURSE_ID" in columns:
        pk_columns.append("COURSE_ID")
    if "CLASS_NBR" in columns:
        pk_columns.append("CLASS_NBR")
    
    pk_clause = ", ".join(pk_columns)
    dup_enrollments = con_duck.execute(f"""
        SELECT {pk_clause}, COUNT(*) as cnt 
        FROM enrollments 
        GROUP BY {pk_clause}
        HAVING COUNT(*) > 1
    """).fetchall()
    if dup_enrollments:
        issues['duplicate_enrollments'] = {
            'count': len(dup_enrollments),
            'examples': dup_enrollments[:5]
        }
    
    # 5. Check for orphaned academic programs (student doesn't exist)
    orphaned_acad = con_duck.execute("""
        SELECT a.EMPLID, COUNT(*) as cnt
        FROM acad_prog a
        LEFT JOIN student s ON a.EMPLID = s.EMPLID
        WHERE s.EMPLID IS NULL
        GROUP BY a.EMPLID
    """).fetchall()
    if orphaned_acad:
        issues['orphaned_acad_prog'] = {
            'count': sum(row[1] for row in orphaned_acad),
            'student_ids': [row[0] for row in orphaned_acad[:5]]
        }
    
    # 6. Check for orphaned enrollments (student doesn't exist)
    orphaned_enr_student = con_duck.execute("""
        SELECT e.EMPLID, COUNT(*) as cnt
        FROM enrollments e
        LEFT JOIN student s ON e.EMPLID = s.EMPLID
        WHERE s.EMPLID IS NULL
        GROUP BY e.EMPLID
    """).fetchall()
    if orphaned_enr_student:
        issues['orphaned_enrollments_student'] = {
            'count': sum(row[1] for row in orphaned_enr_student),
            'student_ids': [row[0] for row in orphaned_enr_student[:5]]
        }
    
    # 7. Check for enrollments with invalid departments
    orphaned_enr_dept = con_duck.execute("""
        SELECT e.DEPARTMENT, COUNT(*) as cnt
        FROM enrollments e
        LEFT JOIN departments d ON e.DEPARTMENT = d.DEPT_CODE
        WHERE d.DEPT_CODE IS NULL
        GROUP BY e.DEPARTMENT
    """).fetchall()
    if orphaned_enr_dept:
        issues['orphaned_enrollments_dept'] = {
            'count': sum(row[1] for row in orphaned_enr_dept),
            'departments': [row[0] for row in orphaned_enr_dept[:5]]
        }
    
    # 8. Check for NULL values in required fields
    null_checks = [
        ('student', 'EMPLID', 'Student ID cannot be NULL'),
        ('student', 'LAST_NAME', 'Student last name cannot be NULL'),
        ('enrollments', 'EMPLID', 'Enrollment student ID cannot be NULL'),
        ('enrollments', 'STRM', 'Enrollment term cannot be NULL'),
        ('enrollments', 'CREDIT_HOURS', 'Credit hours cannot be NULL'),
        ('departments', 'DEPT_CODE', 'Department code cannot be NULL'),
    ]
    
    for table, column, message in null_checks:
        try:
            null_count = con_duck.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
            """).fetchone()[0]
            
            if null_count > 0:
                issue_key = f'null_{table}_{column}'.lower()
                issues[issue_key] = {
                    'count': null_count,
                    'message': message,
                    'table': table,
                    'column': column
                }
        except:
            # Column might not exist, skip
            pass
    
    # 9. Check for invalid credit hours (negative or unreasonably high)
    try:
        invalid_credits = con_duck.execute("""
            SELECT EMPLID, STRM, CREDIT_HOURS, COUNT(*) as cnt
            FROM enrollments
            WHERE CREDIT_HOURS < 0 OR CREDIT_HOURS > 30
            GROUP BY EMPLID, STRM, CREDIT_HOURS
        """).fetchall()
        
        if invalid_credits:
            issues['invalid_credit_hours'] = {
                'count': len(invalid_credits),
                'examples': invalid_credits[:5],
                'message': 'Credit hours should be between 0 and 30'
            }
    except:
        pass
    
    # 10. Check for empty/whitespace-only strings in key fields
    try:
        empty_names = con_duck.execute("""
            SELECT EMPLID, LAST_NAME
            FROM student
            WHERE TRIM(LAST_NAME) = '' OR LAST_NAME IS NULL
            LIMIT 5
        """).fetchall()
        
        if empty_names:
            issues['empty_student_names'] = {
                'count': len(empty_names),
                'examples': empty_names,
                'message': 'Student last name cannot be empty'
            }
    except:
        pass
    
    # 11. Check for students with no enrollments (potential data issue)
    try:
        students_no_enrollments = con_duck.execute("""
            SELECT s.EMPLID, s.LAST_NAME
            FROM student s
            LEFT JOIN enrollments e ON s.EMPLID = e.EMPLID
            WHERE e.EMPLID IS NULL
            LIMIT 5
        """).fetchall()
        
        if students_no_enrollments:
            total_count = con_duck.execute("""
                SELECT COUNT(*)
                FROM student s
                LEFT JOIN enrollments e ON s.EMPLID = e.EMPLID
                WHERE e.EMPLID IS NULL
            """).fetchone()[0]
            
            issues['students_no_enrollments'] = {
                'count': total_count,
                'examples': students_no_enrollments,
                'message': 'Students with no enrollment records (may be valid for new admits)',
                'severity': 'warning'  # This might be expected
            }
    except:
        pass
    
    # Report results
    if not issues:
        logger.info("  ✓ All data quality checks passed")
        return True, issues
    else:
        logger.warning("  ⚠ Data quality issues found:")
        logger.warning("")
        for issue_type, details in issues.items():
            logger.warning(f"  {issue_type.upper().replace('_', ' ')}:")
            logger.warning(f"    Total: {details.get('count', 'N/A')} issues")
            
            # Print severity if specified
            severity = details.get('severity', 'error')
            severity_icon = '⚠' if severity == 'warning' else '❌'
            log_func = logger.warning if severity == 'warning' else logger.error
            
            # Print message if available
            if 'message' in details:
                log_func(f"    {severity_icon} {details['message']}")
            
            # Print detailed examples
            if 'examples' in details:
                log_func("    Examples (showing first 5):")
                for example in details['examples']:
                    log_func(f"      {example}")
            elif 'student_ids' in details:
                log_func("    Student IDs with issues (showing first 5):")
                for sid in details['student_ids']:
                    log_func(f"      EMPLID: {sid}")
            elif 'departments' in details:
                log_func("    Department codes with issues (showing first 5):")
                for dept in details['departments']:
                    log_func(f"      DEPT_CODE: {dept}")
            log_func("")
        return False, issues

def print_detailed_issue_records(con_duck, issues):
    """
    Print detailed record information for data quality issues.
    Shows actual data from the database for investigation.
    """
    logger.info("\n" + "="*60)
    logger.info("DETAILED DATA QUALITY ISSUE REPORT")
    logger.info("="*60 + "\n")
    
    # 1. Duplicate students
    if 'duplicate_students' in issues:
        logger.info("1. DUPLICATE STUDENTS:")
        logger.info("   Records with the same EMPLID:\n")
        dup_ids = [ex[0] for ex in issues['duplicate_students']['examples']]
        for emplid in dup_ids:
            records = con_duck.execute(f"""
                SELECT * FROM student WHERE EMPLID = {emplid}
            """).fetchall()
            logger.info(f"   EMPLID {emplid} appears {len(records)} times:")
            for i, rec in enumerate(records, 1):
                logger.info(f"     Record {i}: {rec}")
            logger.info("")
    
    # 2. Duplicate academic programs
    if 'duplicate_acad_prog' in issues:
        logger.info("2. DUPLICATE ACADEMIC PROGRAMS:")
        logger.info("   Records with the same (EMPLID, ACAD_PROG):\n")
        for ex in issues['duplicate_acad_prog']['examples'][:3]:
            emplid, acad_prog = ex[0], ex[1]
            records = con_duck.execute(f"""
                SELECT * FROM acad_prog 
                WHERE EMPLID = {emplid} AND ACAD_PROG = '{acad_prog}'
            """).fetchall()
            logger.info(f"   EMPLID {emplid}, ACAD_PROG '{acad_prog}' appears {len(records)} times:")
            for i, rec in enumerate(records, 1):
                logger.info(f"     Record {i}: {rec}")
            logger.info("")
    
    # 3. Duplicate departments
    if 'duplicate_departments' in issues:
        logger.info("3. DUPLICATE DEPARTMENTS:")
        logger.info("   Records with the same DEPT_CODE:\n")
        for ex in issues['duplicate_departments']['examples'][:3]:
            dept_code = ex[0]
            records = con_duck.execute(f"""
                SELECT * FROM departments WHERE DEPT_CODE = '{dept_code}'
            """).fetchall()
            logger.info(f"   DEPT_CODE '{dept_code}' appears {len(records)} times:")
            for i, rec in enumerate(records, 1):
                logger.info(f"     Record {i}: {rec}")
            logger.info("")
    
    # 4. Orphaned academic programs
    if 'orphaned_acad_prog' in issues:
        logger.info("4. ORPHANED ACADEMIC PROGRAMS:")
        logger.info("   Academic programs for non-existent students:\n")
        for emplid in issues['orphaned_acad_prog']['student_ids'][:3]:
            records = con_duck.execute(f"""
                SELECT * FROM acad_prog WHERE EMPLID = {emplid}
            """).fetchall()
            logger.info(f"   EMPLID {emplid} (student not found in student table):")
            for rec in records:
                logger.info(f"     {rec}")
            logger.info("")
    
    # 5. Orphaned enrollments (student)
    if 'orphaned_enrollments_student' in issues:
        logger.info("5. ORPHANED ENROLLMENTS (Missing Student):")
        logger.info("   Enrollments for non-existent students:\n")
        for emplid in issues['orphaned_enrollments_student']['student_ids'][:3]:
            records = con_duck.execute(f"""
                SELECT * FROM enrollments WHERE EMPLID = {emplid} LIMIT 3
            """).fetchall()
            logger.info(f"   EMPLID {emplid} (student not found in student table):")
            for rec in records:
                logger.info(f"     {rec}")
            logger.info("")
    
    # 6. Orphaned enrollments (department)
    if 'orphaned_enrollments_dept' in issues:
        logger.info("6. ORPHANED ENROLLMENTS (Invalid Department):")
        logger.info("   Enrollments with invalid department codes:\n")
        for dept_code in issues['orphaned_enrollments_dept']['departments'][:3]:
            records = con_duck.execute(f"""
                SELECT * FROM enrollments WHERE DEPARTMENT = '{dept_code}' LIMIT 3
            """).fetchall()
            logger.info(f"   DEPT_CODE '{dept_code}' (not found in departments table):")
            for rec in records:
                logger.info(f"     {rec}")
            logger.info("")
    
    logger.info("="*60 + "\n")

def clean_data_quality_issues(con_duck, issues):
    """
    Attempt to clean data quality issues automatically.
    Returns True if all issues were resolved.
    """
    logger.info("\nAttempting to clean data quality issues...")
    
    # 1. Remove duplicate students (keep first occurrence)
    if 'duplicate_students' in issues:
        con_duck.execute("""
            CREATE OR REPLACE TABLE student AS
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY EMPLID ORDER BY EMPLID) as rn
                FROM student
            ) WHERE rn = 1
        """)
        logger.info(f"  ✓ Removed {issues['duplicate_students']['count']} duplicate students")
    
    # 2. Remove duplicate academic programs (only true duplicates where ALL fields match)
    if 'duplicate_acad_prog' in issues:
        acad_cols = con_duck.execute("DESCRIBE acad_prog").fetchall()
        col_names = [col[0] for col in acad_cols]
        partition_clause = ", ".join(col_names)
        
        con_duck.execute(f"""
            CREATE OR REPLACE TABLE acad_prog AS
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY {col_names[0]}) as rn
                FROM acad_prog
            ) WHERE rn = 1
        """)
        logger.info(f"  ✓ Removed {issues['duplicate_acad_prog']['count']} duplicate academic programs")
    
    # 3. Remove duplicate departments
    if 'duplicate_departments' in issues:
        con_duck.execute("""
            CREATE OR REPLACE TABLE departments AS
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY DEPT_CODE ORDER BY DEPT_CODE) as rn
                FROM departments
            ) WHERE rn = 1
        """)
        logger.info(f"  ✓ Removed {issues['duplicate_departments']['count']} duplicate departments")
    
    # 4. Remove duplicate enrollments
    if 'duplicate_enrollments' in issues:
        columns = [row[0] for row in con_duck.execute("DESCRIBE enrollments").fetchall()]
        pk_columns = ["EMPLID", "STRM"]
        if "COURSE_ID" in columns:
            pk_columns.append("COURSE_ID")
        if "CLASS_NBR" in columns:
            pk_columns.append("CLASS_NBR")
        pk_clause = ", ".join(pk_columns)
        
        con_duck.execute(f"""
            CREATE OR REPLACE TABLE enrollments AS
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_clause} ORDER BY {pk_columns[0]}) as rn
                FROM enrollments
            ) WHERE rn = 1
        """)
        logger.info(f"  ✓ Removed {issues['duplicate_enrollments']['count']} duplicate enrollments")
    
    # 5. Remove orphaned academic programs
    if 'orphaned_acad_prog' in issues:
        con_duck.execute("""
            CREATE OR REPLACE TABLE acad_prog AS
            SELECT a.* FROM acad_prog a
            INNER JOIN student s ON a.EMPLID = s.EMPLID
        """)
        logger.info(f"  ✓ Removed {issues['orphaned_acad_prog']['count']} orphaned academic programs")
    
    # 6. Remove orphaned enrollments (student)
    if 'orphaned_enrollments_student' in issues:
        con_duck.execute("""
            CREATE OR REPLACE TABLE enrollments AS
            SELECT e.* FROM enrollments e
            INNER JOIN student s ON e.EMPLID = s.EMPLID
        """)
        logger.info(f"  ✓ Removed {issues['orphaned_enrollments_student']['count']} orphaned enrollments (student)")
    
    # 7. Remove enrollments with invalid departments
    if 'orphaned_enrollments_dept' in issues:
        con_duck.execute("""
            CREATE OR REPLACE TABLE enrollments AS
            SELECT e.* FROM enrollments e
            INNER JOIN departments d ON e.DEPARTMENT = d.DEPT_CODE
        """)
        logger.info(f"  ✓ Removed {issues['orphaned_enrollments_dept']['count']} enrollments with invalid departments")
    
    # 8. Remove records with NULL required fields
    null_field_issues = [k for k in issues.keys() if k.startswith('null_')]
    for issue_key in null_field_issues:
        table = issues[issue_key]['table']
        column = issues[issue_key]['column']
        count = issues[issue_key]['count']
        
        con_duck.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM {table} WHERE {column} IS NOT NULL
        """)
        logger.info(f"  ✓ Removed {count} records with NULL {column} from {table}")
    
    # 9. Fix invalid credit hours (set to 0 if negative, cap at 30 if too high)
    if 'invalid_credit_hours' in issues:
        con_duck.execute("""
            CREATE OR REPLACE TABLE enrollments AS
            SELECT 
                EMPLID,
                STRM,
                DEPARTMENT,
                CASE 
                    WHEN CREDIT_HOURS < 0 THEN 0
                    WHEN CREDIT_HOURS > 30 THEN 30
                    ELSE CREDIT_HOURS
                END AS CREDIT_HOURS,
                * EXCLUDE (EMPLID, STRM, DEPARTMENT, CREDIT_HOURS)
            FROM enrollments
        """)
        logger.info(f"  ✓ Fixed {issues['invalid_credit_hours']['count']} invalid credit hour values")
    
    # 10. Remove students with empty names
    if 'empty_student_names' in issues:
        con_duck.execute("""
            CREATE OR REPLACE TABLE student AS
            SELECT * FROM student 
            WHERE TRIM(LAST_NAME) != '' AND LAST_NAME IS NOT NULL
        """)
        logger.info(f"  ✓ Removed {issues['empty_student_names']['count']} students with empty names")
    
    # Note: students_no_enrollments is a warning, not cleaned automatically
    if 'students_no_enrollments' in issues:
        logger.warning(f"  ⚠ {issues['students_no_enrollments']['count']} students have no enrollments (not cleaned - may be valid)")
    
    return True

def apply_database_constraints(con_duck):
    """
    Apply primary and foreign key constraints to enforce referential integrity.
    This improves data quality, query performance, and documents relationships.
    
    NOTE: This function assumes data quality has been validated first!
    DuckDB requires constraints to be defined during table creation, not via ALTER TABLE.
    """
    logger.info("\nApplying database constraints...")
    
    try:
        # Get column info for each table to build CREATE TABLE statements
        
        # 1. Student table - EMPLID is primary key
        student_cols = con_duck.execute("DESCRIBE student").fetchall()
        col_defs = [f"{col[0]} {col[1]}" for col in student_cols]
        col_defs_str = ", ".join(col_defs)
        
        con_duck.execute(f"""
            CREATE TABLE student_new (
                {col_defs_str},
                PRIMARY KEY (EMPLID)
            )
        """)
        con_duck.execute("INSERT INTO student_new SELECT * FROM student")
        con_duck.execute("DROP TABLE student")
        con_duck.execute("ALTER TABLE student_new RENAME TO student")
        
        # 2. Departments table - DEPT_CODE is primary key (no foreign keys, so do this before acad_prog/enrollments)
        dept_cols = con_duck.execute("DESCRIBE departments").fetchall()
        col_defs = [f"{col[0]} {col[1]}" for col in dept_cols]
        col_defs_str = ", ".join(col_defs)
        
        con_duck.execute(f"""
            CREATE TABLE departments_new (
                {col_defs_str},
                PRIMARY KEY (DEPT_CODE)
            )
        """)
        con_duck.execute("INSERT INTO departments_new SELECT * FROM departments")
        con_duck.execute("DROP TABLE departments")
        con_duck.execute("ALTER TABLE departments_new RENAME TO departments")
        
        # 3. Academic Program table - composite key includes EFFDT for temporal tracking + FK to student
        acad_cols = con_duck.execute("DESCRIBE acad_prog").fetchall()
        col_defs = [f"{col[0]} {col[1]}" for col in acad_cols]
        col_defs_str = ", ".join(col_defs)
        
        # Check if EFFDT column exists for temporal primary key
        col_names = [col[0] for col in acad_cols]
        if 'EFFDT' in col_names:
            pk_clause = "EMPLID, ACAD_PROG, EFFDT"
        else:
            # Fallback to just EMPLID, ACAD_PROG if no EFFDT
            pk_clause = "EMPLID, ACAD_PROG"
        
        con_duck.execute(f"""
            CREATE TABLE acad_prog_new (
                {col_defs_str},
                PRIMARY KEY ({pk_clause}),
                FOREIGN KEY (EMPLID) REFERENCES student(EMPLID)
            )
        """)
        con_duck.execute("INSERT INTO acad_prog_new SELECT * FROM acad_prog")
        con_duck.execute("DROP TABLE acad_prog")
        con_duck.execute("ALTER TABLE acad_prog_new RENAME TO acad_prog")
        
        # 4. Enrollments table - composite key and foreign keys
        enr_cols = con_duck.execute("DESCRIBE enrollments").fetchall()
        col_defs = [f"{col[0]} {col[1]}" for col in enr_cols]
        col_defs_str = ", ".join(col_defs)
        
        # Build primary key based on available columns
        columns = [col[0] for col in enr_cols]
        pk_columns = ["EMPLID", "STRM"]
        if "COURSE_ID" in columns:
            pk_columns.append("COURSE_ID")
        if "CLASS_NBR" in columns:
            pk_columns.append("CLASS_NBR")
        pk_clause = ", ".join(pk_columns)
        
        con_duck.execute(f"""
            CREATE TABLE enrollments_new (
                {col_defs_str},
                PRIMARY KEY ({pk_clause}),
                FOREIGN KEY (EMPLID) REFERENCES student(EMPLID),
                FOREIGN KEY (DEPARTMENT) REFERENCES departments(DEPT_CODE)
            )
        """)
        con_duck.execute("INSERT INTO enrollments_new SELECT * FROM enrollments")
        con_duck.execute("DROP TABLE enrollments")
        con_duck.execute("ALTER TABLE enrollments_new RENAME TO enrollments")
        
        logger.info("  ✓ Primary keys added to all tables")
        logger.info("  ✓ Foreign key relationships established")
        logger.info("  ✓ Referential integrity enforced")
        return True
        
    except Exception as e:
        logger.error(f"  ✗ Failed to apply constraints: {e}")
        logger.error(f"  Error details: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# SQL to create views for transformations
view_sql = r"""
-- total credits per student-term
CREATE OR REPLACE VIEW total_credits AS
SELECT EMPLID AS student_id, STRM AS term, SUM(CAST(CREDIT_HOURS AS INTEGER)) AS total_credits
FROM enrollments
GROUP BY EMPLID, STRM;

-- credits per student-term-department
CREATE OR REPLACE VIEW credits_by_dept AS
SELECT EMPLID AS student_id, STRM AS term, DEPARTMENT AS dept_code,
       SUM(CAST(CREDIT_HOURS AS INTEGER)) AS dept_credits
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
"""

# Final SELECT query for output
final_select = r"""
SELECT
  t.student_id,
  s.LAST_NAME AS last_name,
  t.term,
  t.total_credits,
  rd.dept_name AS focused_department_name,
  rd.dept_contact AS focused_department_contact
FROM total_credits t
LEFT JOIN ranked_depts rd ON t.student_id = rd.student_id AND t.term = rd.term AND rd.rn = 1
LEFT JOIN student s ON t.student_id = s.EMPLID
ORDER BY t.student_id, t.term
"""

def run():
    metrics.start()
    logger.info("Starting load_and_transform...")
    
    # Validate input files exist
    logger.info("Validating input files...")
    required_files = {
        "student_info.sqlite3": SQLITE_PATH,
        "enrollments.dat": ENROLLMENTS_PATH,
        "departments.json": DEPARTMENTS_PATH
    }
    
    missing_files = []
    for name, path in required_files.items():
        if not os.path.exists(path):
            missing_files.append(name)
    
    if missing_files:
        logger.error("\nERROR: Missing required input files:")
        for file in missing_files:
            logger.error(f"  - {file}")
        logger.error(f"\nPlease ensure all input files are in the '{INPUT_DIR}' directory.")
        logger.error("Required files:")
        logger.error("  - student_info.sqlite3")
        logger.error("  - enrollments.dat")
        logger.error("  - departments.json")
        return
    
    logger.info("  ✓ All input files found")
    logger.info("")

    # connect to/create duckdb file
    con = duckdb.connect(database=OUT_DUCKDB, read_only=False)
    try:
        # Drop all existing tables to avoid foreign key dependency issues
        logger.info("Cleaning existing database...")
        step_start = time.time()
        try:
            con.execute("DROP TABLE IF EXISTS enrollments CASCADE")
            con.execute("DROP TABLE IF EXISTS acad_prog CASCADE")
            con.execute("DROP TABLE IF EXISTS departments CASCADE")
            con.execute("DROP TABLE IF EXISTS student CASCADE")
            logger.info("  ✓ Existing tables dropped")
        except Exception as e:
            # If tables don't exist, that's fine
            pass
        metrics.record_step("Database cleanup", time.time() - step_start)
        
        logger.info("Loading sqlite tables...")
        step_start = time.time()
        load_sqlite_tables_to_duckdb(con, SQLITE_PATH)
        student_count = con.execute("SELECT COUNT(*) FROM student").fetchone()[0]
        acad_count = con.execute("SELECT COUNT(*) FROM acad_prog").fetchone()[0]
        metrics.record_step("Load SQLite tables", time.time() - step_start, student_count + acad_count)

        logger.info("Loading enrollments...")
        step_start = time.time()
        load_enrollments_to_duckdb(con, ENROLLMENTS_PATH)
        enr_count = con.execute("SELECT COUNT(*) FROM enrollments").fetchone()[0]
        metrics.record_step("Load enrollments", time.time() - step_start, enr_count)

        logger.info("Loading departments...")
        step_start = time.time()
        load_departments_to_duckdb(con, DEPARTMENTS_PATH)
        dept_count = con.execute("SELECT COUNT(*) FROM departments").fetchone()[0]
        metrics.record_step("Load departments", time.time() - step_start, dept_count)

        # Validate data quality BEFORE applying constraints
        logger.info("")
        step_start = time.time()
        is_valid, issues = validate_data_quality(con)
        metrics.record_step("Data quality validation", time.time() - step_start)
        
        if not is_valid:
            # Print detailed information about problematic records
            print_detailed_issue_records(con, issues)
            
            # Attempt to clean the data
            step_start = time.time()
            clean_data_quality_issues(con, issues)
            metrics.record_step("Data cleaning", time.time() - step_start)
            
            # Re-validate after cleaning
            logger.info("")
            is_valid, remaining_issues = validate_data_quality(con)
            
            if not is_valid:
                logger.error("\n❌ ERROR: Data quality issues remain after cleaning:")
                for issue_type, details in remaining_issues.items():
                    logger.error(f"  - {issue_type}: {details}")
                logger.error("\nPlease fix these issues in the source data.")
                return
        
        # Only apply constraints if data is clean
        step_start = time.time()
        success = apply_database_constraints(con)
        metrics.record_step("Apply constraints", time.time() - step_start)
        if not success:
            logger.warning("\n⚠ Warning: Constraints could not be applied, but continuing with report generation...")

        logger.info("\nRunning SQL transforms...")
        step_start = time.time()
        
        # Create views first
        con.execute(view_sql)
        
        # Get row count for metrics
        row_count = con.execute(f"SELECT COUNT(*) FROM ({final_select})").fetchone()[0]
        metrics.record_step("SQL transformations", time.time() - step_start, row_count)
        logger.info(f"  ✓ Generated report: {row_count} rows")

        # Write CSV directly using DuckDB's COPY command (faster than pandas)
        step_start = time.time()
        con.execute(f"""
            COPY (
                {final_select}
            ) TO '{OUT_CSV}' (HEADER, DELIMITER ',')
        """)
        metrics.record_step("Write output CSV", time.time() - step_start)
        logger.info(f"Done — output written to: {OUT_CSV}")
        
        # Print performance summary
        metrics.print_summary()

    except Exception as e:
        logger.error("ERROR: transform failed.")
        traceback.print_exc(file=sys.stdout)
    finally:
        con.close()

if __name__ == "__main__":
    run()
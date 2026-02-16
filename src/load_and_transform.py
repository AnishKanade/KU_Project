import os
import sqlite3
import json
import pandas as pd
import duckdb
import sys
import traceback

# Paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(ROOT, "KU_Input")
SQLITE_PATH = os.path.join(INPUT_DIR, "student_info.sqlite3")
ENROLLMENTS_PATH = os.path.join(INPUT_DIR, "enrollments.dat")
DEPARTMENTS_PATH = os.path.join(INPUT_DIR, "departments.json")

OUT_DUCKDB = os.path.join(ROOT, "ku.duckdb")
OUT_CSV = os.path.join(ROOT, "output.csv")

# helper loaders
def load_sqlite_tables_to_duckdb(con_duck, sqlite_path):
    sqlite_conn = sqlite3.connect(sqlite_path)
    tables = [r[0] for r in sqlite_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    for t in tables:
        df = pd.read_sql_query(f"SELECT * FROM {t}", sqlite_conn)
        df.columns = [c.upper() for c in df.columns]
        con_duck.register("tmp_df", df)
        con_duck.execute(f"CREATE OR REPLACE TABLE {t.lower()} AS SELECT * FROM tmp_df")
        con_duck.unregister("tmp_df")
        
       
        # print(f"  ✓ Loaded table '{t}': {len(df)} rows")
    sqlite_conn.close()

def load_enrollments_to_duckdb(con_duck, enrollments_path):
    df = pd.read_csv(enrollments_path, sep="|", dtype=str)
    df.columns = [c.upper() for c in df.columns]
    for c in df.select_dtypes(include=["object", "string"]).columns:
        df[c] = df[c].str.strip()
    if "CREDIT_HOURS" in df.columns:
        df["CREDIT_HOURS"] = pd.to_numeric(df["CREDIT_HOURS"], errors="coerce").fillna(0).astype(int)
    con_duck.register("tmp_enr", df)
    con_duck.execute("CREATE OR REPLACE TABLE enrollments AS SELECT * FROM tmp_enr")
    con_duck.unregister("tmp_enr")
    
    # print(f"  ✓ Loaded enrollments: {len(df)} records")

def load_departments_to_duckdb(con_duck, departments_path):
    with open(departments_path, "r") as f:
        data = json.load(f)
    dept_df = pd.DataFrame(data)
    dept_df.columns = [c.upper() for c in dept_df.columns]
    for c in dept_df.select_dtypes(include=["object", "string"]).columns:
        dept_df[c] = dept_df[c].str.strip()
    con_duck.register("tmp_dept", dept_df)
    con_duck.execute("CREATE OR REPLACE TABLE departments AS SELECT * FROM tmp_dept")
    con_duck.unregister("tmp_dept")
    
    # print(f"  ✓ Loaded departments: {len(dept_df)} departments")

# main transform SQL 
final_sql = r"""
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

-- final select returns one row per student per term with focused dept (rn=1)
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
ORDER BY t.student_id, t.term;
"""

def run():
    print("Starting load_and_transform...")
    
    # Validate input files exist
    print("Validating input files...")
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
        print("\nERROR: Missing required input files:")
        for file in missing_files:
            print(f"  - {file}")
        print(f"\nPlease ensure all input files are in the '{INPUT_DIR}' directory.")
        print("Required files:")
        print("  - student_info.sqlite3")
        print("  - enrollments.dat")
        print("  - departments.json")
        return
    
    print("  ✓ All input files found")
    print()

    # connect to/create duckdb file
    con = duckdb.connect(database=OUT_DUCKDB, read_only=False)
    try:
        print("Loading sqlite tables...")
        load_sqlite_tables_to_duckdb(con, SQLITE_PATH)

        print("Loading enrollments...")
        load_enrollments_to_duckdb(con, ENROLLMENTS_PATH)

        print("Loading departments...")
        load_departments_to_duckdb(con, DEPARTMENTS_PATH)

        print("Running SQL transforms...")
        # Execute the SQL and capture result
        df_out = con.execute(final_sql).df()
        print(f"  ✓ Generated report: {len(df_out)} rows")

        # Ensure exact columns in safe way (avoid KeyError)
        desired_cols = ["student_id", "last_name", "term", "total_credits",
                        "focused_department_name", "focused_department_contact"]

        # reindex will add missing columns as NaN instead of crashing
        df_out = df_out.reindex(columns=desired_cols)

        # Convert total_credits to integer
        if "total_credits" in df_out.columns:
            df_out["total_credits"] = df_out["total_credits"].fillna(0).astype(int)

        # Replace any NaN in contact with empty string for nicer CSV
        if "focused_department_contact" in df_out.columns:
            df_out["focused_department_contact"] = df_out["focused_department_contact"].fillna("")

        # Write CSV without quotes to match expected format
        df_out.to_csv(OUT_CSV, index=False)  # quoting=1 would add quotes around all values
        print("Done — output written to:", OUT_CSV)

    except Exception as e:
        print("ERROR: transform failed.")
        traceback.print_exc(file=sys.stdout)
    finally:
        con.close()

if __name__ == "__main__":
    run()

# example queries practices to deeper my understanding:


# How many students per department?:

# SELECT focused_department_name, COUNT(DISTINCT student_id) AS student_count
# FROM (
#     SELECT
#       t.student_id,
#       COALESCE(d.DEPT_NAME, c.DEPT_CODE) AS focused_department_name
#     FROM credits_by_dept c
#     LEFT JOIN departments d ON c.dept_code = d.DEPT_CODE
#     JOIN total_credits t ON c.student_id = t.student_id AND c.term = t.term
# )
# GROUP BY focused_department_name
# ORDER BY student_count DESC
# LIMIT 5;
# """).df()




# Slice by term:

# SELECT term, SUM(total_credits) AS total_term_credits
# FROM total_credits
# GROUP BY term
# ORDER BY term;
# """).df()
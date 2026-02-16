Purpose: Load the provided inputs (student_info.sqlite3, enrollments.dat, departments.json) into a single analytics store, run SQL transforms to compute one row per student × term, and export output.csv with the required fields.

High-level flow
[ student_info.sqlite3 ]   [ enrollments.dat ]   [ departments.json ]
        │                        │                     │
        └─────> Load into :contentReference[oaicite:0]{index=0} (student, acad_prog, enrollments, departments)
                                   │
                                   ▼
                         SQL transforms (views & queries)
                                   │
                                   ▼
                                output.csv

Transformation stages (short)

total_credits — sum CREDIT_HOURS grouped by EMPLID + STRM.

credits_by_dept — sum CREDIT_HOURS grouped by EMPLID + STRM + DEPARTMENT.

ranked_depts — use ROW_NUMBER() window function partitioned by student_id, term, ordered by dept_credits DESC, then dept_name ASC to break ties alphabetically.

final select — join totals, focused department (where rn = 1), and student.LAST_NAME to produce the final CSV columns:

student_id,last_name,term,total_credits,focused_department_name,focused_department_contact

How data is loaded (brief)

Read SQLite tables using sqlite3 / pandas and write them into DuckDB. (Student table provides LAST_NAME.)

Read enrollments.dat with pandas.read_csv(sep="|") and persist into DuckDB as enrollments.

Read departments.json with json → pandas.DataFrame and persist as departments.
(You can mention in README that pandas and duckdb are used.) For clarity: we used Python + pandas to load/clean data and persist into DuckDB.

Key design decisions

DuckDB chosen because it is embeddable, fast for analytic SQL, and supports window functions (cleanly implements the tie-break requirement).

Keep source tables (student, acad_prog) intact in DuckDB to preserve lineage and enable future queries.

Use SQL views for stepwise validation (readable and testable).

Prefer SQL for aggregation and ranking rather than manual Python grouping (simpler, less error-prone).

Assumptions & edge cases

Non-numeric or missing CREDIT_HOURS → treated as 0.

If DEPARTMENT (from enrollments) has no match in departments.json, the pipeline uses the department code as the department name and leaves contact empty (NULL).

Only student+term combinations with enrollments are included in output (matches spec).

All EMPLID values are treated as strings to preserve leading zeros where present.

Quick validation checks (run after producing output.csv)

Verify header matches output_snippet.csv:
head -n 1 output.csv should equal the expected header.

Spot-check totals for one student-term by summing enrollments:
SELECT SUM(CREDIT_HOURS) FROM enrollments WHERE EMPLID='1000000' AND STRM='2251'; (run in DuckDB).

Verify tie-break logic by querying credits_by_dept for a sample student-term and checking ORDER BY dept_credits DESC, dept_name ASC.
"""
Simple unit tests for the KU Student Data Pipeline
"""
import os
import duckdb

# Paths
ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_CSV = os.path.join(ROOT, "output.csv")
OUT_DUCKDB = os.path.join(ROOT, "ku.duckdb")

def test_output_csv_exists():
    """Test that output.csv was created"""
    assert os.path.exists(OUT_CSV), "output.csv not found"
    print("✓ Test passed: output.csv exists")

def test_output_csv_structure():
    """Test that output.csv has correct columns"""
    con = duckdb.connect()
    columns = con.execute(f"SELECT * FROM read_csv('{OUT_CSV}') LIMIT 0").description
    actual_cols = [col[0] for col in columns]
    expected_cols = ["student_id", "last_name", "term", "total_credits",
                     "focused_department_name", "focused_department_contact"]
    assert actual_cols == expected_cols, f"Column mismatch: {actual_cols}"
    con.close()
    print("✓ Test passed: output.csv has correct columns")

def test_output_csv_row_count():
    """Test that output.csv has data"""
    con = duckdb.connect()
    row_count = con.execute(f"SELECT COUNT(*) FROM read_csv('{OUT_CSV}')").fetchone()[0]
    assert row_count > 0, "output.csv is empty"
    con.close()
    print(f"✓ Test passed: output.csv has {row_count} rows")

def test_output_csv_no_nulls():
    """Test that critical columns have no null values"""
    con = duckdb.connect()
    null_checks = con.execute(f"""
        SELECT 
            COUNT(*) FILTER (WHERE student_id IS NULL) as null_student_id,
            COUNT(*) FILTER (WHERE last_name IS NULL) as null_last_name,
            COUNT(*) FILTER (WHERE term IS NULL) as null_term,
            COUNT(*) FILTER (WHERE total_credits IS NULL) as null_total_credits
        FROM read_csv('{OUT_CSV}')
    """).fetchone()
    con.close()
    assert null_checks[0] == 0, "student_id has null values"
    assert null_checks[1] == 0, "last_name has null values"
    assert null_checks[2] == 0, "term has null values"
    assert null_checks[3] == 0, "total_credits has null values"
    print("✓ Test passed: No null values in critical columns")

def test_output_csv_data_types():
    """Test that columns have correct data types"""
    con = duckdb.connect()
    # Check if student_id and total_credits can be cast to integers
    try:
        con.execute(f"""
            SELECT 
                CAST(student_id AS INTEGER),
                CAST(total_credits AS INTEGER)
            FROM read_csv('{OUT_CSV}')
            LIMIT 1
        """).fetchone()
        con.close()
        print("✓ Test passed: Data types are correct")
    except Exception as e:
        con.close()
        raise AssertionError(f"Data type validation failed: {e}")

def test_total_credits_are_integers():
    """Test that total_credits are integers and non-negative"""
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT 
            COUNT(*) FILTER (WHERE total_credits < 0) as negative_count,
            COUNT(*) FILTER (WHERE TRY_CAST(total_credits AS INTEGER) IS NULL) as non_integer_count
        FROM read_csv('{OUT_CSV}')
    """).fetchone()
    con.close()
    assert result[0] == 0, "Found negative credit values"
    assert result[1] == 0, "Found non-integer credit values"
    print("✓ Test passed: All credit values are non-negative integers")

def test_no_null_values():
    """Test that critical columns have no null values"""
    test_output_csv_no_nulls()

def test_no_duplicate_student_term():
    """Test that there are no duplicate student-term combinations"""
    con = duckdb.connect()
    duplicates = con.execute(f"""
        SELECT 
            student_id, 
            term, 
            COUNT(*) as count
        FROM read_csv('{OUT_CSV}')
        GROUP BY student_id, term
        HAVING COUNT(*) > 1
    """).fetchall()
    con.close()
    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate student-term combinations"
    print("✓ Test passed: No duplicate student-term combinations")

def test_duckdb_exists():
    """Test that DuckDB database was created"""
    assert os.path.exists(OUT_DUCKDB), "ku.duckdb not found"
    print("✓ Test passed: ku.duckdb exists")

def test_duckdb_tables():
    """Test that DuckDB has expected tables"""
    con = duckdb.connect(database=OUT_DUCKDB, read_only=True)
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    con.close()
    
    expected_tables = ["student", "acad_prog", "enrollments", "departments"]
    for table in expected_tables:
        assert table in tables, f"Table '{table}' not found in DuckDB"
    print(f"✓ Test passed: DuckDB has all expected tables: {expected_tables}")

def test_sample_data_matches():
    """Test that sample rows match expected output"""
    con = duckdb.connect()
    
    # Test case from output_snippet.csv
    row = con.execute(f"""
        SELECT * FROM read_csv('{OUT_CSV}')
        WHERE student_id = 1000000 AND term = 2244
    """).fetchone()
    con.close()
    
    if row:
        # row format: (student_id, last_name, term, total_credits, dept_name, dept_contact)
        assert row[1] == "Anderson", "Last name mismatch"
        assert row[3] == 13, "Total credits mismatch"
        assert row[4] == "Physics", "Department mismatch"
        assert row[5] == "Dr. James Wilson", "Contact mismatch"
        print("✓ Test passed: Sample data matches expected output")
    else:
        print("⚠ Warning: Sample student not found in output")

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("Running Unit Tests for KU Student Data Pipeline")
    print("="*60 + "\n")
    
    tests = [
        test_output_csv_exists,
        test_output_csv_structure,
        test_output_csv_row_count,
        test_no_null_values,
        test_total_credits_are_integers,
        test_no_duplicate_student_term,
        test_duckdb_exists,
        test_duckdb_tables,
        test_sample_data_matches
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ Test failed: {test.__name__} - {e}")
            failed += 1
        except Exception as e:
            print(f"✗ Test error: {test.__name__} - {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("="*60 + "\n")
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)

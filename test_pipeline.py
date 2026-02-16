"""
Simple unit tests for the KU Student Data Pipeline
"""
import os
import pandas as pd
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
    df = pd.read_csv(OUT_CSV)
    expected_cols = ["student_id", "last_name", "term", "total_credits",
                     "focused_department_name", "focused_department_contact"]
    assert list(df.columns) == expected_cols, f"Column mismatch: {list(df.columns)}"
    print("✓ Test passed: output.csv has correct columns")

def test_output_csv_row_count():
    """Test that output.csv has data"""
    df = pd.read_csv(OUT_CSV)
    assert len(df) > 0, "output.csv is empty"
    print(f"✓ Test passed: output.csv has {len(df)} rows")

def test_no_null_values():
    """Test that there are no null values in critical columns"""
    df = pd.read_csv(OUT_CSV)
    assert df["student_id"].notna().all(), "Found null student_id values"
    assert df["last_name"].notna().all(), "Found null last_name values"
    assert df["term"].notna().all(), "Found null term values"
    assert df["total_credits"].notna().all(), "Found null total_credits values"
    print("✓ Test passed: No null values in critical columns")

def test_total_credits_are_integers():
    """Test that total_credits are integers"""
    df = pd.read_csv(OUT_CSV)
    assert df["total_credits"].dtype in ['int64', 'int32'], "total_credits should be integers"
    print("✓ Test passed: total_credits are integers")

def test_no_duplicate_student_term():
    """Test that there are no duplicate student-term combinations"""
    df = pd.read_csv(OUT_CSV)
    duplicates = df.groupby(["student_id", "term"]).size()
    duplicates = duplicates[duplicates > 1]
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
    df = pd.read_csv(OUT_CSV)
    
    # Test case from output_snippet.csv
    row = df[(df["student_id"] == 1000000) & (df["term"] == 2244)]
    if len(row) > 0:
        row = row.iloc[0]
        assert row["last_name"] == "Anderson", "Last name mismatch"
        assert row["total_credits"] == 13, "Total credits mismatch"
        assert row["focused_department_name"] == "Physics", "Department mismatch"
        assert row["focused_department_contact"] == "Dr. James Wilson", "Contact mismatch"
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

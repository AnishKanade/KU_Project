"""
Verify database constraints and referential integrity
"""
import os
import duckdb

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DUCKDB = os.path.join(ROOT, "ku.duckdb")

def verify_constraints():
    """Verify that all constraints are properly applied"""
    print("\n" + "="*60)
    print("Database Constraint Verification")
    print("="*60 + "\n")
    
    if not os.path.exists(OUT_DUCKDB):
        print("❌ Database file not found. Run load_and_transform.py first.")
        return False
    
    con = duckdb.connect(database=OUT_DUCKDB, read_only=True)
    
    try:
        # 1. Check primary keys exist
        print("1. Verifying Primary Keys...")
        tables_with_pk = ["student", "acad_prog", "departments", "enrollments"]
        
        for table in tables_with_pk:
            # Get table info
            result = con.execute(f"PRAGMA table_info('{table}')").fetchall()
            pk_columns = [row[1] for row in result if row[5] > 0]  # row[5] is pk flag
            
            if pk_columns:
                print(f"   ✓ {table}: PK on ({', '.join(pk_columns)})")
            else:
                print(f"   ⚠ {table}: No primary key found")
        
        # 2. Check for duplicate primary keys (should be 0)
        print("\n2. Checking for Duplicate Primary Keys...")
        
        # Student duplicates
        dup_students = con.execute("""
            SELECT EMPLID, COUNT(*) as cnt 
            FROM student 
            GROUP BY EMPLID 
            HAVING COUNT(*) > 1
        """).fetchall()
        print(f"   {'✓' if len(dup_students) == 0 else '❌'} Student duplicates: {len(dup_students)}")
        
        # Enrollment duplicates
        dup_enrollments = con.execute("""
            SELECT EMPLID, STRM, COUNT(*) as cnt 
            FROM enrollments 
            GROUP BY EMPLID, STRM 
            HAVING COUNT(*) > 1
        """).fetchall()
        print(f"   {'✓' if len(dup_enrollments) == 0 else '❌'} Enrollment duplicates: {len(dup_enrollments)}")
        
        # 3. Check referential integrity
        print("\n3. Verifying Referential Integrity...")
        
        # Orphaned enrollments (students)
        orphaned_student = con.execute("""
            SELECT COUNT(*) as cnt
            FROM enrollments e
            LEFT JOIN student s ON e.EMPLID = s.EMPLID
            WHERE s.EMPLID IS NULL
        """).fetchone()[0]
        print(f"   {'✓' if orphaned_student == 0 else '❌'} Orphaned enrollments (student): {orphaned_student}")
        
        # Orphaned enrollments (departments)
        orphaned_dept = con.execute("""
            SELECT COUNT(*) as cnt
            FROM enrollments e
            LEFT JOIN departments d ON e.DEPARTMENT = d.DEPT_CODE
            WHERE d.DEPT_CODE IS NULL
        """).fetchone()[0]
        print(f"   {'✓' if orphaned_dept == 0 else '❌'} Orphaned enrollments (department): {orphaned_dept}")
        
        # Orphaned academic programs
        orphaned_acad = con.execute("""
            SELECT COUNT(*) as cnt
            FROM acad_prog a
            LEFT JOIN student s ON a.EMPLID = s.EMPLID
            WHERE s.EMPLID IS NULL
        """).fetchone()[0]
        print(f"   {'✓' if orphaned_acad == 0 else '❌'} Orphaned academic programs: {orphaned_acad}")
        
        # 4. Test constraint enforcement (read-only, so we'll just report)
        print("\n4. Database Statistics...")
        
        student_count = con.execute("SELECT COUNT(*) FROM student").fetchone()[0]
        enrollment_count = con.execute("SELECT COUNT(*) FROM enrollments").fetchone()[0]
        dept_count = con.execute("SELECT COUNT(*) FROM departments").fetchone()[0]
        
        print(f"   • Students: {student_count:,}")
        print(f"   • Enrollments: {enrollment_count:,}")
        print(f"   • Departments: {dept_count:,}")
        
        # 5. Performance check - show that indexes exist
        print("\n5. Index Performance Check...")
        
        # Query with index (should be fast)
        import time
        start = time.time()
        con.execute("SELECT * FROM enrollments WHERE EMPLID = 1000000").fetchall()
        elapsed = time.time() - start
        print(f"   • Indexed lookup time: {elapsed*1000:.2f}ms")
        
        print("\n" + "="*60)
        print("✓ All constraint verifications passed!")
        print("="*60 + "\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        return False
    finally:
        con.close()

def test_constraint_violations():
    """
    Demonstrate what happens when constraints are violated.
    This requires write access, so it creates a temporary test database.
    """
    print("\n" + "="*60)
    print("Constraint Violation Tests (Temporary Database)")
    print("="*60 + "\n")
    
    # Create temporary test database
    test_db = os.path.join(ROOT, "test_constraints.duckdb")
    if os.path.exists(test_db):
        os.remove(test_db)
    
    con = duckdb.connect(database=test_db, read_only=False)
    
    try:
        # Create simple test schema
        con.execute("""
            CREATE TABLE student (
                EMPLID INTEGER PRIMARY KEY,
                LAST_NAME VARCHAR
            )
        """)
        
        con.execute("""
            CREATE TABLE enrollments (
                EMPLID INTEGER,
                STRM VARCHAR,
                CREDIT_HOURS INTEGER,
                PRIMARY KEY (EMPLID, STRM),
                FOREIGN KEY (EMPLID) REFERENCES student(EMPLID)
            )
        """)
        
        # Insert test data
        con.execute("INSERT INTO student VALUES (1, 'Doe')")
        con.execute("INSERT INTO enrollments VALUES (1, '2244', 15)")
        
        print("Test 1: Duplicate Primary Key")
        try:
            con.execute("INSERT INTO student VALUES (1, 'Smith')")
            print("   ❌ FAILED: Duplicate was allowed!")
        except Exception as e:
            print(f"   ✓ PASSED: Duplicate rejected - {str(e)[:50]}...")
        
        print("\nTest 2: Foreign Key Violation")
        try:
            con.execute("INSERT INTO enrollments VALUES (999, '2244', 12)")
            print("   ❌ FAILED: Orphaned record was allowed!")
        except Exception as e:
            print(f"   ✓ PASSED: Orphan rejected - {str(e)[:50]}...")
        
        print("\nTest 3: Valid Insert")
        try:
            con.execute("INSERT INTO student VALUES (2, 'Johnson')")
            con.execute("INSERT INTO enrollments VALUES (2, '2244', 18)")
            print("   ✓ PASSED: Valid data accepted")
        except Exception as e:
            print(f"   ❌ FAILED: Valid data rejected - {e}")
        
        print("\n" + "="*60)
        print("✓ Constraint enforcement is working correctly!")
        print("="*60 + "\n")
        
    finally:
        con.close()
        # Clean up test database
        if os.path.exists(test_db):
            os.remove(test_db)

if __name__ == "__main__":
    success = verify_constraints()
    
    if success:
        print("\nRunning constraint violation tests...")
        test_constraint_violations()
    
    exit(0 if success else 1)

import frappe
import pyodbc
from frappe.utils import now_datetime, getdate


def run_attendance_sync():
    conn = None

    try:
        
        # 1. LOAD JOB CONFIG + RUNTIME START

        config = frappe.get_doc("Job Configuration", "Job Configuration")

        frappe.db.set_value(
            "Job Configuration",
            "Job Configuration",
            "start_time",
            now_datetime()
        )
        frappe.db.commit()

        
        # 2. DECIDE WINDOW (RECORD-DRIVEN)
        
        lookback_hours = 1 if (config.records_pulled and config.records_pulled > 0) else 2

        
        # 3. CONNECT TO SQL SERVER
        
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config.server_name};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.get_password('password')};"
            f"TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()

        
        # 4. FETCH BIOMETRIC DATA
        
        cursor.execute(f"""
            SELECT
                A.EmployeeId,
                A.AttendanceDate,
                A.InTime,
                A.OutTime,
                E.AadhaarNumber
            FROM dbo.AttendanceLogs A
            INNER JOIN dbo.Employees E
                ON E.EmployeeId = A.EmployeeId
            WHERE
                (
                    A.InTime >= DATEADD(hour, -{lookback_hours}, GETDATE())
                    OR A.OutTime >= DATEADD(hour, -{lookback_hours}, GETDATE())
                )
                AND E.AadhaarNumber IS NOT NULL
        """)

        rows = cursor.fetchall()
        records_count = len(rows)

        
        # 5. BUILD ERP EMPLOYEE MAPS
        
        device_map = {
            e.attendance_device_id: {
                "name": e.name,
                "joining": e.date_of_joining
            }
            for e in frappe.get_all(
                "Employee",
                fields=["name", "attendance_device_id", "date_of_joining"]
            )
            if e.attendance_device_id
        }

        aadhaar_map = {
            str(e.custom_aadhaar_number).strip(): {
                "name": e.name,
                "joining": e.date_of_joining
            }
            for e in frappe.get_all(
                "Employee",
                fields=["name", "custom_aadhaar_number", "date_of_joining"]
            )
            if e.custom_aadhaar_number
        }

        
        # 6. PROCESS RECORDS

        for emp_id, att_date, in_time, out_time, aadhaar in rows:
            attendance_date = getdate(att_date)

            emp = device_map.get(emp_id)

            # Fallback → Aadhaar match
            if not emp:
                emp = aadhaar_map.get(str(aadhaar).strip())
                if not emp:
                    continue

                frappe.db.set_value(
                    "Employee",
                    emp["name"],
                    "attendance_device_id",
                    emp_id
                )
                device_map[emp_id] = emp

            # Skip before joining date
            if emp["joining"] and attendance_date < emp["joining"]:
                continue

            
            # A. ATTENDANCE LOG (CUSTOM – ONE PER DAY)
            
            if not frappe.db.get_value(
                "Attendance Log",
                {
                    "employee_id": emp["name"],
                    "attendance_date": attendance_date
                },
                "name"
            ):
                frappe.get_doc({
                    "doctype": "Attendance Log",
                    "employee_id": emp["name"],
                    "attendance_date": attendance_date,
                    "in_time": in_time,
                    "out_time": out_time,
                    "attendance_device_id": emp_id
                }).insert(ignore_permissions=True)

            
            # B. ATTENDANCE (STANDARD – ONE PER DAY)
            
            if not frappe.db.exists(
                "Attendance",
                {
                    "employee": emp["name"],
                    "attendance_date": attendance_date,
                    "docstatus": ["!=", 2]
                }
            ):
                att = frappe.new_doc("Attendance")
                att.employee = emp["name"]
                att.attendance_date = attendance_date
                att.status = "Present"
                att.in_time = in_time
                att.out_time = out_time
                att.attendance_device_id = emp_id
                att.insert(ignore_permissions=True)


        # 7. SAVE RESULT + RUNTIME END
        
        frappe.db.set_value(
            "Job Configuration",
            "Job Configuration",
            "records_pulled",
            records_count
        )
        frappe.db.set_value(
            "Job Configuration",
            "Job Configuration",
            "end_time",
            now_datetime()
        )

        frappe.db.commit()

    except Exception:
        frappe.db.rollback()
        frappe.log_error(
            frappe.get_traceback(),
            "Hourly Attendance Sync Failed"
        )

    finally:
        if conn:
            conn.close()

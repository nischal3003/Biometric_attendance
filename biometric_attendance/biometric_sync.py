import frappe
import pyodbc
from frappe.utils import now_datetime, getdate, get_datetime

# Biometric Sync Script - Version 1.1.2 (STRICT SET-ONCE MAPPING)
def run_attendance_sync():
    conn = None

    try:
        # 1. LOAD CONFIG & SET START TIME
        config = frappe.get_doc("Job Configuration", "Job Configuration")
        start_time = now_datetime()
        
        frappe.db.set_value("Job Configuration", "Job Configuration", "start_time", start_time)
        frappe.db.commit()

        # 2. DECIDE WINDOW
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

        # 4. PROACTIVE MAPPING PHASE (BULLETPROOF)
        missing_id_emps = frappe.get_all(
            "Employee", 
            filters={"attendance_device_id": ("is", "not set"), "custom_aadhaar_number": ("is", "set")}, 
            fields=["name", "custom_aadhaar_number"]
        )

        if missing_id_emps:
            aadhaar_to_f_name = {str(e.custom_aadhaar_number).strip(): e.name for e in missing_id_emps}
            aadhaar_list = [f"'{a}'" for a in aadhaar_to_f_name.keys()]
            
            cursor.execute(f"SELECT EmployeeId, AadhaarNumber FROM dbo.Employees WHERE AadhaarNumber IN ({','.join(aadhaar_list)})")
            
            for bio_row in cursor.fetchall():
                bio_id = str(bio_row.EmployeeId).strip()
                bio_aadhaar = str(bio_row.AadhaarNumber).strip()
                f_name = aadhaar_to_f_name.get(bio_aadhaar)
                
                if f_name:
                    existing_owner = frappe.db.get_value("Employee", {"attendance_device_id": bio_id}, "name")
                    if not existing_owner:
                        frappe.db.set_value("Employee", f_name, "attendance_device_id", bio_id, update_modified=False)
            
            frappe.db.commit()

        # 5. FETCH BIOMETRIC DATA
        cursor.execute(f"""
    SELECT
        A.EmployeeId,
        CAST(A.AttendanceDate AS DATE) AS AttendanceDate,
        MIN(A.InTime) AS InTime,
        MAX(A.OutTime) AS OutTime,
        E.AadhaarNumber
    FROM dbo.AttendanceLogs A
    INNER JOIN dbo.Employees E 
        ON E.EmployeeId = A.EmployeeId
    WHERE
        (
            A.AttendanceDate >= CAST(DATEADD(hour, -{lookback_hours}, GETDATE()) AS DATE)
            OR A.InTime >= DATEADD(hour, -{lookback_hours}, GETDATE())
            OR A.OutTime >= DATEADD(hour, -{lookback_hours}, GETDATE())
        )
        AND E.AadhaarNumber IS NOT NULL
    GROUP BY
        A.EmployeeId,
        CAST(A.AttendanceDate AS DATE),
        E.AadhaarNumber
    """)
        rows = cursor.fetchall()
        if not rows:
            save_results(0, start_time)
            return

        # 6. REFRESH EMPLOYEE MAPS
        # Optimization: Fetch all needed employee data in one go
        employees = frappe.get_all(
            "Employee",
            fields=["name", "attendance_device_id", "custom_aadhaar_number", "date_of_joining"],
            ignore_permissions=True
        )
        
        device_map = {}
        aadhaar_map = {}
        
        for e in employees:
            raw_id = e.get("attendance_device_id")
            normalized_id = str(raw_id).strip() if raw_id is not None and str(raw_id).strip() != "" else None
            
            e_data = {
                "name": e.name,
                "joining": e.date_of_joining,
                "attendance_device_id": normalized_id
            }
            if normalized_id:
                device_map[normalized_id] = e_data
            if e.get("custom_aadhaar_number"):
                aadhaar_map[str(e.custom_aadhaar_number).strip()] = e_data

        # 7. BATCH CHECK EXISTENCE (Efficiency boost)
        unique_dates = list(set(getdate(r[1]) for r in rows))
        
        existing_logs = frappe.db.get_list("Attendance Log", {
            "attendance_date": ["in", unique_dates]
        }, ["employee_id", "attendance_date"], ignore_permissions=True)
        existing_logs_set = {(l.employee_id, getdate(l.attendance_date)) for l in existing_logs}
        
        existing_attendance = frappe.db.get_list("Attendance", {
            "attendance_date": ["in", unique_dates],
            "docstatus": ["!=", 2]
        }, ["employee", "attendance_date"], ignore_permissions=True)
        existing_att_set = {(a.employee, getdate(a.attendance_date)) for a in existing_attendance}

        # 8. PROCESS RECORDS
        for emp_id_raw, att_date, in_time_raw, out_time_raw, aadhaar_raw in rows:
            emp_id = str(emp_id_raw).strip()
            attendance_date = getdate(att_date)
            aadhaar_val = str(aadhaar_raw).strip() if aadhaar_raw else ""

            # Parse times — pyodbc may return strings or datetime objects depending on driver/config.
            # Pass datetime objects through directly; only call get_datetime() on strings.
            import datetime as _dt
            def _to_dt(val):
                if not val:
                    return None
                if isinstance(val, _dt.datetime):
                    return val
                try:
                    return get_datetime(val)
                except Exception:
                    return None
            in_time = _to_dt(in_time_raw)
            out_time = _to_dt(out_time_raw)

            # MATCHING: Priority Aadhaar > Device ID
            emp = aadhaar_map.get(aadhaar_val) or device_map.get(emp_id)
            if not emp:
                continue

            # 🛡️ STRICT "SET-ONCE" MAPPING logic
            # ONLY update the Employee record if they have NO device_id yet.
            # We check for [None, ""] to avoid treating 0 as missing.
            if emp.get("attendance_device_id") in [None, ""]:
                # Fresh DB check — the in-memory map might be stale if a previous
                # iteration already committed a mapping for this employee.
                current_device_id = frappe.db.get_value("Employee", emp["name"], "attendance_device_id")
                if current_device_id:
                    # Someone else (Phase 4 or a prior row) already set it — sync memory and skip.
                    emp["attendance_device_id"] = str(current_device_id).strip()
                    device_map[emp["attendance_device_id"]] = emp
                else:
                    # Collision check: make sure no OTHER employee already owns this bio_id.
                    existing_owner = frappe.db.get_value("Employee", {"attendance_device_id": emp_id}, "name")

                    if not existing_owner:
                        # ✅ First-time assignment — commit immediately so it survives any
                        # later exception/rollback within this sync run.
                        frappe.db.set_value("Employee", emp["name"], "attendance_device_id", emp_id, update_modified=False)
                        frappe.db.commit()
                        emp["attendance_device_id"] = emp_id  # Sync local memo
                        device_map[emp_id] = emp
                        frappe.log_error(f"Sync: Mapped {emp['name']} to ID {emp_id}", "Attendance Sync Mapping")
                    elif existing_owner == emp["name"]:
                        # Already set in DB (race condition), sync local memory only.
                        emp["attendance_device_id"] = emp_id
                        device_map[emp_id] = emp
            # Employee ALREADY HAS an ID (or was just assigned above). We NEVER overwrite it.

            # Skip before joining date or if invalid year
            # A. Attendance Log (Custom)
            if not frappe.db.get_value(
            "Attendance Log",
            {
                "employee_id": emp["name"],
                "attendance_date": attendance_date
            },
            "name"
        ):
                try:
                    frappe.get_doc({
                        "doctype": "Attendance Log",
                        "employee_id": emp["name"],
                        "attendance_date": attendance_date,
                        "in_time": in_time,
                        "out_time": out_time,
                        "in_device_id": emp_id
                    }).insert(ignore_permissions=True)
                    existing_logs_set.add((emp["name"], attendance_date))
                except Exception:
                    frappe.log_error(
                        f"Employee: {emp['name']} | Date: {attendance_date} | in_time: {in_time} | out_time: {out_time}\n"
                        + frappe.get_traceback(),
                        "Attendance Log Insert Failed"
                    )

            # B. Standard Attendance
            if not frappe.db.exists(
            "Attendance",
            {   
                "employee": emp["name"],
                "attendance_date": attendance_date,
                "docstatus": ["!=", 2]
            }
        ):
                try:
                    att = frappe.new_doc("Attendance")
                    att.employee = emp["name"]
                    att.attendance_date = attendance_date
                    att.status = "Present"
                    att.in_time = in_time
                    att.out_time = out_time
                    att.insert(ignore_permissions=True)
                    existing_att_set.add((emp["name"], attendance_date))
                except Exception:
                    frappe.log_error(
                        f"Employee: {emp['name']} | Date: {attendance_date} | in_time: {in_time} | out_time: {out_time}\n"
                        + frappe.get_traceback(),
                        "Attendance Insert Failed"
                    )

        # 9. SAVE RESULTS
        save_results(len(rows), start_time)
        frappe.db.commit()

    except Exception:
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), "Hourly Attendance Sync Failed")
    finally:
        if conn:
            conn.close()

def save_results(count, start_time):
    frappe.db.set_value("Job Configuration", "Job Configuration", {
        "records_pulled": count,
        "end_time": now_datetime()
    })

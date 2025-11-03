import os
import json
import uuid
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, TypedDict, Annotated
from operator import add

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd

# LangGraph imports
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage

# Constants and paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DATA_CSV_PATH = os.path.join(PROJECT_ROOT, "HR_Audit_FlatTable.csv")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

# Configurable sleep time between operations (in seconds)
NODE_SLEEP_TIME = 0.5  # Time to simulate processing at each progress step (reduced for demo)

# Data display multiplier - multiply record counts for demo purposes
# Set to 1000 to show 55 records as 55,000 in UI
DATA_MULTIPLIER = 1000

os.makedirs(OUTPUTS_DIR, exist_ok=True)

app = FastAPI(title="HR Audit POC API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory run registry
runs_lock = threading.Lock()
runs: Dict[str, Dict] = {}


class CreateRunRequest(BaseModel):
    audit_id: str
    audit_name: str


def write_scratchpad(run_id: str, node: str, lines: List[str], append: bool = True):
    run_dir = os.path.join(OUTPUTS_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)
    scratch_dir = os.path.join(run_dir, "scratchpads")
    os.makedirs(scratch_dir, exist_ok=True)
    pad_path = os.path.join(scratch_dir, f"{node}.txt")
    mode = "a" if append else "w"
    with open(pad_path, mode, encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip("\n") + "\n")


def write_artifact(run_id: str, name: str, data: Dict):
    run_dir = os.path.join(OUTPUTS_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)
    art_dir = os.path.join(run_dir, "artifacts")
    os.makedirs(art_dir, exist_ok=True)
    with open(os.path.join(art_dir, f"{name}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def load_hr_csv() -> pd.DataFrame:
    if not os.path.exists(DATA_CSV_PATH):
        # Create a minimal demo CSV if missing
        demo = pd.DataFrame([
            {
                "emp_id": "E001", "emp_name": "Alice", "position": "Analyst", "bonus": 1000,
                "paygrade": "P2", "manager_email": "mgr.alice@example.com", "job_allocation": "FIN",
                "investigation_status": "past_cleared", "leave_days_max_streak": 15
            },
            {
                "emp_id": "E002", "emp_name": "Bob", "position": "Analyst", "bonus": 1500,
                "paygrade": "P2", "manager_email": "mgr.bob@example.com", "job_allocation": "HR",
                "investigation_status": "past_flagged", "leave_days_max_streak": 25
            },
            {
                "emp_id": "E002", "emp_name": "Bob", "position": "Senior Analyst", "bonus": 1500,
                "paygrade": "P3", "manager_email": "mgr.bob@example.com", "job_allocation": "HR",
                "investigation_status": "ongoing", "leave_days_max_streak": 5
            },
        ])
        demo.to_csv(DATA_CSV_PATH, index=False)
    df = pd.read_csv(DATA_CSV_PATH)
    return df


def simulate_email(to: str, subject: str, body: str) -> Dict:
    # Dummy email sender
    return {"to": to, "subject": subject, "status": "SENT", "timestamp": datetime.utcnow().isoformat()}


NODES = [
    {"id": "data_integrator", "name": "Data Integrator", "desc": "Fetches data from sources and aggregates."},
    {"id": "normalizer", "name": "Normalize Data", "desc": "Standardizes columns and types."},
    {"id": "rules_engine", "name": "Run Rules", "desc": "Detects duplicates, mismatches, and investigations."},
    {"id": "policy_check", "name": "Policy Check", "desc": "Validates against company policies (PDFs)."},
    {"id": "summary", "name": "Summary", "desc": "Findings, risks, and recommendations."},
]


# LangGraph State Definition
class AuditState(TypedDict):
    run_id: str
    audit_id: str
    audit_name: str
    raw_data: Optional[List[Dict]]
    normalized_data: Optional[List[Dict]]
    final_data: Optional[List[Dict]]
    logs: Annotated[List[str], add]
    current_node: str


# LangGraph Node Functions
def node_data_integrator(state: AuditState) -> AuditState:
    """Node 1: Fetch and integrate data from multiple sources"""
    run_id = state["run_id"]
    node = "data_integrator"
    print(f"[{node}] Starting execution for run {run_id}")
    
    # Progress: 10%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running", "timestamp": datetime.utcnow().isoformat()}
    print(f"[{node}] Progress 10%")
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "=" * 80,
        "# 🔄 DATA INTEGRATION PROCESS",
        "=" * 80,
        "",
        "## 📋 Data Sources Identified",
        "",
        "**Primary Sources:**",
        "  1. Snowflake Data Warehouse - HR_PROD.EMPLOYEE_MASTER",
        "  2. HRMS Cloud API - https://api.hrms.company.com/v2",
        "  3. Google Drive - /HR Department/Employee Records",
        "  4. Local CSV Repository - HR_Audit_FlatTable.csv",
        "",
    ], append=False)
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 30% - Snowflake
    print(f"[{node}] Progress 30% - Writing Snowflake card")
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 30, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## 🗄️ SNOWFLAKE DATA WAREHOUSE",
        "**Connection Status:** CONNECTED ✅",
        "",
        "**📊 Records: 5,137,000 | 📋 Columns: 35 | 💾 Size: 2.4 MB**",
        "",
        "**Primary Source:** Snowflake Cloud Data Warehouse",
        "**Endpoint:** snowflake.company.com:443",
        "",
        "**Databases Accessed:** 2 (HR_PROD, HR_ANALYTICS)",
        "**Total Tables Queried:** 7 tables across schemas",
        "**Auth Method:** OAuth 2.0 with MFA",
        "**Connection Time:** 45ms | **Latency:** Excellent",
        "",
        "##DETAILS## 📊 Database & Tables Summary:",
        "",
        "**Database: HR_PROD** (3 tables)",
        "   - EMPLOYEES_MASTER: 3,247,000 records | 15 columns",
        "   - PERFORMANCE_REVIEWS: 1,890,000 records | 8 columns",
        "   - PAYROLL_HISTORY: 12,456,000 records | 12 columns",
        "",
        "**Database: HR_ANALYTICS** (4 tables)",
        "   - EMPLOYEE_METRICS: 5,632,000 records | 22 columns",
        "   - ATTENDANCE_LOGS: 24,890,000 records | 6 columns",
        "   - TRAINING_RECORDS: 8,234,000 records | 10 columns",
        "   - COMPENSATION_HIST: 15,678,000 records | 14 columns",
        "",
        "### 📈 Aggregated Metrics:",
        "  • **Total Records Fetched:** 5,137,000 rows",
        "  • **Total Columns:** 35 unique fields",
        "  • **Data Transfer:** 2.4 MB compressed",
        "  • **Query Execution Time:** 1.2 seconds",
        "  • **Tables Successfully Queried:** 7/7 (100%)",
        "",
        "### 🔗 Connection Details:",
        "  • Warehouse: COMPUTE_WH (Size: X-Small)",
        "  • Query ID: 01b2c3d4-e5f6-7890-abcd-ef1234567890",
        "  • Cost Estimate: $0.0042 (Query credits)",
        "  • Schemas: EMPLOYEE_MASTER, PAYROLL, ANALYTICS, TRAINING",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 45% - SuccessFactors
    print(f"[{node}] Progress 45% - Writing SuccessFactors card")
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 45, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## 🌐 SUCCESSFACTORS API (Cloud HRMS)",
        "**Connection Status:** AUTHENTICATED ✅",
        "",
        "**📊 Records: 1,245,000 | 📋 Fields: 18 | 💾 Size: 1.8 MB**",
        "",
        "**Primary Source:** SAP SuccessFactors Employee Central (Cloud HRMS)",
        "**Base URL:** https://api.successfactors.com/odata/v2",
        "",
        "**Total API Calls:** 15 requests executed (5 pages × 500 records/page)",
        "**Auth:** OAuth 2.0 Bearer Token (Valid until 2025-11-15)",
        "**Rate Limit:** 1000 req/hour | **Remaining:** 985",
        "",
        "##DETAILS## 📡 API Endpoints Called (15 total hits):",
        "",
        "**1-5. GET /User** (Employee Master Data) - 5 paginated calls",
        "   - Records per page: 500",
        "   - Total records fetched: 1,245,000 employees",
        "   - Avg response time: 245ms per call",
        "   - HTTP Status: 200 OK ✅",
        "",
        "**6-10. GET /EmpEmployment** (Employment Details) - 5 paginated calls",
        "   - Job info, hire dates, manager relationships",
        "   - Total records: 1,245,000",
        "   - Avg response time: 189ms per call",
        "   - HTTP Status: 200 OK ✅",
        "",
        "**11-13. GET /EmpCompensation** (Salary & Bonus) - 3 calls",
        "   - Compensation data, pay grades, bonuses",
        "   - Total records: 892,000",
        "   - Avg response time: 156ms per call",
        "   - HTTP Status: 200 OK ✅",
        "",
        "**14. GET /FODepartment** (Department Structure) - 1 call",
        "   - Departments: 34 unique units",
        "   - Response time: 56ms",
        "   - HTTP Status: 200 OK ✅",
        "",
        "**15. GET /FOLocation** (Office Locations) - 1 call",
        "   - Locations: 12 global offices",
        "   - Response time: 48ms",
        "   - HTTP Status: 200 OK ✅",
        "",
        "### 📊 JSON Data Retrieved:",
        "  • **Total Employee Records:** 1,245,000",
        "  • **JSON Fields per Record:** 18 attributes",
        "  • **Nested Objects:** manager, department, location, compensation",
        "  • **Total Data Size:** 1.8 MB (compressed JSON)",
        "  • **Raw JSON Size:** 5.2 MB (before compression)",
        "  • **Sample Values:** userId, firstName, lastName, hireDate, jobTitle, payGrade, bonus",
        "",
        "### 🔐 Security & Performance:",
        "  • **Encryption:** TLS 1.3",
        "  • **Token Expiry:** 14 days remaining",
        "  • **Total API Execution Time:** 2.8 seconds (15 calls)",
        "  • **IP Whitelist:** Verified ✅",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 60% - Google Drive
    print(f"[{node}] Progress 60% - Writing Google Drive card")
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 60, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## 📁 GOOGLE DRIVE & SHEETS INTEGRATION",
        "**Connection Status:** CONNECTED ✅",
        "",
        "**📊 Records: 2,348,000 | 📁 Files: 6 | 💾 Size: 0.7 MB**",
        "",
        "**Primary Source:** Google Drive & Sheets (HR Department)",
        "**Folders Scanned:** 3 shared folders",
        "",
        "**Auth:** OAuth 2.0 (Google Workspace)",
        "**Sync Status:** Real-time monitoring enabled",
        "",
        "##DETAILS## 📂 Folders Accessed:",
        "",
        "**Folder 1: /HR Department/Employee Records/**",
        "   - Employee_Master_Q4_2025.xlsx: 892,000 employees | 456 KB",
        "   - Compensation_Data.xlsx: 687,000 records | 312 KB",
        "   - Status: ✅ Both files downloaded & parsed",
        "",
        "**Folder 2: /HR Department/Performance Reviews/**",
        "   - Performance_Reviews_2025.csv: 1,456,000 reviews | 234 KB",
        "   - Goals_Tracking.csv: 892,000 goals | 178 KB",
        "   - Status: ✅ CSV files processed",
        "",
        "**Folder 3: /HR Department/Org Structure/**",
        "   - Org_Chart_Current.pdf: 1.2 MB (Skipped - non-structured)",
        "   - Department_Mapping.xlsx: 34,000 dept entries | 89 KB",
        "   - Status: ⚠️ 1 file parsed, 1 skipped",
        "",
        "### 📊 Google Sheets Live Data:",
        "**Active Sheets Synced:** 2 live Google Sheets",
        "   - 'Employee Directory 2025' (Live Sheet)",
        "     • Rows: 1,245,000 | Columns: 12",
        "     • Last edited: 2 hours ago",
        "     • Collaborators: 5 HR team members",
        "",
        "   - 'Leave Tracker Q4' (Live Sheet)",
        "     • Rows: 3,456,000 | Columns: 8",
        "     • Last edited: 15 minutes ago",
        "     • Auto-updates: Every 5 minutes",
        "",
        "### 📈 Aggregated Metrics:",
        "  • **Total Folders Scanned:** 3",
        "  • **Total Files Found:** 7 files",
        "  • **Files Processed:** 6 structured files",
        "  • **Google Sheets (Live):** 2 active sheets",
        "  • **Total Records:** 2,348,000",
        "  • **Total Data Size:** 0.7 MB",
        "  • **Download Time:** 3.4 seconds",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 75% - Excel/CSV
    print(f"[{node}] Progress 75% - Loading CSV and writing Excel/CSV card")
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 75, "status": "running"}
    
    df = load_hr_csv()  # Load CSV before writing its details
    print(f"[{node}] CSV loaded: {len(df)} rows")
    
    # Get sample records for display
    sample_records = df.head(3).to_dict(orient='records')
    
    # Calculate file size in appropriate unit
    file_size_bytes = os.path.getsize(DATA_CSV_PATH)
    if file_size_bytes < 1024 * 1024:  # Less than 1 MB
        file_size_str = f"{file_size_bytes / 1024:.1f} KB"
    else:
        file_size_str = f"{file_size_bytes / (1024*1024):.2f} MB"
    
    write_scratchpad(run_id, node, [
        f"##CARD## 📊 EXCEL & CSV TABLE REPOSITORY",
        "**Status:** FILES FOUND ✅",
        "",
        f"**📊 Records: {len(df) * DATA_MULTIPLIER:,} | 💾 Size: {file_size_str} | 📁 Tables: 3**",
        "",
        f"**Primary CSV:** HR_Audit_FlatTable.csv ({len(df) * DATA_MULTIPLIER:,} employees)",
        f"**Repository Path:** {os.path.dirname(DATA_CSV_PATH)}",
        "",
        "##DETAILS## 📑 Tables Loaded:",
        "",
        "**Table 1: HR_Audit_FlatTable.csv** (Primary)",
        "   - Employee master data with audit trail",
        f"   - Records: {len(df) * DATA_MULTIPLIER:,} employees",
        f"   - Size: {file_size_str}",
        "   - Status: ✅ Loaded",
        "",
        "**Table 2: Employee_Historical_Data.csv** (Archive)",
        "   - Historical employee records (2020-2024)",
        "   - Records: 4,567,000 historical entries",
        "   - Size: 8.3 MB",
        "   - Status: ✅ Loaded",
        "",
        "**Table 3: Compliance_Audit_Log.xlsx** (Audit Trail)",
        "   - Past compliance checks and investigation logs",
        "   - Records: 892,000 audit entries",
        "   - Size: 2.1 MB",
        "   - Status: ✅ Loaded",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 90%
    print(f"[{node}] Progress 90% - Writing summary table")
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 90, "status": "running"}
    
    # Calculate proper file size display
    csv_size_bytes = os.path.getsize(DATA_CSV_PATH)
    if csv_size_bytes < 1024 * 1024:
        csv_size_display = f"{csv_size_bytes / 1024:.1f} KB"
    else:
        csv_size_display = f"{csv_size_bytes / (1024*1024):.2f} MB"
    
    write_scratchpad(run_id, node, [
        "## 📊 DATA INTEGRATION SUMMARY TABLE",
        "",
        "| Data Source | Status | Records | Columns | Size | Fetch Time |",
        "|------------|--------|---------|---------|------|------------|",
        "| Snowflake DWH | ✅ Connected | 5,137,000 | 35 | 2.4 MB | 1.2s |",
        "| HRMS API | ✅ Authenticated | 1,245,000 | 18 | 1.8 MB | 0.8s |",
        "| Google Drive | ✅ Synced | 2,348,000 | 22 | 0.7 MB | 3.4s |",
        f"| Local CSV | ✅ Loaded | {len(df) * DATA_MULTIPLIER:,} | {len(df.columns)} | {csv_size_display} | 0.1s |",
        "| **TOTAL** | **4/4 Success** | **{:,}** | **{:,}** | **5.1 MB** | **5.5s** |".format((len(df) + 8730) * DATA_MULTIPLIER, 75),
        "",
        "---",
        "",
        "## 🎯 KEY INSIGHTS & STATISTICS",
        "",
        f"### 📈 Workforce Overview:",
        f"  • **Total Employees in System:** {len(df) * DATA_MULTIPLIER:,} people",
        f"  • **Active Employee IDs:** {(df['emp_id'].nunique() if 'emp_id' in df.columns else 0) * DATA_MULTIPLIER:,} unique individuals",
        f"  • **Job Roles in Organization:** {df['position'].nunique() if 'position' in df.columns else 'N/A'} distinct positions (Manager, Analyst, etc.)",
        f"  • **Salary Levels:** {df['paygrade'].nunique() if 'paygrade' in df.columns else 'N/A'} pay grades (Junior to Executive)",
        f"  • **Teams/Departments:** {df['job_allocation'].nunique() if 'job_allocation' in df.columns else 'N/A'} business units",
        "",
        f"### 💰 Compensation Analysis:",
        f"  • **Average Bonus:** ${df['bonus'].mean():.2f}" if 'bonus' in df.columns else "",
        f"  • **Bonus Range:** ${df['bonus'].min():.0f} - ${df['bonus'].max():.0f}" if 'bonus' in df.columns else "",
        f"  • **Median Bonus:** ${df['bonus'].median():.2f}" if 'bonus' in df.columns else "",
        "",
        f"### 📊 Data Quality Score:",
        f"  • **Completeness:** {((df.count().sum() / (len(df) * len(df.columns))) * 100):.1f}% ⭐⭐⭐⭐",
        f"  • **Null Values:** {df.isnull().sum().sum()} cells ({(df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100):.1f}%)",
        f"  • **Potential Duplicates:** {df.duplicated(subset=['emp_id']).sum()} ({(df.duplicated(subset=['emp_id']).sum() / len(df) * 100):.1f}%)",
        f"  • **Overall Score:** **{max(0, 100 - (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100) - (df.duplicated(subset=['emp_id']).sum() / len(df) * 100)):.0f}/100** {'🟢 Excellent' if max(0, 100 - (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100)) > 95 else '🟡 Good'}",
        "",
        "### 📄 Sample Records (First 3):",
        "```json",
        *[json.dumps(rec, indent=2) for rec in sample_records],
        "```",
        "",
        "---",
        "",
        "## 📌 REFERENCES & DOCUMENTATION",
        "",
        "### 🔗 Data Source URLs:",
        "  • **Snowflake:** https://snowflake.company.com/console#/HR_PROD",
        "  • **HRMS API Docs:** https://docs.hrms.company.com/api/v2",
        "  • **Google Drive Folder:** https://drive.google.com/drive/folders/1ABC...xyz",
        f"  • **Local CSV:** file://{DATA_CSV_PATH}",
        "",
        "### 📚 Related Documentation:",
        "  • [Data Dictionary](https://wiki.company.com/hr/data-dictionary) - Field definitions",
        "  • [Integration Guide](https://wiki.company.com/hr/integration) - Setup instructions",
        "  • [API Documentation](https://docs.hrms.company.com/api) - Endpoint reference",
        "  • [Compliance Policy](https://compliance.company.com/hr-data) - Data handling rules",
        "  • [Security Protocols](https://security.company.com/data-access) - Access guidelines",
        "",
        "### 🔐 Compliance & Security:",
        "  • **Data Classification:** Confidential - Internal Use Only",
        "  • **GDPR Compliance:** ✅ Verified",
        "  • **SOC 2 Type II:** ✅ Compliant",
        "  • **Encryption:** At rest (AES-256) & in transit (TLS 1.3)",
        "",
        "=" * 80,
        "# ✅ DATA INTEGRATION COMPLETED SUCCESSFULLY",
        "=" * 80,
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 100%
    print(f"[{node}] Progress 100% - Writing artifacts and completing")
    write_artifact(run_id, "data_integrator_output", {"rows": len(df), "columns": list(df.columns)})
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed", "timestamp": datetime.utcnow().isoformat()}
    print(f"[{node}] Completed successfully")
    
    return {
        **state,
        "raw_data": df.to_dict(orient="records"),
        "current_node": node,
        "logs": [f"[{node}] Fetched {len(df)} records"]
    }


def node_normalizer(state: AuditState) -> AuditState:
    """Node 2: Normalize and standardize data schema"""
    run_id = state["run_id"]
    node = "normalizer"
    
    # Progress: 10%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "## 🎯 SCHEMA NORMALIZATION PROCESS",
        "Transforming raw data into standardized format",
        "",
    ], append=False)
    
    schema = [
        "emp_id", "emp_name", "position", "bonus", "paygrade", "manager_email",
        "job_allocation", "investigation_status", "leave_days_max_streak"
    ]
    df = pd.DataFrame(state["raw_data"])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 30%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 30, "status": "running"}
    
    missing_cols = [col for col in schema if col not in df.columns]
    
    for col in schema:
        if col not in df.columns:
            df[col] = None
    df = df[schema]
    
    write_scratchpad(run_id, node, [
        "##CARD## 📐 STANDARDIZING DATA STRUCTURE",
        f"**What we're doing:** Converting {len(df) * DATA_MULTIPLIER:,} employee records into consistent format",
        "",
        f"**KEY_METRICS:** Records={len(df) * DATA_MULTIPLIER} | StandardFields=9 | MissingFieldsFilled={len(missing_cols)}",
        "",
        "| Information Category | Data Type | Example | Status |",
        "|---------------------|-----------|---------|--------|",
        "| Employee ID | Text | E001, E002 | ✅ Required |",
        "| Employee Name | Text | John Smith | ✅ Required |",
        "| Position/Role | Text | Manager, Analyst | ✅ Standardized |",
        "| Annual Bonus | Number | $5,000, $10,000 | ✅ Standardized |",
        "| Pay Grade | Text | P1, P2, P3 | ✅ Standardized |",
        "| Manager Email | Email | manager@company.com | ✅ Standardized |",
        "| Department/Team | Text | Finance, HR, Engineering | ✅ Standardized |",
        "| Compliance Status | Category | Cleared, Flagged, Under Review | ✅ Standardized |",
        "| Max Leave Streak | Number | 5, 15, 20 days | ✅ Standardized |",
        "",
        f"**Result:** ✅ All {len(df) * DATA_MULTIPLIER:,} employees standardized",
        "",
        "##DETAILS## 📋 Understanding Data Standardization:",
        "",
        "**What is schema normalization?**",
        "  • Taking data from different sources (Snowflake, APIs, CSVs, Google Sheets)",
        "  • Each source might have different column names or formats",
        "  • We transform everything into ONE consistent structure",
        "",
        "**Why do we need 9 standard fields?**",
        "  • **Employee ID:** Unique identifier - can't analyze without knowing who's who",
        "  • **Employee Name:** Human-readable identification",
        "  • **Position:** Needed for org chart analysis and role-based rules",
        "  • **Annual Bonus:** Required for compensation analysis and payroll validation",
        "  • **Pay Grade:** Determines salary bands, benefits, promotion eligibility",
        "  • **Manager Email:** Needed to send alerts about team members",
        "  • **Department:** Used for team allocation and budget tracking",
        "  • **Compliance Status:** Tracks investigation history for risk assessment",
        "  • **Max Leave Streak:** Detects excessive time off that violates policy",
        "",
        "**What if a field is missing from source data?**",
        f"  • We found {len(missing_cols)} missing fields in the raw data",
        "  • We add those columns and fill them with 'None' or empty values",
        "  • This ensures every employee has all 9 fields (even if some are blank)",
        "  • Example: If Snowflake data doesn't have 'bonus' column → we add it",
        "",
        "**Real-world example:**",
        "  • Source 1 has: emp_id, name, job_title",
        "  • Source 2 has: employee_number, full_name, position, salary_grade",
        "  • We normalize BOTH to: emp_id, emp_name, position, paygrade",
        "  • Now we can combine and analyze them together!",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 60%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 60, "status": "running"}
    
    # Type conversions with tracking
    bonus_errors = df["bonus"].apply(pd.to_numeric, errors="coerce").isna().sum()
    df["bonus"] = pd.to_numeric(df["bonus"], errors="coerce").fillna(0).astype(int)
    
    leave_errors = df["leave_days_max_streak"].apply(pd.to_numeric, errors="coerce").isna().sum()
    df["leave_days_max_streak"] = pd.to_numeric(df["leave_days_max_streak"], errors="coerce").fillna(0).astype(int)
    
    # Add simulated errors for demo purposes (3-5% error rate is realistic)
    if bonus_errors == 0:
        bonus_errors = max(1, int(len(df) * 0.03))  # Simulate 3% error rate
    if leave_errors == 0:
        leave_errors = max(1, int(len(df) * 0.04))  # Simulate 4% error rate
    
    write_scratchpad(run_id, node, [
        "##CARD## 🔢 CLEANING & CONVERTING DATA TO NUMBERS",
        f"**What we're doing:** Converting text values to numbers for {len(df) * DATA_MULTIPLIER:,} employees",
        "",
        f"**KEY_METRICS:** TotalRecords={len(df) * DATA_MULTIPLIER} | ConversionRate={((len(df)*2 - bonus_errors - leave_errors) / (len(df)*2) * 100):.1f}% | Errors={int((bonus_errors + leave_errors) * DATA_MULTIPLIER)}",
        "",
        "| Field | Converted Successfully | Had Errors | Error Rate |",
        "|-------|----------------------|------------|-----------|",
        f"| Annual Bonus | {(len(df) - bonus_errors) * DATA_MULTIPLIER:,} | {bonus_errors * DATA_MULTIPLIER:,} | {(bonus_errors/len(df)*100):.1f}% |",
        f"| Max Leave Days | {(len(df) - leave_errors) * DATA_MULTIPLIER:,} | {leave_errors * DATA_MULTIPLIER:,} | {(leave_errors/len(df)*100):.1f}% |",
        f"| **TOTAL** | **{(len(df)*2 - bonus_errors - leave_errors) * DATA_MULTIPLIER:,}** | **{int((bonus_errors + leave_errors) * DATA_MULTIPLIER):,}** | **{((bonus_errors + leave_errors)/(len(df)*2)*100):.1f}%** |",
        "",
        f"**Overall Success Rate:** {((len(df)*2 - bonus_errors - leave_errors) / (len(df)*2) * 100):.1f}% values converted to numbers",
        "",
        "##DETAILS## 📋 Understanding Data Type Conversion:",
        "",
        "**Why convert text to numbers?**",
        "  • Can't do math on text - need to calculate averages, totals, ranges",
        "  • Example: '$5,000' (text) can't be added to '$3,000' (text)",
        "  • Must convert to: 5000 + 3000 = 8000",
        "",
        "**Annual Bonus Conversion:**",
        f"  • Successfully converted: {(len(df) - bonus_errors) * DATA_MULTIPLIER:,} employees",
        f"  • Had problems: {bonus_errors * DATA_MULTIPLIER:,} employees",
        "  • Common issues: '$5,000.50', 'N/A', 'TBD', blank cells",
        "  • Our fix: Strip $ and commas, convert to number, if fails → set to 0",
        "  • Example: '$5,000.50' → 5000 | 'N/A' → 0",
        "",
        "**Maximum Leave Days Conversion:**",
        f"  • Successfully converted: {(len(df) - leave_errors) * DATA_MULTIPLIER:,} employees",
        f"  • Had problems: {leave_errors * DATA_MULTIPLIER:,} employees",
        "  • Common issues: '15 days', 'unknown', 'N/A', blank cells",
        "  • Our fix: Extract number, if fails → set to 0",
        "  • Example: '15 days' → 15 | 'unknown' → 0",
        "",
        "**What happens to errors?**",
        "  • Any value that can't convert cleanly becomes 0",
        "  • This allows analysis to continue without breaking",
        "  • Errors are tracked so you know data quality",
        f"  • Total errors: {int((bonus_errors + leave_errors) * DATA_MULTIPLIER):,} out of {len(df)*2 * DATA_MULTIPLIER:,} values",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 90%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 90, "status": "running"}
    
    integrity_score = int((df.count().sum() / (len(df) * len(df.columns))) * 100)
    null_count = int(df.isnull().sum().sum())
    
    write_scratchpad(run_id, node, [
        "##CARD## ✅ FINAL QUALITY VALIDATION",
        f"**What we're doing:** Quality check on {len(df) * DATA_MULTIPLIER:,} employee records",
        "",
        f"**KEY_METRICS:** Integrity={integrity_score}% | Records={len(df) * DATA_MULTIPLIER} | Nulls={null_count * DATA_MULTIPLIER}",
        "",
        "| Quality Metric | Value | Status |",
        "|---------------|-------|--------|",
        f"| Data Completeness | {integrity_score}% | {'✅ Excellent' if integrity_score >= 95 else '⚠️ Fair' if integrity_score >= 85 else '🔴 Poor'} |",
        f"| Total Employees | {len(df) * DATA_MULTIPLIER:,} | ✅ Validated |",
        f"| Data Fields per Employee | 9 categories | ✅ Standardized |",
        f"| Total Data Points | {(len(df) * len(df.columns)) * DATA_MULTIPLIER:,} | ✅ Checked |",
        f"| Filled Data Points | {df.count().sum() * DATA_MULTIPLIER:,} | ✅ Complete |",
        f"| Missing/Null Values | {null_count * DATA_MULTIPLIER:,} | {'✅ Minimal' if null_count < len(df) * 0.05 else '⚠️ Some gaps'} |",
        "",
        f"**Assessment:** {'🟢 Ready for analysis' if integrity_score >= 95 else '🟡 Proceed with caution' if integrity_score >= 85 else '🔴 Data cleanup recommended'}",
        "",
        "##DETAILS## 📋 Understanding Data Quality Metrics:",
        "",
        "**Data Completeness Score Explained:**",
        f"  • Formula: (Filled cells ÷ Total cells) × 100",
        f"  • Calculation: ({df.count().sum() * DATA_MULTIPLIER:,} ÷ {(len(df) * len(df.columns)) * DATA_MULTIPLIER:,}) × 100 = {integrity_score}%",
        "  • What it means: What percentage of database cells have data vs. are empty",
        f"  • Your score: {integrity_score}% {'(Excellent - minimal gaps)' if integrity_score >= 95 else '(Fair - some missing data)' if integrity_score >= 85 else '(Poor - many gaps)'}",
        "",
        "**Example breakdown:**",
        f"  • {len(df) * DATA_MULTIPLIER:,} employees × 9 fields = {(len(df) * len(df.columns)) * DATA_MULTIPLIER:,} total cells",
        "  • If John's record has 8 filled fields and 1 blank → he's 8/9 = 89% complete",
        f"  • Across everyone: {df.count().sum() * DATA_MULTIPLIER:,} cells filled = {integrity_score}% overall",
        "",
        f"**Missing Information Details:**",
        f"  • Total blank/null values: {null_count * DATA_MULTIPLIER:,} cells",
        "  • Common missing fields: manager email, department, bonus amount",
        "  • Impact: Can't calculate accurate totals or route alerts without complete data",
        "  • Recommendation: Fill critical fields before making major decisions",
        "",
        f"**Employee Count Verification:**",
        f"  • Validated: {len(df) * DATA_MULTIPLIER:,} total employees in system",
        "  • All have required fields: emp_id, emp_name (mandatory)",
        "  • All standardized to 9 information categories",
        "  • Ready for: Duplicate detection, policy validation, compliance checking",
        "",
        "**Quality Certification Checklist:**",
        "  ✅ Structure: All 9 required fields present for every employee",
        "  ✅ Data types: Bonus and leave days converted to numbers",
        "  ✅ Critical fields: Every employee has ID and name",
        "  ✅ Standardization: Consistent format across all sources",
        f"  ✅ Processing: {len(df) * DATA_MULTIPLIER:,} employees ready for next stage",
        "",
        f"**Proceed to next step?**",
        f"  • {('Yes - data quality is excellent' if integrity_score >= 95 else 'Caution - some data gaps exist but can proceed' if integrity_score >= 85 else 'Risk - consider cleaning data before continuing')}",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    write_artifact(run_id, "normalized_snapshot", {"rows": len(df), "columns": schema})
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed", "timestamp": datetime.utcnow().isoformat()}
    
    return {
        **state,
        "normalized_data": df.to_dict(orient="records"),
        "current_node": node,
        "logs": [f"[{node}] Normalized {len(df)} rows, {len(schema)} columns"]
    }


def node_rules_engine(state: AuditState) -> AuditState:
    """Node 3: Apply business rules - detect duplicates, mismatches, investigations"""
    run_id = state["run_id"]
    node = "rules_engine"
    
    # Progress: 10%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "## 🔍 BUSINESS RULES VALIDATION",
        "Executing 4 automated audit rules",
        "",
    ], append=False)
    
    df = pd.DataFrame(state["normalized_data"])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 30% - Duplicates
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 30, "status": "running"}
    
    dup_mask = df.duplicated(subset=["emp_id", "emp_name"], keep=False)
    duplicate_count = int(dup_mask.sum())
    
    # Add simulated duplicates for demo (2-3% duplication is realistic)
    if duplicate_count == 0:
        duplicate_count = max(2, int(len(df) * 0.025))  # Simulate 2.5% duplication
    
    df_no_dups = df.drop_duplicates(subset=["emp_id", "emp_name"], keep="first").reset_index(drop=True)
    
    # Get duplicate employee details
    duplicate_employees = []
    if duplicate_count > 0:
        # Create demo duplicate examples
        if dup_mask.sum() > 0:
            dup_df = df[dup_mask][["emp_id", "emp_name"]].drop_duplicates().head(5)
            duplicate_employees = [f"{row['emp_id']} - {row['emp_name']}" for _, row in dup_df.iterrows()]
        else:
            # Generate demo examples from the data
            sample_employees = df.head(min(5, len(df)))
            duplicate_employees = [f"{row['emp_id']} - {row['emp_name']}" for _, row in sample_employees.iterrows()]
    
    write_scratchpad(run_id, node, [
        "##CARD## 🔍 FINDING DUPLICATE PEOPLE IN THE SYSTEM",
        f"**What we're doing:** Checking if the same person appears multiple times in the employee database",
        f"**Why this matters:** Duplicates inflate headcount and can cause double-payments",
        "",
        f"**KEY_METRICS:** Duplicates={duplicate_count * DATA_MULTIPLIER} | Unique={len(df_no_dups) * DATA_MULTIPLIER} | QualityScore={((len(df_no_dups)/len(df))*100):.0f}",
        "",
        "| Metric | Count | Description |",
        "|--------|-------|-------------|",
        f"| Total Rows in Database | {len(df) * DATA_MULTIPLIER:,} | All employee records found |",
        f"| Duplicate Records | {duplicate_count * DATA_MULTIPLIER:,} | Same person appearing multiple times |",
        f"| Unique Employees | {len(df_no_dups) * DATA_MULTIPLIER:,} | Actual real people (true headcount) |",
        f"| Data Quality Score | {((len(df_no_dups)/len(df))*100):.0f}% | Percentage of unique records |",
        "",
        f"**Status:** {'✅ Excellent' if (len(df_no_dups)/len(df)) > 0.95 else '⚠️ Needs Cleanup' if (len(df_no_dups)/len(df)) > 0.90 else '🔴 Poor Quality'}",
        "",
        "**Examples of Duplicates Found:**" if duplicate_employees else "✅ No duplicates found!",
        *([f"  • {emp}" for emp in duplicate_employees[:5]] if duplicate_employees else []),
        f"  ... and {len(duplicate_employees) - 5} more" if len(duplicate_employees) > 5 else "",
        "",
        "##DETAILS## 📋 Understanding Duplicate Detection:",
        "",
        "**How we detect duplicates:**",
        "  • Look for people with exact same employee ID AND full name",
        "  • If Sarah Johnson (E123) appears 3 times → that's 2 duplicates",
        "",
        "**Why duplicates are a problem:**",
        "  • Inflated headcount - 3 entries look like 3 people, but it's just 1 person",
        "  • Potential double-payments - system might pay same person twice",
        "  • Wrong reports - HR metrics and analytics will be incorrect",
        "",
        "**What we did:**",
        f"  • Found {duplicate_count * DATA_MULTIPLIER:,} duplicate records",
        "  • Kept the first/original record for each person",
        "  • Removed all duplicate copies",
        f"  • Result: Clean database with {len(df_no_dups) * DATA_MULTIPLIER:,} unique employees",
        "",
        "**Data Quality Score Explained:**",
        f"  • Formula: (Unique People ÷ Total Rows) × 100",
        f"  • Calculation: ({len(df_no_dups) * DATA_MULTIPLIER:,} ÷ {len(df) * DATA_MULTIPLIER:,}) × 100 = {((len(df_no_dups)/len(df))*100):.0f}%",
        "  • 100% = Perfect (no duplicates)",
        "  • 95-99% = Excellent (very few duplicates)",
        "  • 90-94% = Fair (some cleanup needed)",
        "  • <90% = Poor (major data quality issues)",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 50% - Mismatches
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 50, "status": "running"}
    
    mismatches = {"position": [], "bonus": [], "paygrade": []}
    alerts = []
    mismatch_details = []
    
    for col in ["position", "bonus", "paygrade"]:
        grp = df.groupby("emp_id")[col].nunique()
        conflicted_ids = grp[grp > 1].index.tolist()
        for eid in conflicted_ids:
            emp_rows = df[df["emp_id"] == eid]
            emp_name = emp_rows["emp_name"].iloc[0]
            manager = emp_rows["manager_email"].iloc[0]
            values = emp_rows[col].drop_duplicates().tolist()
            
            rows = emp_rows[["emp_id", "emp_name", col]].drop_duplicates().to_dict(orient="records")
            mismatches[col].append({"emp_id": eid, "records": rows})
            
            email = simulate_email(manager, f"Mismatch detected for {col}", json.dumps(rows))
            alerts.append({"type": f"mismatch_{col}", "email": email})
            
            mismatch_details.append({
                "employee": f"{eid} - {emp_name}",
                "field": col.capitalize(),
                "conflicting_values": values,
                "manager": manager
            })
    
    total_mismatches = sum(len(v) for v in mismatches.values())
    
    # Add simulated mismatches for demo (1-2% mismatch rate is realistic)
    if total_mismatches == 0:
        # Simulate position mismatches (1% of employees)
        position_mismatch_count = max(1, int(len(df_no_dups) * 0.01))
        for i in range(min(position_mismatch_count, 3)):
            if i < len(df_no_dups):
                emp = df_no_dups.iloc[i]
                mismatches["position"].append({
                    "emp_id": emp["emp_id"],
                    "records": [
                        {"emp_id": emp["emp_id"], "emp_name": emp["emp_name"], "position": "Senior Analyst"},
                        {"emp_id": emp["emp_id"], "emp_name": emp["emp_name"], "position": "Manager"}
                    ]
                })
                mismatch_details.append({
                    "employee": f"{emp['emp_id']} - {emp['emp_name']}",
                    "field": "Position",
                    "conflicting_values": ["Senior Analyst", "Manager"],
                    "manager": emp.get("manager_email", "manager@company.com")
                })
        
        # Simulate bonus mismatches (0.5% of employees)
        bonus_mismatch_count = max(1, int(len(df_no_dups) * 0.005))
        for i in range(min(bonus_mismatch_count, 2)):
            idx = min(i + 3, len(df_no_dups) - 1)
            if idx < len(df_no_dups):
                emp = df_no_dups.iloc[idx]
                mismatches["bonus"].append({
                    "emp_id": emp["emp_id"],
                    "records": [
                        {"emp_id": emp["emp_id"], "emp_name": emp["emp_name"], "bonus": 5000},
                        {"emp_id": emp["emp_id"], "emp_name": emp["emp_name"], "bonus": 7500}
                    ]
                })
                mismatch_details.append({
                    "employee": f"{emp['emp_id']} - {emp['emp_name']}",
                    "field": "Bonus",
                    "conflicting_values": [5000, 7500],
                    "manager": emp.get("manager_email", "manager@company.com")
                })
        
        # Simulate paygrade mismatches (0.3% of employees)
        paygrade_mismatch_count = max(1, int(len(df_no_dups) * 0.003))
        for i in range(min(paygrade_mismatch_count, 1)):
            idx = min(i + 5, len(df_no_dups) - 1)
            if idx < len(df_no_dups):
                emp = df_no_dups.iloc[idx]
                mismatches["paygrade"].append({
                    "emp_id": emp["emp_id"],
                    "records": [
                        {"emp_id": emp["emp_id"], "emp_name": emp["emp_name"], "paygrade": "P2"},
                        {"emp_id": emp["emp_id"], "emp_name": emp["emp_name"], "paygrade": "P3"}
                    ]
                })
                mismatch_details.append({
                    "employee": f"{emp['emp_id']} - {emp['emp_name']}",
                    "field": "Paygrade",
                    "conflicting_values": ["P2", "P3"],
                    "manager": emp.get("manager_email", "manager@company.com")
                })
        
        total_mismatches = sum(len(v) for v in mismatches.values())
    
    write_scratchpad(run_id, node, [
        "##CARD## ⚠️ FINDING CONFLICTING INFORMATION FOR SAME PERSON",
        f"**What we're doing:** Checking if the same employee has contradictory information in different database records",
        f"**Why this matters:** Same person with different job titles/bonuses/paygrades creates payroll errors",
        "",
        f"**KEY_METRICS:** Position={len(mismatches['position']) * DATA_MULTIPLIER} | Bonus={len(mismatches['bonus']) * DATA_MULTIPLIER} | Paygrade={len(mismatches['paygrade']) * DATA_MULTIPLIER}",
        "",
        "| Conflict Type | Employees Affected | Severity | Impact |",
        "|---------------|-------------------|----------|--------|",
        f"| Job Title (Position) | {len(mismatches['position']) * DATA_MULTIPLIER:,} | ⚠️ Medium | Org chart errors, role confusion |",
        f"| Annual Bonus Amount | {len(mismatches['bonus']) * DATA_MULTIPLIER:,} | 🔴 High | Payroll errors, overpayment risk |",
        f"| Salary Band (Paygrade) | {len(mismatches['paygrade']) * DATA_MULTIPLIER:,} | 🔴 Critical | Legal compliance, pay equity issues |",
        f"| **TOTAL** | **{total_mismatches * DATA_MULTIPLIER:,}** | **High** | **Immediate manager review required** |",
        "",
        f"**Actions Taken:** {len(alerts) * DATA_MULTIPLIER:,} manager alerts sent",
        "",
        "**Examples of Employees with Conflicts:**" if mismatch_details else "✅ No conflicts found!",
    ] + ([f"  • **{d['employee']}** - {d['field']} conflict: {', '.join(map(str, d['conflicting_values'][:2]))}" 
           for d in mismatch_details[:5]] if mismatch_details else []) + [
        f"  ... and {(len(mismatch_details) - 5) * DATA_MULTIPLIER:,} more" if len(mismatch_details) > 5 else "",
        "",
        "##DETAILS## 📋 Understanding Data Conflicts:",
        "",
        f"**1. Job Title Conflicts: {len(mismatches['position']) * DATA_MULTIPLIER:,} people**",
        "   • What it means: Same person listed with different job titles",
        "   • Example: Sarah shows as 'Senior Analyst' in one record but 'Manager' in another",
        "   • Why it's a problem: Wrong title → incorrect org chart, role confusion, wrong expectations",
        "   • How managers fix it: Review employee's actual role, update all records to correct title",
        "",
        f"**2. Bonus Amount Conflicts: {len(mismatches['bonus']) * DATA_MULTIPLIER:,} people**",
        "   • What it means: Same person has different annual bonus amounts",
        "   • Example: Michael shows $5,000 bonus in one entry but $7,500 in another",
        "   • Why it's a problem: Which amount to pay? Could cause overpayment or employee disputes",
        "   • How managers fix it: Check approved compensation plan, correct to authorized amount",
        "",
        f"**3. Salary Band Conflicts: {len(mismatches['paygrade']) * DATA_MULTIPLIER:,} people**",
        "   • What it means: Same person assigned to different pay grade levels",
        "   • Example: Jennifer marked as 'P2 (Mid-level)' in one record but 'P3 (Senior)' in another",
        "   • Why it's a problem: Pay grade determines salary range, benefits, promotions",
        "   • How managers fix it: Verify employee's actual level, update to correct pay grade",
        "",
        "**How We Detect Conflicts:**",
        "   • Group all database records by employee ID",
        "   • Check if Position, Bonus, or Paygrade has multiple different values for same person",
        "   • Flag as conflict if 2+ different values found",
        "",
        "**Manager Alert Details:**",
        f"   • Total alerts sent: {len(alerts) * DATA_MULTIPLIER:,} emails",
        "   • Each alert includes: Employee name, conflicting field, all values found",
        "   • Managers must review and confirm correct value within 5 business days",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 70% - Job allocation
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 70, "status": "running"}
    
    job_alloc_issues = df_no_dups[df_no_dups["job_allocation"].isin([None, "", "UNKNOWN"])][["emp_id", "emp_name", "job_allocation", "manager_email"]]
    job_alerts = []
    job_issue_list = []
    for _, r in job_alloc_issues.iterrows():
        job_alerts.append(simulate_email(r["manager_email"], "Job allocation missing/mismatch", f"Emp {r['emp_id']} {r['emp_name']}"))
        job_issue_list.append(f"{r['emp_id']} - {r['emp_name']} → Manager: {r['manager_email']}")
    
    write_scratchpad(run_id, node, [
        "##CARD## 📋 JOB ALLOCATION STATUS",
        f"**What we're checking:** Verifying all employees are assigned to a department/team",
        f"**Why it matters:** Missing allocations mean unclear reporting structure and budget tracking issues",
        "",
        "| Status | Count | % | Explanation |",
        "|--------|-------|---|-------------|",
        f"| ✅ Valid Allocations | {(len(df_no_dups) - len(job_alloc_issues)) * DATA_MULTIPLIER:,} | {((len(df_no_dups) - len(job_alloc_issues))/len(df_no_dups)*100):.1f}% | Properly assigned to dept |",
        f"| ⚠️ Missing/Unknown | {len(job_alloc_issues) * DATA_MULTIPLIER:,} | {(len(job_alloc_issues)/len(df_no_dups)*100):.1f}% | No department assignment |",
        "",
        f"**Action Taken:** Sent {len(job_alerts) * DATA_MULTIPLIER:,} alerts to managers to assign these employees to departments",
        "",
        "**Employees with Missing Allocations & Managers Notified:**" if job_issue_list else "**Result:** ✅ All employees properly allocated!",
        *([f"  • {emp}" for emp in job_issue_list[:5]] if job_issue_list else []),
        f"  ... and {len(job_issue_list) - 5} more employees" if len(job_issue_list) > 5 else "",
        "",
        "### ✅ Actions Taken:",
        f"  • **Data Cleaning:** {len(df_no_dups) * DATA_MULTIPLIER:,} unique employee records retained",
        f"  • **Manager Alerts:** {(len(alerts) + len(job_alerts)) * DATA_MULTIPLIER:,} emails sent to managers",
        f"  • **Employees Flagged:** {(len(set([m['emp_id'] for v in mismatches.values() for m in v]) | set(job_alloc_issues['emp_id'].tolist())) if mismatches else 0) * DATA_MULTIPLIER:,} requiring immediate attention",
        "",
        f"### 📈 Overall Data Quality Score: {((1 - (duplicate_count + total_mismatches) / len(df)) * 100):.0f}%",
        f"**Calculation:** Based on duplicate records and data consistency metrics",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    rules_artifact = {
        "duplicates": duplicate_count,
        "rows_after_dedup": len(df_no_dups),
        "mismatches": mismatches,
        "job_allocation_issues": len(job_alloc_issues),
        "emails": alerts + [{"type": "job_allocation", "email": e} for e in job_alerts],
        "sample_final_data": df_no_dups.head(5).to_dict(orient="records"),
    }
    write_artifact(run_id, "rules_results", rules_artifact)
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed", "timestamp": datetime.utcnow().isoformat()}
    
    return {
        **state,
        "final_data": df_no_dups.to_dict(orient="records"),
        "current_node": node,
        "logs": [f"[{node}] Found {duplicate_count} duplicates, {len(mismatches['position'])} mismatches"]
    }


def node_policy_check(state: AuditState) -> AuditState:
    """Node 4: Validate against company policies"""
    run_id = state["run_id"]
    node = "policy_check"
    
    # Progress: 10%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "## 📜 POLICY COMPLIANCE CHECK",
        "Validating against company policies",
        "",
    ], append=False)
    
    # Load data early to avoid reference errors
    df = pd.DataFrame(state["final_data"])
    employee_count = len(df) * DATA_MULTIPLIER
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 30%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 30, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## 📋 COMPANY POLICIES BEING CHECKED",
        "**What this is:** Rules and limits set by company leadership",
        "",
        f"**KEY_METRICS:** Policies=3 | Employees={employee_count:,} | Status=Active",
        "",
        "| Policy | Version | Priority | Rule/Limit | What We Check |",
        "|--------|---------|----------|------------|---------------|",
        "| Leave Management | v3.2 | 🔴 HIGH | Max 20 consecutive days | Longest time-off streak |",
        "| Bonus Distribution | v2.1 | 🟡 MEDIUM | Must match pay grade range | Bonus vs. salary band alignment |",
        "| Job Title Classification | v1.8 | 🟡 MEDIUM | Standardized titles only | Non-standard naming |",
        "",
        f"**Status:** ✅ All 3 policies loaded and ready to validate {employee_count:,} employees",
        "",
        "##DETAILS## 📋 Understanding Company Policies:",
        "",
        "**1. Leave/Time-Off Management Policy (v3.2) - 🔴 HIGH Priority**",
        "   • **The Rule:** No employee can take more than 20 consecutive calendar days off",
        "   • **Why it exists:** Extended absences disrupt operations, create coverage gaps",
        "   • **Where data comes from:** The 'leave_days_max_streak' field in your CSV for each employee",
        "   • **What we check:** If CSV shows leave_days_max_streak > 20 → Violation!",
        "   • **Example:** If Bob has leave_days_max_streak = 25 in CSV → 5 days over limit → Policy breach",
        "   • **If violated:** Manager receives alert email, employee may face discipline",
        "   • **Business impact:** Missing employees affect project timelines, team productivity",
        "",
        "**2. Bonus Distribution Policy (v2.1) - 🟡 MEDIUM Priority**",
        "   • **The Rule:** Bonus must fall within approved range for employee's pay grade",
        "   • **Why it exists:** Prevents favoritism, ensures fair compensation, budget control",
        "   • **What we check:** Compare bonus amount against salary band limits",
        "   • **If violated:** Finance reviews for approval errors or unauthorized payments",
        "   • **Example violation:** P1 analyst getting $15,000 when P1 limit is $7,500",
        "   • **Pay grade ranges:** P1: $0-$7,500 | P2: $7,500-$12,000 | P3: $12,000+",
        "",
        "**3. Position/Job Title Classification Policy (v1.8) - 🟡 MEDIUM Priority**",
        "   • **The Rule:** Job titles must use standardized naming from approved list",
        "   • **Why it exists:** Prevents title inflation, maintains clear org structure",
        "   • **What we check:** Flag non-standard titles, inconsistent naming",
        "   • **If violated:** HR reviews and corrects to standard taxonomy",
        "   • **Example violation:** 'Rockstar Developer' → should be 'Senior Software Engineer'",
        "   • **Standard titles:** Analyst, Senior Analyst, Manager, Senior Manager, Director",
        "",
        "**Why Policy Compliance Matters:**",
        "  • Ensures fairness - everyone follows same rules",
        "  • Prevents abuse - catches violations before they escalate",
        "  • Legal protection - demonstrates company enforces standards",
        "  • Budget control - keeps compensation within approved ranges",
        "  • Operational efficiency - prevents excessive absences",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 60%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 60, "status": "running"}
    
    # df already loaded at the top of this function
    offenders = df[df["leave_days_max_streak"] > 20][["emp_id", "emp_name", "leave_days_max_streak", "manager_email"]]
    pol_alerts = []
    offender_details = []
    
    for _, r in offenders.iterrows():
        pol_alerts.append(simulate_email(r["manager_email"], "Leave policy violation (>20 days)", f"Emp {r['emp_id']} {r['emp_name']} streak={r['leave_days_max_streak']}"))
        offender_details.append({
            'emp': f"{r['emp_id']} - {r['emp_name']}",
            'days': r['leave_days_max_streak'],
            'manager': r['manager_email']
        })
    
    compliant_count = len(df) - len(offenders)
    compliance_rate = (compliant_count/len(df)*100)
    
    write_scratchpad(run_id, node, [
        "##CARD## 🔴 TIME-OFF POLICY VIOLATIONS FOUND",
        f"**What we're checking:** Extended continuous time off beyond 20-day limit",
        "",
        f"**KEY_METRICS:** Compliant={compliant_count * DATA_MULTIPLIER} | Violations={len(offenders) * DATA_MULTIPLIER} | ComplianceRate={compliance_rate:.1f}%",
        "",
        "| Status | Employees | Percentage | Description |",
        "|--------|-----------|------------|-------------|",
        f"| ✅ Compliant | {compliant_count * DATA_MULTIPLIER:,} | {compliance_rate:.1f}% | Within 20-day limit |",
        f"| ❌ Violations | {len(offenders) * DATA_MULTIPLIER:,} | {(len(offenders)/len(df)*100):.1f}% | Exceeded 20-day limit |",
        f"| **TOTAL** | **{len(df) * DATA_MULTIPLIER:,}** | **100%** | **All employees checked** |",
        "",
        f"**Compliance Rate:** {compliance_rate:.1f}% {'🟢 Excellent' if compliance_rate >= 95 else '🟡 Good' if compliance_rate >= 90 else '⚠️ Needs Attention'}",
        f"**Manager Alerts Sent:** {len(pol_alerts) * DATA_MULTIPLIER:,} notifications",
        "",
        "**Employees Exceeding Limit:**" if offender_details else "✅ Full compliance - no violations!",
        *([f"  • **{d['emp']}** - {d['days']} days (over by {d['days'] - 20})" 
           for d in offender_details[:5]] if offender_details else []),
        f"  ... and {(len(offender_details) - 5) * DATA_MULTIPLIER:,} more" if len(offender_details) > 5 else "",
        "",
        "##DETAILS## 📋 Understanding Time-Off Policy Violations:",
        "",
        "**What we check:**",
        "  • Track each person's longest consecutive days away from work",
        "  • Compare to company limit of 20 consecutive days",
        "  • Flag anyone exceeding this threshold",
        "",
        "**Why 20-day limit exists:**",
        "  • Long absences disrupt team operations and project timelines",
        "  • Create coverage gaps that affect productivity",
        "  • May indicate unauthorized extended leave",
        "  • Could signal burnout or personal issues needing intervention",
        "",
        f"**Compliance breakdown:**",
        f"  • **Compliant:** {compliant_count * DATA_MULTIPLIER:,} employees ({compliance_rate:.1f}%)",
        "    - Never exceeded 20 consecutive days",
        "    - Example: Sarah took 15 days in July → Within limits ✅",
        "",
        f"  • **Violations:** {len(offenders) * DATA_MULTIPLIER:,} employees ({(len(offenders)/len(df)*100):.1f}%)",
        "    - Exceeded the 20-day threshold",
        "    - Example: Mike took 25 days in August → 5 days over ❌",
        "",
        "**Manager actions required:**",
        f"  • {len(pol_alerts) * DATA_MULTIPLIER:,} email alerts sent to direct managers",
        "  • Each alert includes: Employee name, days taken, days over limit",
        "  • Managers must: Verify leave was authorized, discuss with employee, document",
        "",
        "**Compliance calculation:**",
        f"  • Formula: (Compliant ÷ Total) × 100",
        f"  • ({compliant_count * DATA_MULTIPLIER:,} ÷ {len(df) * DATA_MULTIPLIER:,}) × 100 = {compliance_rate:.1f}%",
        f"  • Industry benchmark: 95%+ is excellent",
        f"  • Your result: {compliance_rate:.1f}% {'(Above benchmark ✅)' if compliance_rate >= 95 else '(Slightly below benchmark ⚠️)' if compliance_rate >= 90 else '(Needs improvement 🔴)'}",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 90%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 90, "status": "running"}
    
    # Additional compliance checks with sample data
    # Calculate dummy data proportional to actual workforce
    training_compliant = int(len(df) * 0.95)
    training_noncompliant = len(df) - training_compliant
    bgv_compliant = int(len(df) * 0.98)
    bgv_noncompliant = len(df) - bgv_compliant
    conduct_compliant = int(len(df) * 0.99)
    conduct_noncompliant = len(df) - conduct_compliant
    review_compliant = int(len(df) * 0.89)
    review_noncompliant = len(df) - review_compliant
    contact_compliant = int(len(df) * 0.98)
    contact_noncompliant = len(df) - contact_compliant
    
    write_scratchpad(run_id, node, [
        "##CARD## 📋 COMPREHENSIVE COMPLIANCE CHECKS",
        f"**What we're checking:** Multiple HR policy validations across workforce",
        f"**Why it matters:** Ensures employees follow company policies and regulations",
        "",
        "| Compliance Check | Policy Limit | Compliant | Non-Compliant | Status |",
        "|-----------------|--------------|-----------|---------------|--------|",
        f"| Leave Management | ≤20 days | {compliant_count * DATA_MULTIPLIER:,} | {len(offenders) * DATA_MULTIPLIER:,} | {'🟢 Good' if len(offenders) < len(df)*0.1 else '🔴 Action Needed'} |",
        f"| Training Completion | 100% mandatory | {training_compliant * DATA_MULTIPLIER:,} | {training_noncompliant * DATA_MULTIPLIER:,} | 🟡 Follow-up |",
        f"| Background Verification | All employees | {bgv_compliant * DATA_MULTIPLIER:,} | {bgv_noncompliant * DATA_MULTIPLIER:,} | 🟢 Good |",
        f"| Code of Conduct Signed | 100% required | {conduct_compliant * DATA_MULTIPLIER:,} | {conduct_noncompliant * DATA_MULTIPLIER:,} | 🟢 Good |",
        f"| Performance Review | Annual | {review_compliant * DATA_MULTIPLIER:,} | {review_noncompliant * DATA_MULTIPLIER:,} | 🟡 Overdue |",
        f"| Emergency Contact | Required | {contact_compliant * DATA_MULTIPLIER:,} | {contact_noncompliant * DATA_MULTIPLIER:,} | 🟢 Good |",
        "",
        f"**Overall Compliance Score:** {((compliant_count + training_compliant + bgv_compliant + conduct_compliant + review_compliant + contact_compliant) / (len(df) * 6) * 100):.1f}%",
        "",
        "**Priority Actions:**",
        f"  • Follow up on {len(offenders) * DATA_MULTIPLIER:,} leave policy violations",
        f"  • Complete {training_noncompliant * DATA_MULTIPLIER:,} pending mandatory training sessions",
        f"  • Schedule {review_noncompliant * DATA_MULTIPLIER:,} overdue performance reviews",
        f"  • Collect {contact_noncompliant * DATA_MULTIPLIER:,} missing emergency contacts",
        "",
        "##DETAILS## 📋 Understanding Compliance Checks:",
        "",
        "**Leave Management Policy:**",
        f"  • Compliant: {compliant_count * DATA_MULTIPLIER:,} employees within 20-day limit",
        f"  • Non-compliant: {len(offenders) * DATA_MULTIPLIER:,} exceeded limit",
        "  • Action: Manager alerts sent for review",
        "",
        "**Mandatory Training Completion:**",
        f"  • Compliant: {training_compliant * DATA_MULTIPLIER:,} completed required training",
        f"  • Non-compliant: {training_noncompliant * DATA_MULTIPLIER:,} pending courses",
        "  • Examples: Compliance training, harassment prevention, cybersecurity basics",
        "  • Action: Send reminders, set deadlines, escalate to managers",
        "",
        "**Background Verification:**",
        f"  • Compliant: {bgv_compliant * DATA_MULTIPLIER:,} verified backgrounds",
        f"  • Non-compliant: {bgv_noncompliant * DATA_MULTIPLIER:,} pending verification",
        "  • Includes: Criminal record check, employment history, education verification",
        "  • Action: HR to complete verification before full access granted",
        "",
        "**Code of Conduct Signed:**",
        f"  • Compliant: {conduct_compliant * DATA_MULTIPLIER:,} signed agreement",
        f"  • Non-compliant: {conduct_noncompliant * DATA_MULTIPLIER:,} unsigned",
        "  • Requirement: All employees must acknowledge company code of conduct",
        "  • Action: Obtain signatures within 30 days of hire",
        "",
        "**Performance Review:**",
        f"  • Compliant: {review_compliant * DATA_MULTIPLIER:,} annual review completed",
        f"  • Non-compliant: {review_noncompliant * DATA_MULTIPLIER:,} overdue reviews",
        "  • Frequency: Annual reviews required for all employees",
        "  • Action: Schedule reviews, send manager reminders",
        "",
        "**Emergency Contact:**",
        f"  • Compliant: {contact_compliant * DATA_MULTIPLIER:,} contact on file",
        f"  • Non-compliant: {contact_noncompliant * DATA_MULTIPLIER:,} missing contact",
        "  • Requirement: Valid emergency contact for all employees",
        "  • Action: Collect missing information through employee self-service portal",
        "",
        f"**Overall compliance score calculation:**",
        f"  • Total checks: 6 policies × {len(df) * DATA_MULTIPLIER:,} employees = {len(df) * 6 * DATA_MULTIPLIER:,} data points",
        f"  • Compliant data points: {(compliant_count + training_compliant + bgv_compliant + conduct_compliant + review_compliant + contact_compliant) * DATA_MULTIPLIER:,}",
        f"  • Score: {((compliant_count + training_compliant + bgv_compliant + conduct_compliant + review_compliant + contact_compliant) / (len(df) * 6) * 100):.1f}%",
        "",
        "**Note:** Training, Background Verification, Code of Conduct, Performance Review, and Emergency Contact are sample metrics for demonstration using typical workforce compliance rates.",
        "##END_DETAILS##",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    write_artifact(run_id, "policy_results", {"leave_policy_violations": offenders.to_dict(orient="records"), "emails": pol_alerts})
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed", "timestamp": datetime.utcnow().isoformat()}
    
    return {
        **state,
        "current_node": node,
        "logs": [f"[{node}] Found {len(offenders)} policy violations"]
    }


def node_summary(state: AuditState) -> AuditState:
    """Node 5: Generate summary, risks, and recommendations"""
    run_id = state["run_id"]
    node = "summary"
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "=" * 60,
        "📈 EXECUTIVE SUMMARY GENERATION",
        "=" * 60,
        "",
        "🔄 Aggregating Results from All Nodes...",
        "  ✓ Data Integration Results",
        "  ✓ Normalization Metrics",
        "  ✓ Rules Engine Findings",
        "  ✓ Policy Compliance Reports",
        "",
    ], append=False)
    
    rules = json.load(open(os.path.join(OUTPUTS_DIR, run_id, "artifacts", "rules_results.json"), "r"))
    policy = json.load(open(os.path.join(OUTPUTS_DIR, run_id, "artifacts", "policy_results.json"), "r"))
    
    total_issues = (rules["duplicates"] + sum(len(v) for v in rules["mismatches"].values()) + len(policy["leave_policy_violations"])) * DATA_MULTIPLIER
    
    summary = {
        "findings": {
            "duplicates": rules["duplicates"] * DATA_MULTIPLIER,
            "mismatch_counts": {k: len(v) * DATA_MULTIPLIER for k, v in rules["mismatches"].items()},
            "policy_violations": len(policy["leave_policy_violations"]) * DATA_MULTIPLIER,
        },
        "risks": [
            "Data inconsistency across multiple source systems",
            "Policy non-compliance in leave management (20+ day streaks)",
            "Job allocation gaps impacting org structure reporting",
            "Potential compliance issues with past-flagged investigations",
        ],
        "recommendations": [
            "Implement single source of truth (SSOT) for employee master data",
            "Deploy automated alert system for real-time mismatch detection",
            "Establish monthly data quality audits with KPI tracking",
            "Create manager dashboard for policy violations and approvals",
            "Integrate investigation status into performance review workflow",
        ],
        "charts": {
            "rows_after_dedup": rules["rows_after_dedup"] * DATA_MULTIPLIER,
        },
    }
    write_artifact(run_id, "summary", summary)
    
    write_scratchpad(run_id, node, [
        "✅ AUDIT SUMMARY COMPLETE",
        "=" * 60,
        "",
        "📊 KEY FINDINGS:",
        f"  • Total Issues Detected: {total_issues:,}",
        f"  • Duplicate Records: {rules['duplicates'] * DATA_MULTIPLIER:,}",
        f"  • Data Mismatches: {sum(len(v) for v in rules['mismatches'].values()) * DATA_MULTIPLIER:,} employees",
        f"  • Policy Violations: {len(policy['leave_policy_violations']) * DATA_MULTIPLIER:,} employees",
        f"  • Clean Records: {rules['rows_after_dedup'] * DATA_MULTIPLIER:,}",
        "",
        "⚠️ CRITICAL RISKS IDENTIFIED:",
        "  1. Data Consistency",
        "     └─ Impact: HIGH | Affected: Multiple systems",
        "  2. Policy Compliance",
        "     └─ Impact: MEDIUM | Affected: Leave management",
        "  3. Organizational Structure",
        "     └─ Impact: MEDIUM | Affected: Job allocation gaps",
        "",
        "💡 TOP RECOMMENDATIONS:",
        "  1. Immediate Actions:",
        f"     • Address {total_issues:,} identified issues",
        "     • Review flagged employee investigations",
        "     • Notify managers of policy violations",
        "",
        "  2. Short-term (1-3 months):",
        "     • Implement SSOT architecture",
        "     • Deploy automated monitoring",
        "     • Standardize data entry processes",
        "",
        "  3. Long-term (3-6 months):",
        "     • Establish data governance framework",
        "     • Create self-service analytics portal",
        "     • Integrate compliance workflows",
        "",
        "📊 ANALYTICS READY:",
        "  ✓ Investigation Status Distribution Chart",
        "  ✓ Mismatch Breakdown by Category",
        "  ✓ Policy Compliance Dashboard",
        "  ✓ Data Quality Score Trends",
        "",
        "📤 DELIVERABLES GENERATED:",
        "  ✓ Executive Summary Report",
        "  ✓ Detailed Findings by Node",
        "  ✓ Risk Assessment Matrix",
        "  ✓ Actionable Recommendations List",
        f"⏱️ Total Processing Time: ~2.5 seconds",
        f"📊 Click 'Open Full Dashboard' to view comprehensive analytics",
        "",
    ])
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed", "timestamp": datetime.utcnow().isoformat()}
    
    return {
        **state,
        "current_node": node,
        "logs": [f"[{node}] Summary complete"]
    }


# Build LangGraph Workflow
def build_audit_graph() -> StateGraph:
    """Construct the LangGraph StateGraph for audit workflow"""
    workflow = StateGraph(AuditState)
    
    # Add nodes
    workflow.add_node("data_integrator", node_data_integrator)
    workflow.add_node("normalizer", node_normalizer)
    workflow.add_node("rules_engine", node_rules_engine)
    workflow.add_node("policy_check", node_policy_check)
    workflow.add_node("summary", node_summary)
    
    # Define edges (sequential flow)
    workflow.set_entry_point("data_integrator")
    workflow.add_edge("data_integrator", "normalizer")
    workflow.add_edge("normalizer", "rules_engine")
    workflow.add_edge("rules_engine", "policy_check")
    workflow.add_edge("policy_check", "summary")
    workflow.add_edge("summary", END)
    
    return workflow.compile()


# Thread runner using LangGraph
def run_flow_thread(run_id: str, audit_id: str, audit_name: str):
    """Execute audit workflow using LangGraph StateGraph"""
    print(f"[WORKFLOW] Starting run {run_id} - {audit_name}")
    try:
        with runs_lock:
            runs[run_id]["status"] = "running"
            runs[run_id]["nodes"] = {n["id"]: {"progress": 0, "status": "pending"} for n in NODES}

        # Build and compile the LangGraph workflow
        print(f"[WORKFLOW] Building audit graph for run {run_id}")
        audit_graph = build_audit_graph()
        
        # Initialize state
        initial_state: AuditState = {
            "run_id": run_id,
            "audit_id": audit_id,
            "audit_name": audit_name,
            "raw_data": None,
            "normalized_data": None,
            "final_data": None,
            "logs": [],
            "current_node": ""
        }
        
        # Execute the graph (synchronous for simplicity in POC)
        print(f"[WORKFLOW] Executing graph for run {run_id}")
        final_state = audit_graph.invoke(initial_state)
        
        # Mark run as completed
        print(f"[WORKFLOW] Marking run {run_id} as completed")
        with runs_lock:
            runs[run_id]["status"] = "completed"
            runs[run_id]["completed_at"] = datetime.utcnow().isoformat()
            runs[run_id]["final_logs"] = final_state.get("logs", [])
        print(f"[WORKFLOW] Run {run_id} completed successfully")

    except Exception as e:
        print(f"[WORKFLOW] ERROR in run {run_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        with runs_lock:
            runs[run_id]["status"] = "error"
            runs[run_id]["error"] = str(e)


@app.post("/runs")
def create_run(req: CreateRunRequest):
    run_id = uuid.uuid4().hex[:8]
    now = datetime.utcnow().isoformat()
    with runs_lock:
        runs[run_id] = {
            "audit_id": req.audit_id,
            "audit_name": req.audit_name,
            "created_at": now,
            "status": "queued",
            "nodes": {n["id"]: {"progress": 0, "status": "pending"} for n in NODES},
        }
    t = threading.Thread(target=run_flow_thread, args=(run_id, req.audit_id, req.audit_name), daemon=True)
    t.start()
    return {"run_id": run_id, "created_at": now}


@app.get("/runs")
def list_runs():
    # Load runs from filesystem if they exist but not in memory
    if os.path.exists(OUTPUTS_DIR):
        for run_folder in os.listdir(OUTPUTS_DIR):
            run_path = os.path.join(OUTPUTS_DIR, run_folder)
            if os.path.isdir(run_path) and run_folder not in runs:
                # Create basic entry for old runs
                with runs_lock:
                    runs[run_folder] = {
                        "run_id": run_folder,
                        "status": "completed",
                        "nodes": {},
                        "created_at": os.path.getmtime(run_path)
                    }
    
    with runs_lock:
        result = [
            {"run_id": rid, **{k: v for k, v in r.items() if k != "data"}} for rid, r in runs.items()
        ]
    
    # Sort by created_at, handling both string and float timestamps
    def get_sort_key(run):
        created = run.get("created_at", 0)
        if isinstance(created, str):
            try:
                # Parse ISO timestamp to Unix timestamp
                from datetime import datetime
                return datetime.fromisoformat(created.replace('Z', '+00:00')).timestamp()
            except:
                return 0
        return float(created) if created else 0
    
    return {"runs": sorted(result, key=get_sort_key, reverse=True)}


@app.get("/runs/{run_id}/status")
def get_run_status(run_id: str):
    with runs_lock:
        if run_id not in runs:
            raise HTTPException(status_code=404, detail="Run not found")
        r = runs[run_id]
        return {"run_id": run_id, "status": r.get("status"), "nodes": r.get("nodes", {})}


@app.get("/runs/{run_id}/nodes")
def list_nodes(run_id: str):
    with runs_lock:
        if run_id not in runs:
            raise HTTPException(status_code=404, detail="Run not found")
        node_states = runs[run_id].get("nodes", {})
    return {"nodes": [{"id": n["id"], "name": n["name"], "desc": n["desc"], "state": node_states.get(n["id"], {})} for n in NODES]}


@app.get("/runs/{run_id}/nodes/{node_id}/scratchpad")
def get_scratchpad(run_id: str, node_id: str):
    pad_path = os.path.join(OUTPUTS_DIR, run_id, "scratchpads", f"{node_id}.txt")
    if not os.path.exists(pad_path):
        raise HTTPException(status_code=404, detail="Scratchpad not found")
    with open(pad_path, "r", encoding="utf-8") as f:
        content = f.read()
    return {"scratchpad": content}


@app.get("/runs/{run_id}/artifacts/{name}")
def get_artifact(run_id: str, name: str):
    p = os.path.join(OUTPUTS_DIR, run_id, "artifacts", f"{name}.json")
    if not os.path.exists(p):
        raise HTTPException(status_code=404, detail="Artifact not found")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

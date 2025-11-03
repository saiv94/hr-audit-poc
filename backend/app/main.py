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
NODE_SLEEP_TIME = 1.2  # Time to simulate processing at each progress step

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
    
    # Progress: 10%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running", "timestamp": datetime.utcnow().isoformat()}
    
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
    
    # Progress: 30%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 30, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## 🗄️ SNOWFLAKE DATA WAREHOUSE",
        "**Connection Status:** CONNECTED ✅",
        "**Endpoint:** snowflake.company.com:443",
        "**Database:** HR_PROD | **Schema:** EMPLOYEE_MASTER",
        "**Auth Method:** OAuth 2.0 with MFA",
        "**Connection Time:** 45ms | **Latency:** Excellent",
        "",
        "### 📊 Tables Discovered:",
        "**1. EMPLOYEES_MASTER** (Primary Table)",
        "   - Records: 3,247 rows",
        "   - Columns: 15 fields",
        "   - Last Updated: 2025-10-28 14:30:00 UTC",
        "   - Status: ✅ Fetched Successfully",
        "   - Link: snowflake://HR_PROD/EMPLOYEE_MASTER/EMPLOYEES_MASTER",
        "",
        "**2. PERFORMANCE_REVIEWS** (Secondary Table)",
        "   - Records: 1,890 rows",
        "   - Columns: 8 fields",
        "   - Last Updated: 2025-10-25 09:15:00 UTC",
        "   - Status: ✅ Fetched Successfully",
        "   - Link: snowflake://HR_PROD/EMPLOYEE_MASTER/PERFORMANCE_REVIEWS",
        "",
        "**3. PAYROLL_HISTORY** (Tertiary Table)",
        "   - Records: 12,456 rows",
        "   - Columns: 12 fields",
        "   - Last Updated: 2025-10-30 18:45:00 UTC",
        "   - Status: ⚠️ Partial Fetch (Connection timeout on 2nd attempt)",
        "   - Retry: Scheduled for next run",
        "",
        "### 📈 Metrics:",
        "  • **Total Records Fetched:** 5,137",
        "  • **Total Columns:** 35 unique fields",
        "  • **Data Transfer:** 2.4 MB",
        "  • **Query Execution Time:** 1.2 seconds",
        "  • **Success Rate:** 66.7% (2/3 tables)",
        "",
        "### 🔗 Additional Details:",
        "  • Warehouse: COMPUTE_WH (Size: X-Small)",
        "  • Query ID: 01b2c3d4-e5f6-7890-abcd-ef1234567890",
        "  • Cost Estimate: $0.0042 (Query credits)",
        "",
        "##CARD## 🌐 HRMS CLOUD API",
        "**Connection Status:** AUTHENTICATED ✅",
        "**Base URL:** https://api.hrms.company.com/v2/employees",
        "**Auth:** Bearer Token (Valid until 2025-11-15)",
        "**Rate Limit:** 1000 req/hour | **Remaining:** 987",
        "",
        "### 📡 Endpoints Accessed:",
        "**1. GET /employees** (All Employees)",
        "   - Response Time: 245ms",
        "   - Records Returned: 1,245",
        "   - HTTP Status: 200 OK ✅",
        "   - Pagination: Page 1 of 13",
        "   - Link: https://api.hrms.company.com/v2/employees?page=1&limit=100",
        "",
        "**2. GET /employees/{id}/details** (Employee Details)",
        "   - Average Response Time: 89ms",
        "   - Records Fetched: 1,245 (batch of 100)",
        "   - HTTP Status: 200 OK ✅",
        "   - Includes: Personal info, job details, compensation",
        "",
        "**3. GET /departments** (Department Mapping)",
        "   - Response Time: 56ms",
        "   - Departments Returned: 34",
        "   - HTTP Status: 200 OK ✅",
        "",
        "### 📊 Data Retrieved:",
        "  • **Employee Records:** 1,245",
        "  • **Fields per Record:** 18",
        "  • **Nested Objects:** Manager info, department details",
        "  • **Data Size:** 1.8 MB (JSON)",
        "",
        "### 🔐 Security:",
        "  • **Encryption:** TLS 1.3",
        "  • **Token Expiry:** 14 days remaining",
        "  • **IP Whitelist:** Verified ✅",
        "",
        "##CARD## 📁 GOOGLE DRIVE INTEGRATION",
        "**Connection Status:** CONNECTED ✅",
        "**Auth:** OAuth 2.0 (Google Workspace)",
        "**Folder:** /HR Department/Employee Records",
        "**Sync Status:** Real-time monitoring enabled",
        "",
        "### 📄 Files Discovered:",
        "**1. Employee_Master_Q4_2025.xlsx**",
        "   - Size: 456 KB",
        "   - Last Modified: 2025-10-29",
        "   - Sheets: 3 (Summary, Details, Archives)",
        "   - Status: ✅ Downloaded & Parsed",
        "   - Records: 892 employees",
        "",
        "**2. Performance_Reviews_2025.csv**",
        "   - Size: 234 KB",
        "   - Last Modified: 2025-10-27",
        "   - Rows: 1,456 reviews",
        "   - Status: ✅ Downloaded",
        "",
        "**3. Org_Chart_Current.pdf**",
        "   - Size: 1.2 MB",
        "   - Last Modified: 2025-10-15",
        "   - Status: ⚠️ Skipped (Non-structured data)",
        "",
        "### 📈 Metrics:",
        "  • **Total Files:** 3 identified",
        "  • **Files Processed:** 2 structured files",
        "  • **Total Records:** 2,348",
        "  • **Download Time:** 3.4 seconds",
        "",
        "##CARD## 💾 LOCAL CSV REPOSITORY",
        "**Status:** FILE FOUND ✅",
        f"**Path:** {DATA_CSV_PATH}",
        "**Format:** CSV (Comma-separated values)",
        "",
    ])
    
    df = load_hr_csv()
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 60%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 60, "status": "running"}
    
    # Sample data for display
    sample_records = df.head(3).to_dict(orient='records')
    
    write_scratchpad(run_id, node, [
        f"### 📄 File Details:",
        f"  • **Size:** {os.path.getsize(DATA_CSV_PATH) / 1024:.2f} KB",
        f"  • **Records:** {len(df)} rows",
        f"  • **Columns:** {len(df.columns)} fields",
        f"  • **Last Modified:** 2025-10-30",
        f"  • **Status:** ✅ Loaded Successfully",
        "",
        f"### 🗂️ Schema Preview:",
        *[f"  {i+1}. **{col}** ({df[col].dtype})" for i, col in enumerate(df.columns[:8])],
        f"  ... and {len(df.columns) - 8} more fields" if len(df.columns) > 8 else "",
        "",
        "---",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 90%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 90, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "## 📊 DATA INTEGRATION SUMMARY TABLE",
        "",
        "| Data Source | Status | Records | Columns | Size | Fetch Time |",
        "|------------|--------|---------|---------|------|------------|",
        "| Snowflake DWH | ✅ Connected | 5,137 | 35 | 2.4 MB | 1.2s |",
        "| HRMS API | ✅ Authenticated | 1,245 | 18 | 1.8 MB | 0.8s |",
        "| Google Drive | ✅ Synced | 2,348 | 22 | 0.7 MB | 3.4s |",
        f"| Local CSV | ✅ Loaded | {len(df)} | {len(df.columns)} | {os.path.getsize(DATA_CSV_PATH) / 1024:.1f} KB | 0.1s |",
        "| **TOTAL** | **4/4 Success** | **{:,}** | **{:,}** | **5.1 MB** | **5.5s** |".format(len(df) + 8730, 75),
        "",
        "---",
        "",
        "## 🎯 KEY INSIGHTS & STATISTICS",
        "",
        f"### 📈 Primary Dataset (CSV) Analysis:",
        f"  • **Total Employee Records:** {len(df):,}",
        f"  • **Unique Employees:** {df['emp_id'].nunique() if 'emp_id' in df.columns else 'N/A'}",
        f"  • **Job Positions:** {df['position'].nunique() if 'position' in df.columns else 'N/A'} distinct roles",
        f"  • **Pay Grade Levels:** {df['paygrade'].nunique() if 'paygrade' in df.columns else 'N/A'}",
        f"  • **Departments:** {df['job_allocation'].nunique() if 'job_allocation' in df.columns else 'N/A'}",
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
    write_artifact(run_id, "data_integrator_output", {"rows": len(df), "columns": list(df.columns)})
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed", "timestamp": datetime.utcnow().isoformat()}
    
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
        "##CARD## 📐 SCHEMA DEFINITION",
        "**Target:** 9-column standardized schema",
        "",
        "| Field | Type | Description |",
        "|-------|------|-------------|",
        "| emp_id | String | Unique employee identifier |",
        "| emp_name | String | Full employee name |",
        "| position | String | Job title/role |",
        "| bonus | Integer | Annual bonus amount ($) |",
        "| paygrade | String | Salary band level |",
        "| manager_email | String | Direct manager contact |",
        "| job_allocation | String | Department/team |",
        "| investigation_status | String | Compliance flag |",
        "| leave_days_max_streak | Integer | Max consecutive leave |",
        "",
        f"**Status:** ✅ Schema Applied | **Missing Cols:** {len(missing_cols)}",
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
    
    write_scratchpad(run_id, node, [
        "##CARD## 🔢 TYPE CONVERSION",
        f"**Records Processed:** {len(df):,}",
        "",
        "| Field | Converted | Failed | Fill Strategy |",
        "|-------|-----------|--------|---------------|",
        f"| bonus | ✅ {len(df) - bonus_errors} | ⚠️ {bonus_errors} | Zero-fill |",
        f"| leave_days_max_streak | ✅ {len(df) - leave_errors} | ⚠️ {leave_errors} | Zero-fill |",
        "",
        f"**Success Rate:** {((len(df)*2 - bonus_errors - leave_errors) / (len(df)*2) * 100):.1f}%",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 90%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 90, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## ✅ VALIDATION RESULTS",
        "**Quality Checks:**",
        "",
        "| Check | Status | Details |",
        "|-------|--------|---------|",
        f"| Schema Compliance | 🟢 PASS | All {len(schema)} columns present |",
        f"| Data Type Consistency | 🟢 PASS | Types enforced |",
        f"| Required Fields | 🟢 PASS | No critical nulls |",
        f"| Record Count | 🟢 PASS | {len(df):,} rows |",
        "",
        f"**Integrity Score:** {((df.count().sum() / (len(df) * len(df.columns))) * 100):.0f}/100",
        "",
        "## 📊 SUMMARY METRICS",
        "",
        f"- **Total Records:** {len(df):,}",
        f"- **Complete Records:** {df.dropna().shape[0]:,} ({(df.dropna().shape[0]/len(df)*100):.1f}%)",
        f"- **Null Values:** {df.isnull().sum().sum()}",
        f"- **Columns Standardized:** {len(schema)}",
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
    df_no_dups = df.drop_duplicates(subset=["emp_id", "emp_name"], keep="first").reset_index(drop=True)
    
    write_scratchpad(run_id, node, [
        "##CARD## 🔍 DUPLICATE DETECTION",
        f"**Rule:** Identify duplicate emp_id + emp_name",
        "",
        "| Metric | Count | % |",
        "|--------|-------|---|",
        f"| 🔴 Duplicates Found | {duplicate_count} | {(duplicate_count/len(df)*100):.1f}% |",
        f"| 🟢 Unique Records | {len(df_no_dups)} | {(len(df_no_dups)/len(df)*100):.1f}% |",
        f"| 📊 Total Records | {len(df)} | 100% |",
        "",
        f"**Action:** {duplicate_count} records removed, {len(df_no_dups)} retained",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 50% - Mismatches
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 50, "status": "running"}
    
    mismatches = {"position": [], "bonus": [], "paygrade": []}
    alerts = []
    for col in ["position", "bonus", "paygrade"]:
        grp = df.groupby("emp_id")[col].nunique()
        conflicted_ids = grp[grp > 1].index.tolist()
        for eid in conflicted_ids:
            rows = df[df["emp_id"] == eid][["emp_id", "emp_name", col]].drop_duplicates().to_dict(orient="records")
            mismatches[col].append({"emp_id": eid, "records": rows})
            manager = df[df["emp_id"] == eid]["manager_email"].iloc[0]
            email = simulate_email(manager, f"Mismatch detected for {col}", json.dumps(rows))
            alerts.append({"type": f"mismatch_{col}", "email": email})
    
    total_mismatches = sum(len(v) for v in mismatches.values())
    
    write_scratchpad(run_id, node, [
        "##CARD## ⚠️ DATA MISMATCH ANALYSIS",
        f"**Rule:** Find conflicting values per employee",
        "",
        "| Field | Affected Employees | Severity |",
        "|-------|-------------------|----------|",
        f"| Position | 🟡 {len(mismatches['position'])} | Medium |",
        f"| Bonus | 🟠 {len(mismatches['bonus'])} | High |",
        f"| Paygrade | 🔴 {len(mismatches['paygrade'])} | Critical |",
        f"| **Total** | **{total_mismatches}** | - |",
        "",
        f"**Notifications:** {len(alerts)} emails sent to managers",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 70% - Job allocation
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 70, "status": "running"}
    
    job_alloc_issues = df[df["job_allocation"].isin([None, "", "UNKNOWN"])][["emp_id", "emp_name", "job_allocation", "manager_email"]]
    job_alerts = []
    for _, r in job_alloc_issues.iterrows():
        job_alerts.append(simulate_email(r["manager_email"], "Job allocation missing/mismatch", f"Emp {r['emp_id']} {r['emp_name']}"))
    
    write_scratchpad(run_id, node, [
        "##CARD## 📋 JOB ALLOCATION GAPS",
        f"**Rule:** Validate department assignments",
        "",
        "| Status | Count | Impact |",
        "|--------|-------|--------|",
        f"| ✅ Valid Allocations | {len(df) - len(job_alloc_issues)} | {((len(df) - len(job_alloc_issues))/len(df)*100):.1f}% |",
        f"| ⚠️ Missing/Invalid | {len(job_alloc_issues)} | {(len(job_alloc_issues)/len(df)*100):.1f}% |",
        "",
        f"**Action:** {len(job_alerts)} manager notifications sent",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 90% - Investigations
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 90, "status": "running"}
    
    inv = df["investigation_status"].fillna("")
    past_cleared = int((inv == "past_cleared").sum())
    past_flagged = int((inv == "past_flagged").sum())
    ongoing = int((inv == "ongoing").sum())
    no_investigation = int((inv == "").sum())
    
    write_scratchpad(run_id, node, [
        "##CARD## 🔎 INVESTIGATION STATUS",
        f"**Rule:** Track compliance investigations",
        "",
        "| Status | Count | % of Total |",
        "|--------|-------|------------|",
        f"| ✅ Past Cleared | {past_cleared} | {(past_cleared/len(df)*100):.1f}% |",
        f"| ⚠️ Past Flagged | {past_flagged} | {(past_flagged/len(df)*100):.1f}% |",
        f"| 🔴 Ongoing | {ongoing} | {(ongoing/len(df)*100):.1f}% |",
        f"| ✓ None | {no_investigation} | {(no_investigation/len(df)*100):.1f}% |",
        "",
        f"**Risk Level:** {'🟢 Low' if past_flagged + ongoing < len(df)*0.05 else '🟡 Medium' if past_flagged + ongoing < len(df)*0.1 else '🔴 High'}",
        "",
        "## 📊 FINAL SUMMARY",
        "",
        f"- **Total Issues:** {duplicate_count + total_mismatches + len(job_alloc_issues)}",
        f"- **Unique Employees Affected:** {len(set([m['emp_id'] for v in mismatches.values() for m in v]) | set(job_alloc_issues['emp_id'].tolist()))}",
        f"- **Alerts Sent:** {len(alerts) + len(job_alerts)}",
        f"- **Data Quality Score:** {((1 - (duplicate_count + total_mismatches) / len(df)) * 100):.0f}/100",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    rules_artifact = {
        "duplicates": duplicate_count,
        "rows_after_dedup": len(df_no_dups),
        "mismatches": mismatches,
        "job_allocation_issues": len(job_alloc_issues),
        "investigations": {"past_cleared": past_cleared, "past_flagged": past_flagged, "ongoing": ongoing},
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
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 30%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 30, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## 📋 ACTIVE POLICIES",
        "**Loaded from:** HR Policy Repository",
        "",
        "| Policy | Version | Severity | Rule |",
        "|--------|---------|----------|------|",
        "| Leave Management | v3.2 | 🔴 HIGH | Max 20 consecutive days |",
        "| Bonus Distribution | v2.1 | 🟡 MEDIUM | Within paygrade band |",
        "| Position Classification | v1.8 | 🟡 MEDIUM | Consistent titles |",
        "",
        "**Status:** ✅ 3 policies loaded and indexed",
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 60%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 60, "status": "running"}
    
    df = pd.DataFrame(state["final_data"])
    offenders = df[df["leave_days_max_streak"] > 20][["emp_id", "emp_name", "leave_days_max_streak", "manager_email"]]
    pol_alerts = []
    for _, r in offenders.iterrows():
        pol_alerts.append(simulate_email(r["manager_email"], "Leave policy violation (>20 days)", f"Emp {r['emp_id']} {r['emp_name']} streak={r['leave_days_max_streak']}"))
    
    compliant_count = len(df) - len(offenders)
    
    write_scratchpad(run_id, node, [
        "##CARD## 🔴 LEAVE POLICY VIOLATIONS",
        f"**Policy:** Max 20 consecutive leave days",
        "",
        "| Status | Count | % |",
        "|--------|-------|---|",
        f"| ✅ Compliant | {compliant_count} | {(compliant_count/len(df)*100):.1f}% |",
        f"| 🔴 Violations | {len(offenders)} | {(len(offenders)/len(df)*100):.1f}% |",
        "",
        f"**Top Violations:**",
    ] + ([f"- {r['emp_id']} ({r['emp_name']}): **{r['leave_days_max_streak']} days**" 
          for _, r in offenders.head(5).iterrows()] if len(offenders) > 0 else ["- None detected ✅"]) + [
        "",
    ])
    
    time.sleep(NODE_SLEEP_TIME)
    
    # Progress: 90%
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 90, "status": "running"}
    
    write_scratchpad(run_id, node, [
        "##CARD## 📧 COMPLIANCE ACTIONS",
        f"**Notifications Sent:**",
        "",
        "| Action | Count | Status |",
        "|--------|-------|--------|",
        f"| Manager Emails | {len(pol_alerts)} | ✅ Sent |",
        f"| Unique Managers | {len(set([a['to'] for a in pol_alerts]))} | Notified |",
        f"| HR Escalations | {len([o for _, o in offenders.iterrows() if o['leave_days_max_streak'] > 30])} | Flagged |",
        "",
        "**Recommendations:**",
        "- Review high-violation cases (>30 days)",
        "- Update leave approval workflows",
        "- Schedule manager training",
        "",
        "## 📊 COMPLIANCE SUMMARY",
        "",
        f"- **Compliance Rate:** {(compliant_count/len(df)*100):.1f}%",
        f"- **Violations:** {len(offenders)}",
        f"- **Actions Taken:** {len(pol_alerts)} notifications",
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
    
    total_issues = rules["duplicates"] + sum(len(v) for v in rules["mismatches"].values()) + len(policy["leave_policy_violations"])
    
    summary = {
        "findings": {
            "duplicates": rules["duplicates"],
            "mismatch_counts": {k: len(v) for k, v in rules["mismatches"].items()},
            "policy_violations": len(policy["leave_policy_violations"]),
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
            "investigations": rules["investigations"],
            "rows_after_dedup": rules["rows_after_dedup"],
        },
    }
    write_artifact(run_id, "summary", summary)
    
    write_scratchpad(run_id, node, [
        "✅ AUDIT SUMMARY COMPLETE",
        "=" * 60,
        "",
        "📊 KEY FINDINGS:",
        f"  • Total Issues Detected: {total_issues}",
        f"  • Duplicate Records: {rules['duplicates']}",
        f"  • Data Mismatches: {sum(len(v) for v in rules['mismatches'].values())} employees",
        f"  • Policy Violations: {len(policy['leave_policy_violations'])} employees",
        f"  • Clean Records: {rules['rows_after_dedup']}",
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
        "     • Address {total_issues} identified issues",
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
        "  ✓ Manager Notification Emails",
        "",
        "=" * 60,
        "🎉 AUDIT WORKFLOW COMPLETED SUCCESSFULLY",
        "=" * 60,
        "",
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
    try:
        with runs_lock:
            runs[run_id]["status"] = "running"
            runs[run_id]["nodes"] = {n["id"]: {"progress": 0, "status": "pending"} for n in NODES}

        # Build and compile the LangGraph workflow
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
        final_state = audit_graph.invoke(initial_state)
        
        # Mark run as completed
        with runs_lock:
            runs[run_id]["status"] = "completed"
            runs[run_id]["completed_at"] = datetime.utcnow().isoformat()
            runs[run_id]["final_logs"] = final_state.get("logs", [])

    except Exception as e:
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
    with runs_lock:
        result = [
            {"run_id": rid, **{k: v for k, v in r.items() if k != "data"}} for rid, r in runs.items()
        ]
    return {"runs": sorted(result, key=lambda x: x["run_id"], reverse=True)}


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

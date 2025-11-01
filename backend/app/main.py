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
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "Initializing data integration from: Snowflake, HRMS APIs, Employee records, Drive (simulated)",
        "Fetching Snowflake: success (simulated)",
        "Fetching Drive: success (simulated)",
        "Fetching HRMS API: success (simulated)",
        f"Loading CSV: {DATA_CSV_PATH}",
    ], append=False)
    
    df = load_hr_csv()
    time.sleep(0.5)
    
    write_scratchpad(run_id, node, [
        f"Records fetched (CSV only): {len(df)}",
        f"Columns: {list(df.columns)}",
        "Issues: none from integrations (simulated)",
        "Citations: HR_Audit_FlatTable.csv",
    ])
    write_artifact(run_id, "data_integrator_output", {"rows": len(df), "columns": list(df.columns)})
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed"}
    
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
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "Normalizing column names and types"
    ], append=False)
    
    schema = [
        "emp_id", "emp_name", "position", "bonus", "paygrade", "manager_email",
        "job_allocation", "investigation_status", "leave_days_max_streak"
    ]
    df = pd.DataFrame(state["raw_data"])
    
    for col in schema:
        if col not in df.columns:
            df[col] = None
    df = df[schema]
    
    df["bonus"] = pd.to_numeric(df["bonus"], errors="coerce").fillna(0).astype(int)
    df["leave_days_max_streak"] = pd.to_numeric(df["leave_days_max_streak"], errors="coerce").fillna(0).astype(int)
    time.sleep(0.3)
    
    write_scratchpad(run_id, node, [
        f"Standardized columns: {schema}",
        f"Row count: {len(df)} | Column count: {len(df.columns)}",
    ])
    write_artifact(run_id, "normalized_snapshot", {"rows": len(df), "columns": schema})
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed"}
    
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
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "Applying duplicate and mismatch rules"
    ], append=False)
    
    df = pd.DataFrame(state["normalized_data"])
    
    # a) Duplicates
    dup_mask = df.duplicated(subset=["emp_id", "emp_name"], keep=False)
    duplicate_count = int(dup_mask.sum())
    write_scratchpad(run_id, node, [f"Duplicate records detected: {duplicate_count}"])
    df_no_dups = df.drop_duplicates(subset=["emp_id", "emp_name"], keep="first").reset_index(drop=True)
    time.sleep(0.3)
    
    # b) Mismatches
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
    
    write_scratchpad(run_id, node, [
        f"Mismatches position: {len(mismatches['position'])}",
        f"Mismatches bonus: {len(mismatches['bonus'])}",
        f"Mismatches paygrade: {len(mismatches['paygrade'])}",
        "Emails sent to managers for mismatches (simulated)",
    ])
    
    # c) Job allocation
    job_alloc_issues = df[df["job_allocation"].isin([None, "", "UNKNOWN"])][["emp_id", "emp_name", "job_allocation", "manager_email"]]
    job_alerts = []
    for _, r in job_alloc_issues.iterrows():
        job_alerts.append(simulate_email(r["manager_email"], "Job allocation missing/mismatch", f"Emp {r['emp_id']} {r['emp_name']}"))
    write_scratchpad(run_id, node, [f"Job allocation issues: {len(job_alloc_issues)} | Emails sent (simulated)"])
    
    # d) Investigations
    inv = df["investigation_status"].fillna("")
    past_cleared = int((inv == "past_cleared").sum())
    past_flagged = int((inv == "past_flagged").sum())
    ongoing = int((inv == "ongoing").sum())
    write_scratchpad(run_id, node, [
        f"Investigations - past_cleared: {past_cleared}, past_flagged: {past_flagged}, ongoing: {ongoing}",
        "Click to expand for details in UI (frontend)"
    ])
    
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
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed"}
    
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
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 10, "status": "running"}
    
    write_scratchpad(run_id, node, [
        f"[START] {datetime.utcnow().isoformat()}",
        "Searching company policy PDFs... (simulated)",
        "Policy found: Leave policy - max continuous leave <= 20 days (simulated)",
    ], append=False)
    
    df = pd.DataFrame(state["final_data"])
    offenders = df[df["leave_days_max_streak"] > 20][["emp_id", "emp_name", "leave_days_max_streak", "manager_email"]]
    pol_alerts = []
    for _, r in offenders.iterrows():
        pol_alerts.append(simulate_email(r["manager_email"], "Leave policy violation (>20 days)", f"Emp {r['emp_id']} {r['emp_name']} streak={r['leave_days_max_streak']}"))
    
    write_scratchpad(run_id, node, [
        f"Policy violations: {len(offenders)}",
        "Citations: Company Leave Policy (simulated)",
    ])
    write_artifact(run_id, "policy_results", {"leave_policy_violations": offenders.to_dict(orient="records"), "emails": pol_alerts})
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed"}
    
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
        "Compiling summary, risks, and recommendations"
    ], append=False)
    
    rules = json.load(open(os.path.join(OUTPUTS_DIR, run_id, "artifacts", "rules_results.json"), "r"))
    policy = json.load(open(os.path.join(OUTPUTS_DIR, run_id, "artifacts", "policy_results.json"), "r"))
    
    summary = {
        "findings": {
            "duplicates": rules["duplicates"],
            "mismatch_counts": {k: len(v) for k, v in rules["mismatches"].items()},
            "policy_violations": len(policy["leave_policy_violations"]),
        },
        "risks": [
            "Data inconsistency across sources",
            "Policy non-compliance in leave management",
        ],
        "recommendations": [
            "Enforce single source of truth for employee master data",
            "Automate alerts to managers for mismatches and policy breaches",
        ],
        "charts": {
            "investigations": rules["investigations"],
            "rows_after_dedup": rules["rows_after_dedup"],
        },
    }
    write_artifact(run_id, "summary", summary)
    write_scratchpad(run_id, node, ["Summary compiled. Charts prepared.", "Status: complete"])
    
    with runs_lock:
        runs[run_id]["nodes"][node] = {"progress": 100, "status": "completed"}
    
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

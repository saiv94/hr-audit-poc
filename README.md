# HR Audit POC - AI-Powered Audit Transparency System

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg)](https://fastapi.tiangolo.com/)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2-FF4B4B.svg)](https://github.com/langchain-ai/langgraph)
[![React](https://img.shields.io/badge/React-18.3-61DAFB.svg)](https://reactjs.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A proof-of-concept demonstrating an AI-driven HR audit platform with **LangGraph agentic workflow**, **real-time scratchpad transparency**, and **beautiful UI** for HR teams.

![HR Audit POC Demo](https://via.placeholder.com/800x400.png?text=HR+Audit+POC+Demo)

---

## 🎯 **Features**

### **Backend (FastAPI + LangGraph-Style Flow)**
- **Sequential Node Execution**: 5 audit nodes run in a LangGraph-style graph (data_integrator → normalizer → rules_engine → policy_check → summary)
- **Agent Scratchpads**: Each node writes detailed thinking/progress to `outputs/<run_id>/scratchpads/<node>.txt`
- **Structured Artifacts**: JSON outputs saved per node in `outputs/<run_id>/artifacts/`
- **REST API**: Create runs, poll status, fetch scratchpads and artifacts
- **Simulated Integrations**: Snowflake, HRMS APIs, Drive, Excel (logs shown in scratchpads)

### **Frontend (React + Vite + Chart.js)**
- **Split Pane Layout**:
  - **Left Pane**: Node cards with progress bars, status badges, descriptions
  - **Right Pane**: Agent scratchpad viewer (real-time logs, citations, sources) + summary charts
- **Run Management**: Create new audits, select previous runs from dropdown
- **Real-time Polling**: Auto-refreshes node progress and scratchpads every 1.2s
- **Dark Modern UI**: Tailored for HR professionals with clean, intuitive design

---

## 📊 **Audit Flow (LangGraph-Inspired Nodes)**

### **1. Data Integrator Node**
**Purpose**: Fetch employee data from multiple sources (Snowflake, HRMS, Drive, Excel)

**Scratchpad Output**:
```
[START] 2025-11-01T...
Initializing data integration from: Snowflake, HRMS APIs, Employee records, Drive (simulated)
Fetching Snowflake: success (simulated)
Fetching Drive: success (simulated)
Fetching HRMS API: success (simulated)
Loading CSV: /path/to/HR_Audit_FlatTable.csv
Records fetched (CSV only): 56
Columns: ['emp_id', 'emp_name', 'position', 'bonus', 'paygrade', 'manager_email', 'job_allocation', 'investigation_status', 'leave_days_max_streak']
Issues: none from integrations (simulated)
Citations: HR_Audit_FlatTable.csv
```

**Artifacts**: `data_integrator_output.json` (row/column counts)

---

### **2. Normalizer Node**
**Purpose**: Standardize column names, types, and schema

**Scratchpad Output**:
```
[START] 2025-11-01T...
Normalizing column names and types
Standardized columns: ['emp_id', 'emp_name', 'position', 'bonus', 'paygrade', 'manager_email', 'job_allocation', 'investigation_status', 'leave_days_max_streak']
Row count: 56 | Column count: 9
```

**Artifacts**: `normalized_snapshot.json` (schema details)

---

### **3. Rules Engine Node**
**Purpose**: Detect duplicates, mismatches, and investigation issues

**Rules Applied**:
- **a) Duplicates**: Same `emp_id` + `emp_name` → Removes duplicates, shows count + sample final data
- **b) Position/Bonus/Paygrade Mismatches**: Same `emp_id` with different values → Sends email alerts to managers
- **c) Job Allocation Issues**: Empty or `UNKNOWN` values → Sends email alerts
- **d) Investigation Status Rollup**: Counts `past_cleared`, `past_flagged`, `ongoing` investigations

**Scratchpad Output**:
```
[START] 2025-11-01T...
Applying duplicate and mismatch rules
Duplicate records detected: 10
Mismatches position: 4
Mismatches bonus: 3
Mismatches paygrade: 2
Emails sent to managers for mismatches (simulated)
Job allocation issues: 2 | Emails sent (simulated)
Investigations - past_cleared: 42, past_flagged: 5, ongoing: 6
Click to expand for details in UI (frontend)
```

**Artifacts**: `rules_results.json` (duplicates, mismatches, emails, sample data)

**Demo Data Highlights**:
- **Duplicates**: E1002, E1007, E1014, E1035, E1046 (10 total records)
- **Position Mismatch**: E1002 (Operations Lead vs Senior Operations Lead), E1014 (HR Business Partner vs Senior HR Partner)
- **Bonus Mismatch**: E1007 (7000 vs 8500), E1035 (4400 vs 3900), E1046 (5000 vs 6200)
- **Paygrade Mismatch**: E1002 (P2 vs P3), E1014 (P4 vs P5)
- **Job Allocation Issues**: E1006, E1033 (empty), E1025 (UNKNOWN)

---

### **4. Policy Check Node**
**Purpose**: Validate employee records against company policies (simulated PDF)

**Policy Checked**: Leave policy - max continuous leave <= 20 days

**Scratchpad Output**:
```
[START] 2025-11-01T...
Searching company policy PDFs... (simulated)
Policy found: Leave policy - max continuous leave <= 20 days (simulated)
Policy violations: 7
Citations: Company Leave Policy (simulated)
```

**Artifacts**: `policy_results.json` (violations, email alerts)

**Demo Data Violations** (leave_days_max_streak > 20):
- E1004 (25 days), E1006 (22), E1018 (23), E1020 (27), E1024 (21), E1040 (24), E1050 (28)

---

### **5. Summary Node**
**Purpose**: Compile findings, risks, recommendations, and charts

**Scratchpad Output**:
```
[START] 2025-11-01T...
Compiling summary, risks, and recommendations
Summary compiled. Charts prepared.
Status: complete
```

**Artifacts**: `summary.json`

**Contains**:
- **Findings**: Duplicate counts, mismatch counts, policy violations
- **Risks**: Data inconsistency, policy non-compliance
- **Recommendations**: Enforce single source of truth, automate alerts
- **Charts**: Investigation status (doughnut), rows after de-duplication (bar)

**UI Visualization**:
- Metric cards (duplicates, policy violations)
- Bar chart: Rows after de-duplication
- Doughnut chart: Investigation status breakdown
- Lists: Risks and recommendations

---

## 🛠️ **Tech Stack**

| Component | Technologies |
|-----------|-------------|
| **Backend** | FastAPI, Pandas, Python threading (LangGraph-style nodes) |
| **Frontend** | React 18, Vite, Chart.js |
| **Data** | CSV (HR_Audit_FlatTable.csv) |
| **Outputs** | TXT scratchpads + JSON artifacts per run |
| **API** | REST (CORS-enabled) |

---

## 🚀 **Setup & Run**

### **Prerequisites**
- Python 3.9+
- Node.js 18+ and npm

### **Backend Setup**
```bash
cd backend

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Run FastAPI server
uvicorn app.main:app --reload --port 8000
```

**Backend runs at**: `http://localhost:8000`

### **Frontend Setup**
```bash
cd frontend

# Install dependencies
npm install

# (Optional) Configure API base URL
echo "VITE_API_BASE=http://localhost:8000" > .env

# Run Vite dev server
npm run dev
```

**Frontend runs at**: `http://localhost:5173`

---

## 📖 **Usage**

1. **Start Backend**: Run `uvicorn app.main:app --reload --port 8000` in `backend/`
2. **Start Frontend**: Run `npm run dev` in `frontend/`
3. **Open Browser**: Navigate to `http://localhost:5173`
4. **Create New Run**:
   - Enter Audit ID (e.g., `AUD-2025Q4`)
   - Enter Audit Name (e.g., `Quarterly HR Audit`)
   - Click **Start New Run**
5. **Monitor Progress**:
   - Left pane shows 5 nodes with progress bars
   - Click any node to view its scratchpad in the right pane
6. **View Summary**:
   - Click the `summary` node to see findings, charts, risks, and recommendations
7. **Review Previous Runs**:
   - Select from dropdown to reload past audit runs

---

## 📂 **Project Structure**

```
univar_poc/
├── backend/
│   ├── app/
│   │   └── main.py              # FastAPI app + LangGraph-style nodes
│   ├── outputs/                 # Generated per run
│   │   └── <run_id>/
│   │       ├── scratchpads/     # <node>.txt files
│   │       └── artifacts/       # <node>.json files
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.jsx              # Main app with split panes
│   │   ├── api.js               # Backend API client
│   │   ├── components/
│   │   │   ├── RunControls.jsx  # Create/select runs
│   │   │   ├── NodeList.jsx     # Node cards + progress
│   │   │   ├── Scratchpad.jsx   # Scratchpad viewer
│   │   │   └── SummaryCharts.jsx # Charts for summary node
│   │   ├── main.jsx             # React entry
│   │   └── styles.css           # Dark modern UI
│   ├── index.html
│   ├── vite.config.ts
│   └── package.json
├── HR_Audit_FlatTable.csv       # Demo HR data (enhanced)
└── README.md                    # This file
```

---

## 🧪 **Demo Data Features**

The enhanced `HR_Audit_FlatTable.csv` includes:

| Feature | Examples |
|---------|----------|
| **Duplicates** | E1002, E1007, E1014, E1035, E1046 (10 total records) |
| **Position Mismatches** | E1002 (Operations Lead → Senior Operations Lead) |
| **Bonus Mismatches** | E1007 ($7000 → $8500), E1046 ($5000 → $6200) |
| **Paygrade Mismatches** | E1014 (P4 → P5) |
| **Job Allocation Issues** | E1006, E1033 (empty), E1025 (UNKNOWN) |
| **Leave Violations (>20d)** | E1004 (25d), E1020 (27d), E1050 (28d) |
| **Investigation Statuses** | 42 past_cleared, 5 past_flagged, 6 ongoing |

---

## ❓ **FAQ**

### **Q: Is this using real LangGraph?**
**A**: **YES!** The backend uses the official **LangGraph library** (`langgraph` package):
- ✅ **StateGraph**: Real LangGraph `StateGraph` with typed `AuditState` (TypedDict)
- ✅ **Node functions**: 5 audit nodes (`node_data_integrator`, `node_normalizer`, `node_rules_engine`, `node_policy_check`, `node_summary`)
- ✅ **Edges**: Sequential flow with `.add_edge()` and `set_entry_point()` / `END`
- ✅ **Compiled graph**: `.compile()` returns executable graph
- ✅ **State management**: Typed state with `Annotated[List[str], add]` for accumulating logs
- ✅ **Graph execution**: `.invoke(initial_state)` runs the entire workflow

**Code highlights** (in `backend/app/main.py`):
```python
from langgraph.graph import StateGraph, END

class AuditState(TypedDict):
    run_id: str
    audit_id: str
    audit_name: str
    raw_data: Optional[List[Dict]]
    normalized_data: Optional[List[Dict]]
    final_data: Optional[List[Dict]]
    logs: Annotated[List[str], add]
    current_node: str

workflow = StateGraph(AuditState)
workflow.add_node("data_integrator", node_data_integrator)
workflow.add_node("normalizer", node_normalizer)
# ... add remaining nodes
workflow.set_entry_point("data_integrator")
workflow.add_edge("data_integrator", "normalizer")
# ... add remaining edges
workflow.add_edge("summary", END)
audit_graph = workflow.compile()

# Execute
final_state = audit_graph.invoke(initial_state)
```

**Standalone LangGraph Test**:
```bash
cd backend
python test_langgraph_standalone.py
```

This runs the LangGraph workflow independently (no FastAPI) to verify:
- Graph builds and compiles
- All 5 nodes execute in sequence
- State flows correctly between nodes
- Scratchpads and artifacts are written

### **Q: Where are logs, scratchpads, and outputs stored?**
**A**: All outputs are in `backend/outputs/<run_id>/`:
- **Scratchpads**: `scratchpads/<node_id>.txt` (agent thinking logs)
- **Artifacts**: `artifacts/<name>.json` (structured node outputs)

### **Q: Can I modify rules or add nodes?**
**A**: Yes! In `backend/app/main.py`:
1. Add node definition to `NODES` list
2. Implement logic in `run_flow_thread()` function
3. Write scratchpad with `write_scratchpad(run_id, node_id, lines)`
4. Save artifacts with `write_artifact(run_id, name, data)`
5. Frontend auto-displays the new node

### **Q: How do I demo different scenarios?**
**A**: Edit `HR_Audit_FlatTable.csv`:
- Add duplicate `emp_id` + `emp_name` rows
- Create mismatches (same `emp_id`, different `position`/`bonus`/`paygrade`)
- Set `job_allocation` to empty or `UNKNOWN`
- Set `leave_days_max_streak` > 20
- Change `investigation_status` (past_cleared, past_flagged, ongoing)

Start a new run to see changes reflected.

---

## 🔧 **Extensibility**

### **Backend**
- **Add integrations**: Replace simulated fetches with real Snowflake/API calls in `data_integrator` node
- **Add rules**: Extend `rules_engine` node with custom validation logic
- **Add policies**: Parse real PDFs in `policy_check` node using LangChain document loaders
- **Add LangGraph**: Replace threading with full LangGraph for complex flows (branching, cycles, human-in-the-loop)

### **Frontend**
- **Add modals**: Pop-ups for detailed mismatch records (expandable lists)
- **Add filters**: Filter nodes by status, search scratchpads
- **Add exports**: Download artifacts as CSV/PDF reports
- **Add real-time streaming**: WebSocket for live scratchpad streaming (instead of polling)

---

## 📝 **API Endpoints**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/runs` | Create new audit run |
| `GET` | `/runs` | List all runs |
| `GET` | `/runs/{run_id}/status` | Get run status + node states |
| `GET` | `/runs/{run_id}/nodes` | Get nodes with metadata |
| `GET` | `/runs/{run_id}/nodes/{node_id}/scratchpad` | Get node scratchpad text |
| `GET` | `/runs/{run_id}/artifacts/{name}` | Get artifact JSON |

---

## 🎨 **UI Screenshots (Conceptual)**

### **Left Pane - Node Progress**
- Node cards with:
  - Name (e.g., "Data Integrator")
  - Description (e.g., "Fetches data from sources...")
  - Status badge (pending/running/completed/error)
  - Progress bar (0-100%)

### **Right Pane - Scratchpad**
- Agent thinking logs with timestamps
- Citations, sources, alerts
- For `summary` node: Charts + metrics + lists

---

## 🔐 **Security Notes**
- This is a POC with **no authentication**
- Production systems should add:
  - OAuth/JWT authentication
  - Role-based access control
  - Audit logging
  - Encrypted data storage

---

## 🤝 **Contributing**

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Quick Start for Contributors
```bash
# Fork and clone the repo
git clone https://github.com/YOUR_USERNAME/univar_poc.git
cd univar_poc

# Backend setup
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python test_langgraph_standalone.py

# Frontend setup
cd ../frontend
npm install
npm run dev
```

## 📧 **Contact & Support**

- **Issues**: [GitHub Issues](https://github.com/YOUR_USERNAME/univar_poc/issues)
- **Discussions**: [GitHub Discussions](https://github.com/YOUR_USERNAME/univar_poc/discussions)
- **Pull Requests**: [Contribution Guide](CONTRIBUTING.md)

---

## ✅ **Summary**

This POC demonstrates:
- ✅ **LangGraph-style node execution** with state tracking
- ✅ **Agent scratchpad transparency** (TXT logs per node)
- ✅ **Structured outputs** (JSON artifacts per node)
- ✅ **Beautiful split-pane UI** (left: nodes, right: scratchpad)
- ✅ **Real-time progress tracking** (polling + progress bars)
- ✅ **Demo-ready CSV** with duplicates, mismatches, violations
- ✅ **Charts & summaries** for final insights

**Ready to run!** Follow setup instructions above.

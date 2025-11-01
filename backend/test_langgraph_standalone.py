"""
Standalone LangGraph test script to verify the audit workflow works independently.
Run this without FastAPI to test the LangGraph flow directly.

Usage:
    python test_langgraph_standalone.py
"""

import os
import sys
from pathlib import Path

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.main import build_audit_graph, AuditState, OUTPUTS_DIR

def test_langgraph_standalone():
    """Test LangGraph audit workflow standalone"""
    print("=" * 60)
    print("Testing LangGraph Audit Workflow (Standalone)")
    print("=" * 60)
    
    # Build the graph
    print("\n[1/3] Building LangGraph StateGraph...")
    audit_graph = build_audit_graph()
    print("✓ Graph compiled successfully")
    print(f"  Nodes: {list(audit_graph.nodes.keys())}")
    
    # Initialize state
    run_id = "test_run_001"
    initial_state: AuditState = {
        "run_id": run_id,
        "audit_id": "AUD-STANDALONE-TEST",
        "audit_name": "Standalone LangGraph Test",
        "raw_data": None,
        "normalized_data": None,
        "final_data": None,
        "logs": [],
        "current_node": ""
    }
    
    print(f"\n[2/3] Executing LangGraph workflow (run_id: {run_id})...")
    print("  This will run all 5 nodes sequentially:")
    print("    1. data_integrator → 2. normalizer → 3. rules_engine → 4. policy_check → 5. summary")
    
    # Execute the graph
    final_state = audit_graph.invoke(initial_state)
    
    print("\n✓ Workflow completed successfully!")
    print(f"\n[3/3] Final State:")
    print(f"  Run ID: {final_state['run_id']}")
    print(f"  Audit ID: {final_state['audit_id']}")
    print(f"  Audit Name: {final_state['audit_name']}")
    print(f"  Current Node: {final_state['current_node']}")
    print(f"  Logs ({len(final_state['logs'])} entries):")
    for log in final_state['logs']:
        print(f"    - {log}")
    
    print(f"\n  Data processed:")
    print(f"    Raw records: {len(final_state['raw_data']) if final_state['raw_data'] else 0}")
    print(f"    Normalized records: {len(final_state['normalized_data']) if final_state['normalized_data'] else 0}")
    print(f"    Final records: {len(final_state['final_data']) if final_state['final_data'] else 0}")
    
    # Check outputs
    run_dir = Path(OUTPUTS_DIR) / run_id
    scratch_dir = run_dir / "scratchpads"
    artifact_dir = run_dir / "artifacts"
    
    print(f"\n  Outputs written to: {run_dir}")
    if scratch_dir.exists():
        scratchpads = list(scratch_dir.glob("*.txt"))
        print(f"    Scratchpads ({len(scratchpads)}): {[f.name for f in scratchpads]}")
    if artifact_dir.exists():
        artifacts = list(artifact_dir.glob("*.json"))
        print(f"    Artifacts ({len(artifacts)}): {[f.name for f in artifacts]}")
    
    print("\n" + "=" * 60)
    print("✅ LangGraph standalone test PASSED")
    print("=" * 60)
    print("\nYou can now:")
    print(f"  - Inspect scratchpads: {scratch_dir}")
    print(f"  - Inspect artifacts: {artifact_dir}")
    print("  - Run the full app: uvicorn app.main:app --reload")
    print("=" * 60)

if __name__ == "__main__":
    test_langgraph_standalone()

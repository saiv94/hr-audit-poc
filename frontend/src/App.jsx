import React, { useEffect, useMemo, useState } from 'react'
import { createRun, getArtifact, getNodes, getRuns, getScratchpad, getStatus } from './api'
import RunControls from './components/RunControls'
import NodeList from './components/NodeList'
import Scratchpad from './components/Scratchpad'
import SummaryCharts from './components/SummaryCharts'
import DashboardPopup from './components/DashboardPopup'

const POLL_MS = 1200

export default function App() {
  const [runs, setRuns] = useState([])
  const [activeRunId, setActiveRunId] = useState('')
  const [nodes, setNodes] = useState([])
  const [nodeStates, setNodeStates] = useState({})
  const [selectedNodeId, setSelectedNodeId] = useState('')
  const [scratch, setScratch] = useState('')
  const [summary, setSummary] = useState(null)
  const [showDashboard, setShowDashboard] = useState(false)

  // load runs on mount (but don't auto-select - start blank)
  useEffect(() => {
    (async () => {
      const rs = await getRuns()
      setRuns(rs)
      // Don't auto-select old runs - keep blank until user submits new audit
    })()
  }, [])

  // poll status and nodes for active run (stop when completed)
  useEffect(() => {
    if (!activeRunId) return
    let timer = null
    let stopped = false
    
    const tick = async () => {
      if (stopped) return
      
      const status = await getStatus(activeRunId)
      setNodeStates(status.nodes || {})
      const ns = await getNodes(activeRunId)
      setNodes(ns)
      
      // Update the runs list with the latest status
      setRuns(prevRuns => 
        prevRuns.map(r => 
          r.run_id === activeRunId 
            ? { ...r, status: status.status, timestamp: status.timestamp }
            : r
        )
      )
      
      // Stop polling if run is completed or errored (check status from API response)
      if (status.status === 'completed' || status.status === 'error') {
        console.log(`[Polling] Run ${activeRunId} is ${status.status}, stopping polling`)
        stopped = true
        if (timer) clearInterval(timer)
        return
      }
    }
    
    tick()
    timer = setInterval(tick, POLL_MS)
    return () => {
      stopped = true
      clearInterval(timer)
    }
  }, [activeRunId])  // REMOVED 'runs' to prevent infinite loop when we update runs

  // load scratchpad for selected node
  useEffect(() => {
    if (!activeRunId || !selectedNodeId) return
    (async () => {
      try {
        const s = await getScratchpad(activeRunId, selectedNodeId)
        setScratch(s)
      } catch (e) {
        setScratch('Scratchpad not available yet.')
      }
      // load summary details if summary node
      if (selectedNodeId === 'summary') {
        try {
          const sum = await getArtifact(activeRunId, 'summary')
          setSummary(sum)
        } catch (e) {
          setSummary(null)
        }
      } else {
        setSummary(null)
      }
    })()
  }, [activeRunId, selectedNodeId])

  const onCreateRun = async (audit_id, audit_name) => {
    const r = await createRun({ audit_id, audit_name })
    const rs = await getRuns()
    setRuns(rs)
    setActiveRunId(r.run_id)
    setSelectedNodeId('data_integrator')
  }

  const activeRun = useMemo(() => runs.find(r => r.run_id === activeRunId), [runs, activeRunId])

  return (
    <div className="app">
      <header className="topbar">
        <h1>HR Audit POC</h1>
      </header>
      <div className="controls">
        <RunControls
          runs={runs}
          activeRunId={activeRunId}
          onSelectRun={setActiveRunId}
          onCreateRun={onCreateRun}
        />
        {activeRun && (
          <div className="run-meta">
            <div><strong>Audit ID:</strong> {activeRun.audit_id}</div>
            <div><strong>Name:</strong> {activeRun.audit_name}</div>
            <div><strong>Status:</strong> {activeRun.status}</div>
          </div>
        )}
      </div>
      <div className="panes">
        {!activeRunId ? (
          <div className="blank-state">
            <div className="blank-state-content">
              <div className="blank-state-icon">🚀</div>
              <h2>Welcome to HR Audit POC</h2>
              <p>Click "Submit Audit" above to start a new audit run</p>
              <div className="blank-state-features">
                <div className="feature">✓ Multi-source data integration</div>
                <div className="feature">✓ Automated compliance checking</div>
                <div className="feature">✓ Real-time policy validation</div>
                <div className="feature">✓ Executive dashboard & insights</div>
              </div>
            </div>
          </div>
        ) : (
          <>
            <div className="left-pane">
              <NodeList
                nodes={nodes}
                nodeStates={nodeStates}
                selectedNodeId={selectedNodeId}
                onSelect={setSelectedNodeId}
                onDashboardClick={() => setShowDashboard(true)}
              />
            </div>
            <div className="right-pane">
              <Scratchpad scratch={scratch} nodeId={selectedNodeId} />
              {selectedNodeId === 'summary' && summary && (
                <>
                  <SummaryCharts summary={summary} runId={activeRunId} />
                  <div style={{ marginTop: '16px', textAlign: 'center' }}>
                    <button 
                      className="dashboard-button" 
                      onClick={() => setShowDashboard(true)}
                    >
                      📊 Open Full Dashboard
                    </button>
                  </div>
                </>
              )}
            </div>
          </>
        )}
      </div>
      
      {/* Dashboard Popup */}
      {showDashboard && summary && (
        <DashboardPopup 
          runId={activeRunId} 
          summary={summary} 
          onClose={() => setShowDashboard(false)} 
        />
      )}
    </div>
  )
}

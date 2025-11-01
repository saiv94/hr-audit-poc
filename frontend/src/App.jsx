import React, { useEffect, useMemo, useState } from 'react'
import { createRun, getArtifact, getNodes, getRuns, getScratchpad, getStatus } from './api'
import RunControls from './components/RunControls'
import NodeList from './components/NodeList'
import Scratchpad from './components/Scratchpad'
import SummaryCharts from './components/SummaryCharts'

const POLL_MS = 1200

export default function App() {
  const [runs, setRuns] = useState([])
  const [activeRunId, setActiveRunId] = useState('')
  const [nodes, setNodes] = useState([])
  const [nodeStates, setNodeStates] = useState({})
  const [selectedNodeId, setSelectedNodeId] = useState('')
  const [scratch, setScratch] = useState('')
  const [summary, setSummary] = useState(null)

  // load runs on mount
  useEffect(() => {
    (async () => {
      const rs = await getRuns()
      setRuns(rs)
      if (rs.length && !activeRunId) setActiveRunId(rs[0].run_id)
    })()
  }, [])

  // poll status and nodes for active run
  useEffect(() => {
    if (!activeRunId) return
    let timer = null
    const tick = async () => {
      const status = await getStatus(activeRunId)
      setNodeStates(status.nodes || {})
      const ns = await getNodes(activeRunId)
      setNodes(ns)
    }
    tick()
    timer = setInterval(tick, POLL_MS)
    return () => clearInterval(timer)
  }, [activeRunId])

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
        <div className="left-pane">
          <NodeList
            nodes={nodes}
            nodeStates={nodeStates}
            selectedNodeId={selectedNodeId}
            onSelect={setSelectedNodeId}
          />
        </div>
        <div className="right-pane">
          <Scratchpad scratch={scratch} nodeId={selectedNodeId} />
          {selectedNodeId === 'summary' && summary && (
            <SummaryCharts summary={summary} runId={activeRunId} />
          )}
        </div>
      </div>
    </div>
  )
}

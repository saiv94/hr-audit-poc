import React, { useState } from 'react'

export default function RunControls({ runs, activeRunId, onSelectRun, onCreateRun }) {
  const [auditId, setAuditId] = useState('AUD-'+Math.random().toString(36).slice(2,6).toUpperCase())
  const [auditName, setAuditName] = useState('Quarterly HR Audit')

  return (
    <div className="run-controls">
      <div className="group">
        <label>Audit ID</label>
        <input value={auditId} onChange={e=>setAuditId(e.target.value)} placeholder="e.g., AUD-2025Q4" />
      </div>
      <div className="group">
        <label>Audit Name</label>
        <input value={auditName} onChange={e=>setAuditName(e.target.value)} placeholder="e.g., Q4 Audit" />
      </div>
      <button onClick={()=>onCreateRun(auditId, auditName)}>Start Audit</button>

      <div className="group" style={{marginLeft: 'auto'}}>
        <label>Previous Runs</label>
        <select value={activeRunId} onChange={(e)=>onSelectRun(e.target.value)}>
          {runs.map(r => (
            <option key={r.run_id} value={r.run_id}>{r.run_id} - {r.audit_name}</option>
          ))}
        </select>
      </div>
    </div>
  )
}

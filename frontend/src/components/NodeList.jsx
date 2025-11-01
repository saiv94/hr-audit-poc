import React from 'react'

function ProgressBar({ value=0 }) {
  return (
    <div className="progress">
      <div className="bar" style={{ width: `${value}%` }} />
    </div>
  )
}

export default function NodeList({ nodes, nodeStates, selectedNodeId, onSelect }) {
  return (
    <div className="node-list">
      {nodes.map(n => {
        const st = nodeStates[n.id] || { progress: 0, status: 'pending' }
        return (
          <div key={n.id} className={`node-card ${selectedNodeId===n.id ? 'selected':''}`} onClick={()=>onSelect(n.id)}>
            <div className="node-header">
              <div className="node-title">{n.name}</div>
              <div className={`status ${st.status}`}>{st.status}</div>
            </div>
            <div className="node-desc">{n.desc}</div>
            <ProgressBar value={st.progress} />
          </div>
        )
      })}
    </div>
  )
}

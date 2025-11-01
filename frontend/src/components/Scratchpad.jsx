import React from 'react'

export default function Scratchpad({ scratch, nodeId }) {
  return (
    <div className="scratchpad">
      <div className="scratchpad-header">
        <h3>Agent Scratchpad {nodeId ? `- ${nodeId}`:''}</h3>
      </div>
      <pre className="scratchpad-body">{scratch}</pre>
    </div>
  )
}

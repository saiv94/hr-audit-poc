import React from 'react'
import ScratchpadCards from './ScratchpadCards'

export default function Scratchpad({ scratch, nodeId }) {
  return (
    <div className="scratchpad">
      <div className="scratchpad-header">
        <h3 style={{ margin: 0, fontSize: '16px', fontWeight: 600 }}>
          📊 Insights Panel {nodeId ? `- ${nodeId}` : ''}
        </h3>
      </div>
      <ScratchpadCards text={scratch} nodeId={nodeId} />
    </div>
  )
}

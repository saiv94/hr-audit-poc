import React from 'react'

function ProgressBar({ value=0 }) {
  return (
    <div className="progress">
      <div className="bar" style={{ width: `${value}%` }} />
    </div>
  )
}

export default function NodeList({ nodes, nodeStates, selectedNodeId, onSelect }) {
  const getStatusIcon = (status) => {
    if (status === 'completed') return '✓'
    if (status === 'running') return '⟳'
    if (status === 'error') return '✗'
    return '○'
  }
  
  const formatTime = (timestamp) => {
    if (!timestamp) return ''
    const date = new Date(timestamp)
    return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  }
  
  const getStatusMessage = (nodeId, status) => {
    if (status !== 'completed') return null
    
    const messages = {
      'data_integrator': '✅ Successfully pulled 1,245 records | Fetched from 4 parallel sources',
      'normalizer': '✅ Schema standardized | All fields validated',
      'rules_engine': '✅ Business rules applied | Duplicates removed, mismatches flagged',
      'policy_check': '✅ Compliance verified | Violations reported to managers'
    }
    
    return messages[nodeId]
  }

  return (
    <div className="node-list">
      {nodes.map((n, idx) => {
        const st = nodeStates[n.id] || { progress: 0, status: 'pending' }
        const statusMsg = getStatusMessage(n.id, st.status)
        
        return (
          <React.Fragment key={n.id}>
            <div 
              className={`node-card ${st.status} ${selectedNodeId===n.id ? 'selected':''}`}
              onClick={()=>onSelect(n.id)}
            >
              <div className="node-header">
                <div className="node-title">{n.name}</div>
                <div className={`status ${st.status}`}>
                  {getStatusIcon(st.status)} {st.status} {st.status === 'running' ? `(${st.progress}%)` : ''}
                </div>
              </div>
              <div className="node-desc">{n.desc}</div>
              {st.timestamp && (
                <div className="node-time">
                  🕐 {formatTime(st.timestamp)}
                </div>
              )}
              <ProgressBar value={st.progress} />
            </div>
            
            {statusMsg && (
              <div className="node-status-message">
                {statusMsg}
              </div>
            )}
            
            {/* Show final message after last node */}
            {idx === nodes.length - 1 && st.status === 'completed' && (
              <div className="node-status-message final">
                🎉 <strong>Audit Completed!</strong> Check the dashboard for comprehensive insights.
              </div>
            )}
          </React.Fragment>
        )
      })}
    </div>
  )
}

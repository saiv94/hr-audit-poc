import React, { useState } from 'react'
import { MetricCard, ProgressCircle, StatGrid, SimpleBarChart, StatusBadge } from './MetricCard'

// Rich text formatter component
function RichText({ text }) {
  if (!text) return null
  
  const lines = text.split('\n')
  const elements = []
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    
    // Skip empty lines
    if (!line.trim()) {
      elements.push(<br key={i} />)
      continue
    }
    
    // H1: # Heading - Large brown/dark
    if (line.startsWith('# ')) {
      elements.push(<h1 key={i} style={{ fontSize: '20px', fontWeight: '700', margin: '16px 0 8px 0', color: '#78350f', background: 'linear-gradient(135deg, #fef3c7 0%, #fde68a 100%)', padding: '8px 12px', borderRadius: '8px', borderLeft: '4px solid #f59e0b' }}>{line.substring(2)}</h1>)
    }
    // H2: ## Heading - Medium blue
    else if (line.startsWith('## ')) {
      elements.push(<h2 key={i} style={{ fontSize: '16px', fontWeight: '600', margin: '12px 0 6px 0', color: '#1e40af', borderBottom: '2px solid #3b82f6', paddingBottom: '4px' }}>{line.substring(3)}</h2>)
    }
    // H3: ### Heading - Teal
    else if (line.startsWith('### ')) {
      elements.push(<h3 key={i} style={{ fontSize: '14px', fontWeight: '600', margin: '10px 0 4px 0', color: '#0e7490' }}>{line.substring(4)}</h3>)
    }
    // Table row
    else if (line.startsWith('|') && line.endsWith('|')) {
      // Collect table rows
      const tableRows = [line]
      let j = i + 1
      while (j < lines.length && lines[j].startsWith('|')) {
        tableRows.push(lines[j])
        j++
      }
      i = j - 1
      
      // Parse table
      const isHeaderSeparator = (row) => /^\|[\s\-:]+\|$/.test(row)
      const headers = tableRows[0].split('|').filter(c => c.trim()).map(c => c.trim())
      const dataRows = tableRows.slice(2).filter(r => !isHeaderSeparator(r)).map(r => 
        r.split('|').filter(c => c.trim()).map(c => c.trim())
      )
      
      elements.push(
        <table key={i} style={{ width: '100%', borderCollapse: 'separate', borderSpacing: '0', margin: '12px 0', fontSize: '12px', border: '2px solid #3b82f6', borderRadius: '8px', overflow: 'hidden', boxShadow: '0 2px 8px rgba(59,130,246,0.1)' }}>
          <thead>
            <tr style={{ background: 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)', color: 'white' }}>
              {headers.map((h, idx) => <th key={idx} style={{ padding: '10px 12px', textAlign: 'left', fontWeight: '600', fontSize: '13px' }}>{formatInline(h)}</th>)}
            </tr>
          </thead>
          <tbody>
            {dataRows.map((row, ridx) => (
              <tr key={ridx} style={{ backgroundColor: ridx % 2 === 0 ? '#f0f9ff' : '#ffffff', borderBottom: '1px solid #dbeafe' }}>
                {row.map((cell, cidx) => <td key={cidx} style={{ padding: '10px 12px', color: '#1e293b', fontWeight: cidx === 0 ? '600' : '400' }}>{formatInline(cell)}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      )
    }
    // Horizontal rule
    else if (line === '---' || line.startsWith('===') || line.startsWith('───')) {
      elements.push(<hr key={i} style={{ border: 'none', borderTop: '2px solid #e2e8f0', margin: '16px 0' }} />)
    }
    // Code block
    else if (line.startsWith('```')) {
      const codeLines = []
      let j = i + 1
      while (j < lines.length && !lines[j].startsWith('```')) {
        codeLines.push(lines[j])
        j++
      }
      i = j
      elements.push(
        <pre key={i} style={{ 
          background: '#f1f5f9', 
          padding: '12px', 
          borderRadius: '6px', 
          fontSize: '11px', 
          overflow: 'auto',
          border: '1px solid #cbd5e1'
        }}>
          <code>{codeLines.join('\n')}</code>
        </pre>
      )
    }
    // List items starting with - or •
    else if (line.trim().startsWith('- ') || line.trim().startsWith('• ')) {
      const content = line.trim().substring(2)
      let icon = '▸'
      let color = '#3b82f6'
      
      // Determine icon and color based on content
      if (content.includes('✅') || content.includes('Success') || content.includes('PASS')) {
        icon = '✅'
        color = '#059669'
      } else if (content.includes('⚠️') || content.includes('Warning') || content.includes('Issue')) {
        icon = '⚠️'
        color = '#f59e0b'
      } else if (content.includes('🔴') || content.includes('Error') || content.includes('FAIL')) {
        icon = '🔴'
        color = '#ef4444'
      } else if (content.includes('**')) {
        icon = '◆'
        color = '#6366f1'
      }
      
      elements.push(
        <div key={i} style={{ marginBottom: '6px', marginLeft: '16px', display: 'flex', gap: '8px', alignItems: 'flex-start' }}>
          <span style={{ color: color, fontWeight: 'bold', fontSize: '14px', marginTop: '2px' }}>{icon}</span>
          <span style={{ flex: 1, color: '#334155', lineHeight: '1.6' }}>{formatInline(content)}</span>
        </div>
      )
    }
    // Regular line with inline formatting
    else {
      elements.push(<div key={i} style={{ marginBottom: '4px', lineHeight: '1.6', color: '#475569' }}>{formatInline(line)}</div>)
    }
  }
  
  return <div>{elements}</div>
}

// Format inline markdown: **bold**, [links](url)
function formatInline(text) {
  if (!text) return text
  
  const parts = []
  let remaining = text
  let key = 0
  
  while (remaining.length > 0) {
    // Bold: **text**
    const boldMatch = remaining.match(/\*\*([^*]+)\*\*/)
    if (boldMatch) {
      const before = remaining.substring(0, boldMatch.index)
      if (before) parts.push(<span key={key++}>{before}</span>)
      parts.push(<strong key={key++} style={{ fontWeight: '700', color: '#78350f' }}>{boldMatch[1]}</strong>)
      remaining = remaining.substring(boldMatch.index + boldMatch[0].length)
      continue
    }
    
    // Links: [text](url)
    const linkMatch = remaining.match(/\[([^\]]+)\]\(([^)]+)\)/)
    if (linkMatch) {
      const before = remaining.substring(0, linkMatch.index)
      if (before) parts.push(<span key={key++}>{before}</span>)
      parts.push(<a key={key++} href={linkMatch[2]} target="_blank" rel="noopener noreferrer" style={{ color: '#3b82f6', textDecoration: 'underline' }}>{linkMatch[1]}</a>)
      remaining = remaining.substring(linkMatch.index + linkMatch[0].length)
      continue
    }
    
    // No more matches
    parts.push(<span key={key++}>{remaining}</span>)
    break
  }
  
  return parts.length > 0 ? parts : text
}

function ScratchCard({ title, content, icon, type, metrics, expandable = true, nodeId }) {
  const [expanded, setExpanded] = useState(false)
  
  // Extract visual metrics from content
  const extractVisualMetrics = (text, cardTitle) => {
    if (!text) return null
    
    // Check if this card should render as visual metrics
    if (cardTitle.includes('SNOWFLAKE') || cardTitle.includes('API') || cardTitle.includes('DRIVE') || cardTitle.includes('CSV')) {
      // Parse data source metrics
      const recordsMatch = text.match(/(\d+,?\d+)\s*(?:records|rows)/i)
      const columnsMatch = text.match(/(\d+)\s*(?:columns|fields)/i)
      const sizeMatch = text.match(/([\d.]+)\s*(?:MB|KB|GB)/i)
      
      if (recordsMatch || columnsMatch || sizeMatch) {
        return {
          type: 'datasource',
          metrics: [
            recordsMatch && { value: recordsMatch[1], label: 'Records', icon: '📊', color: 'blue', description: 'Total rows fetched' },
            columnsMatch && { value: columnsMatch[1], label: 'Columns', icon: '📋', color: 'cyan', description: 'Data fields' },
            sizeMatch && { value: sizeMatch[0], label: 'Size', icon: '💾', color: 'purple', description: 'Data volume' }
          ].filter(Boolean)
        }
      }
    }
    
    if (cardTitle.includes('DUPLICATE')) {
      const duplicatesMatch = text.match(/(\d+)\s*(?:records|rows|duplicates)/i)
      const uniqueMatch = text.match(/(\d+)\s*(?:Unique|records.*Unique)/i)
      const percentMatch = text.match(/([\d.]+)%/i)
      
      return {
        type: 'quality',
        duplicates: duplicatesMatch ? duplicatesMatch[1] : '0',
        unique: uniqueMatch ? uniqueMatch[1] : '0',
        percent: percentMatch ? parseFloat(percentMatch[1]) : 0
      }
    }
    
    if (cardTitle.includes('MISMATCH')) {
      const matches = []
      const positionMatch = text.match(/Position.*?(\d+)/i)
      const bonusMatch = text.match(/Bonus.*?(\d+)/i)
      const paygradeMatch = text.match(/Paygrade.*?(\d+)/i)
      
      if (positionMatch) matches.push({ label: 'Position', value: parseInt(positionMatch[1]), color: '#f59e0b' })
      if (bonusMatch) matches.push({ label: 'Bonus', value: parseInt(bonusMatch[1]), color: '#f97316' })
      if (paygradeMatch) matches.push({ label: 'Paygrade', value: parseInt(paygradeMatch[1]), color: '#ef4444' })
      
      return { type: 'mismatch', data: matches }
    }
    
    if (cardTitle.includes('INVESTIGATION')) {
      const clearedMatch = text.match(/Past Cleared.*?(\d+)/i)
      const flaggedMatch = text.match(/Past Flagged.*?(\d+)/i)
      const ongoingMatch = text.match(/Ongoing.*?(\d+)/i)
      
      return {
        type: 'investigation',
        data: [
          clearedMatch && { label: 'Cleared', value: parseInt(clearedMatch[1]), color: '#10b981', icon: '✓' },
          flaggedMatch && { label: 'Flagged', value: parseInt(flaggedMatch[1]), color: '#f59e0b', icon: '⚠' },
          ongoingMatch && { label: 'Ongoing', value: parseInt(ongoingMatch[1]), color: '#ef4444', icon: '🔍' }
        ].filter(Boolean)
      }
    }
    
    if (cardTitle.includes('VALIDATION') || cardTitle.includes('COMPLIANCE')) {
      const rateMatch = text.match(/([\d.]+)%.*?(?:Compliant|Success|Pass)/i)
      const violationsMatch = text.match(/(\d+).*?(?:Violations|Issues|Errors)/i)
      
      return {
        type: 'compliance',
        rate: rateMatch ? parseFloat(rateMatch[1]) : 0,
        violations: violationsMatch ? parseInt(violationsMatch[1]) : 0
      }
    }
    
    return null
  }
  
  const visualData = extractVisualMetrics(content, title)

  // Render visual metrics if available
  if (visualData) {
    if (visualData.type === 'datasource') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <span className="visual-title">{title}</span>
          </div>
          <div className="metrics-row">
            {visualData.metrics.map((m, i) => (
              <MetricCard 
                key={i} 
                value={m.value} 
                label={m.label} 
                icon={m.icon} 
                color={m.color}
                description={m.description}
              />
            ))}
          </div>
        </div>
      )
    }
    
    if (visualData.type === 'quality') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <span className="visual-title">{title}</span>
          </div>
          <div className="quality-grid">
            <MetricCard 
              value={visualData.duplicates} 
              label="Duplicates" 
              icon="🔴" 
              color="red"
              description="Repeated records identified"
            />
            <MetricCard 
              value={visualData.unique} 
              label="Unique" 
              icon="✓" 
              color="green"
              description="Distinct employee records"
            />
            <ProgressCircle percentage={100 - visualData.percent} label="Data Quality Score" color="#10b981" />
          </div>
        </div>
      )
    }
    
    if (visualData.type === 'mismatch') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <div>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Conflicting data across employees</div>
            </div>
          </div>
          <SimpleBarChart data={visualData.data} />
        </div>
      )
    }
    
    if (visualData.type === 'investigation') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <div>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Employee compliance tracking</div>
            </div>
          </div>
          <StatGrid stats={visualData.data} />
        </div>
      )
    }
    
    if (visualData.type === 'compliance') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <div>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Leave policy adherence check</div>
            </div>
          </div>
          <div className="compliance-grid">
            <ProgressCircle percentage={visualData.rate} label="Compliance Rate" color="#3b82f6" />
            <MetricCard 
              value={visualData.violations} 
              label="Violations" 
              icon="⚠️" 
              color="orange"
              description="Policy breaches found"
            />
          </div>
        </div>
      )
    }
  }
  
  // Fallback to text-based card
  return (
    <div className={`scratch-card-modern ${type}`}>
      <div className="scratch-card-modern-header">
        <div className="scratch-card-modern-icon-wrapper">
          <div className={`scratch-card-modern-icon ${type}`}>{icon}</div>
        </div>
        <div className="scratch-card-modern-title-area">
          <div className="scratch-card-modern-title">{title}</div>
        </div>
        <div className="scratch-card-modern-actions">
          {expandable && (
            <button 
              className="action-btn"
              onClick={(e) => {
                e.stopPropagation()
                setExpanded(!expanded)
              }}
            >
              {expanded ? '▼' : '▶'}
            </button>
          )}
        </div>
      </div>
      {expanded && (
        <div className="scratch-card-modern-content">
          <RichText text={content} />
        </div>
      )}
    </div>
  )
}

function parseScratchpadToCards(text, nodeId) {
  if (!text) return []
  
  const cards = []
  const sections = text.split('##CARD##')
  
  // First section (before any cards) - general description/intro
  // Display as beautiful formatted text, not a card
  if (sections[0] && sections[0].trim()) {
    const intro = sections[0].trim()
    cards.push({
      title: 'Overview',
      content: intro,
      icon: 'ℹ️',
      type: 'overview', // Special type for non-card display
      metrics: [],
      expandable: false
    })
  }
  
  // Parse each card section
  for (let i = 1; i < sections.length; i++) {
    const section = sections[i].trim()
    if (!section) continue
    
    // Extract title from first line
    const lines = section.split('\n')
    const titleLine = lines[0].trim()
    const content = lines.slice(1).join('\n').trim()
    
    // Determine icon and type from title
    let icon = '📝'
    let type = 'info'
    
    if (titleLine.includes('SNOWFLAKE') || titleLine.includes('DATABASE')) {
      icon = '🗄️'
      type = 'source'
    } else if (titleLine.includes('API') || titleLine.includes('HRMS')) {
      icon = '🌐'
      type = 'source'
    } else if (titleLine.includes('DRIVE') || titleLine.includes('FILE')) {
      icon = '📁'
      type = 'source'
    } else if (titleLine.includes('CSV') || titleLine.includes('LOCAL')) {
      icon = '💾'
      type = 'source'
    } else if (titleLine.includes('SCHEMA') || titleLine.includes('NORMALI')) {
      icon = '⚙️'
      type = 'data'
    } else if (titleLine.includes('DUPLICATE') || titleLine.includes('RULE')) {
      icon = '🔍'
      type = 'rule'
    } else if (titleLine.includes('MISMATCH') || titleLine.includes('ALERT')) {
      icon = '⚠️'
      type = 'alert'
    } else if (titleLine.includes('POLICY') || titleLine.includes('COMPLIANCE')) {
      icon = '📜'
      type = 'policy'
    } else if (titleLine.includes('SUMMARY') || titleLine.includes('REPORT')) {
      icon = '📈'
      type = 'info'
    }
    
    cards.push({
      title: titleLine,
      content: content,
      icon: icon,
      type: type,
      metrics: [],
      expandable: true
    })
  }
  
  // If no cards were created, show full text as one card
  if (cards.length === 0) {
    cards.push({
      title: 'Agent Logs',
      content: text,
      icon: '📝',
      type: 'info',
      metrics: [],
      expandable: true
    })
  }
  
  return cards
}

// Popup Modal Component
function PopupModal({ title, content, onClose }) {
  return (
    <div className="popup-overlay" onClick={onClose}>
      <div className="popup-modal" onClick={(e) => e.stopPropagation()}>
        <div className="popup-header">
          <h3>{title}</h3>
          <button className="popup-close" onClick={onClose}>✕</button>
        </div>
        <div className="popup-content">
          <RichText text={content} />
        </div>
      </div>
    </div>
  )
}

export default function ScratchpadCards({ text, nodeId }) {
  const [popupData, setPopupData] = useState(null)
  const cards = parseScratchpadToCards(text, nodeId)
  
  if (!text) {
    return (
      <div className="scratchpad-body">
        <div style={{ textAlign: 'center', color: '#8a93a6', padding: '40px 20px' }}>
          <div style={{ fontSize: '48px', marginBottom: '12px' }}>📋</div>
          <div>No data available</div>
          <div style={{ fontSize: '12px', marginTop: '8px' }}>Run will generate insights here</div>
        </div>
      </div>
    )
  }
  
  return (
    <div className="scratchpad-body">
      <div className="scratchpad-cards">
        {cards.map((card, idx) => {
          // Render overview as compact visual summary
          if (card.type === 'overview') {
            // Extract key info from overview
            const lines = card.content.split('\n').filter(l => l.trim())
            const heading = lines.find(l => l.startsWith('# ')) || ''
            const description = lines.find(l => !l.startsWith('#') && !l.startsWith('=') && l.length > 20) || ''
            
            return (
              <div key={idx} className="overview-visual">
                <div className="overview-heading">{heading.replace('# ', '')}</div>
                {description && <div className="overview-desc">{description}</div>}
              </div>
            )
          }
          
          // Check if this is a summary/reference card (should be clickable popup)
          const isSummaryCard = card.title.includes('SUMMARY') || 
                                card.title.includes('REFERENCE') || 
                                card.title.includes('Sample Records') ||
                                card.title.includes('KEY INSIGHTS')
          
          if (isSummaryCard) {
            return (
              <div key={idx} className="popup-trigger-card" onClick={() => setPopupData(card)}>
                <div className="popup-trigger-content">
                  <span className="popup-trigger-icon">📊</span>
                  <span className="popup-trigger-title">{card.title}</span>
                  <span className="popup-trigger-arrow">→</span>
                </div>
              </div>
            )
          }
          
          return (
            <ScratchCard
              key={idx}
              title={card.title}
              content={card.content}
              icon={card.icon}
              type={card.type}
              metrics={card.metrics}
              expandable={card.expandable !== false}
              nodeId={nodeId}
            />
          )
        })}
      </div>
      
      {popupData && (
        <PopupModal 
          title={popupData.title}
          content={popupData.content}
          onClose={() => setPopupData(null)}
        />
      )}
    </div>
  )
}

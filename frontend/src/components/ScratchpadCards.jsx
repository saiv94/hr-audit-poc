import React, { useState, useEffect } from 'react'
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
      parts.push(<strong key={key++} style={{ fontWeight: '600', color: '#78350f', fontSize: 'inherit' }}>{boldMatch[1]}</strong>)
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

function ScratchCard({ title, content, icon, type, metrics, expandable = true, nodeId, detailsContent, onShowDetails }) {
  const [expanded, setExpanded] = useState(false)
  
  // Extract visual metrics from content
  const extractVisualMetrics = (text, cardTitle) => {
    if (!text) return null
    
    // Check if this card should render as visual metrics
    if (cardTitle.includes('SNOWFLAKE') || cardTitle.includes('API') || cardTitle.includes('DRIVE') || cardTitle.includes('CSV')) {
      // Parse data source metrics - support both "Records: 3,000" and "3,000 records" formats
      const recordsMatch = text.match(/(?:Records?|Rows?):\s*([\d,]+)|(\d+,?\d*)\s*(?:records|rows)/i)
      const columnsMatch = text.match(/(?:Columns?|Fields?):\s*([\d,]+)|(\d+)\s*(?:columns|fields)/i)
      const sizeMatch = text.match(/(?:Size|Data):\s*([\d.]+\s*(?:MB|KB|GB))|([\d.]+\s*(?:MB|KB|GB))/i)
      const tablesMatch = text.match(/(?:Tables?|Files?):\s*(\d+)|(\d+)\s*(?:tables?|files?)/i)
      
      if (recordsMatch || columnsMatch || sizeMatch || tablesMatch) {
        // Determine labels dynamically based on what was matched
        const columnsLabel = text.includes('Fields:') ? 'Fields' : 'Columns'
        const tablesLabel = text.includes('Files:') ? 'Files' : 'Tables'
        
        return {
          type: 'datasource',
          metrics: [
            recordsMatch && { value: recordsMatch[1] || recordsMatch[2], label: 'Records', icon: '📊', color: 'blue', description: 'Total rows fetched' },
            columnsMatch && { value: columnsMatch[1] || columnsMatch[2], label: columnsLabel, icon: '📋', color: 'cyan', description: 'Data fields' },
            sizeMatch && { value: sizeMatch[1] || sizeMatch[2], label: 'Size', icon: '💾', color: 'purple', description: 'Data volume' },
            tablesMatch && { value: tablesMatch[1] || tablesMatch[2], label: tablesLabel, icon: '📁', color: 'green', description: 'Data sources' }
          ].filter(Boolean)
        }
      }
    }
    
    if (cardTitle.includes('DUPLICATE')) {
      // Look for KEY_METRICS line first
      const keyMetricsMatch = text.match(/KEY_METRICS:.*?Duplicates=(\d+).*?Unique=(\d+).*?QualityScore=(\d+)/i)
      
      if (keyMetricsMatch) {
        return {
          type: 'quality',
          duplicates: keyMetricsMatch[1],
          unique: keyMetricsMatch[2],
          percent: parseFloat(keyMetricsMatch[3]),
          description: text
        }
      }
      
      // Fallback to old parsing
      const duplicatesMatch = text.match(/(\d+)\s*Duplicates/i)
      const uniqueMatch = text.match(/(\d+)\s*Unique/i)
      const percentMatch = text.match(/([\d.]+)%.*?Quality/i)
      
      return {
        type: 'quality',
        duplicates: duplicatesMatch ? duplicatesMatch[1] : '0',
        unique: uniqueMatch ? uniqueMatch[1] : '0',
        percent: percentMatch ? parseFloat(percentMatch[1]) : 0,
        description: text
      }
    }
    
    if (cardTitle.includes('MISMATCH')) {
      // Look for KEY_METRICS line first
      const keyMetricsMatch = text.match(/KEY_METRICS:.*?Position=(\d+).*?Bonus=(\d+).*?Paygrade=(\d+)/i)
      
      const matches = []
      if (keyMetricsMatch) {
        matches.push({ label: 'Position', value: parseInt(keyMetricsMatch[1]), color: '#f59e0b' })
        matches.push({ label: 'Bonus', value: parseInt(keyMetricsMatch[2]), color: '#f97316' })
        matches.push({ label: 'Paygrade', value: parseInt(keyMetricsMatch[3]), color: '#ef4444' })
      } else {
        // Fallback parsing
        const positionMatch = text.match(/Position=(\d+)|Position.*?(\d+)/i)
        const bonusMatch = text.match(/Bonus=(\d+)|Bonus.*?(\d+)/i)
        const paygradeMatch = text.match(/Paygrade=(\d+)|Paygrade.*?(\d+)/i)
        
        if (positionMatch) matches.push({ label: 'Position', value: parseInt(positionMatch[1] || positionMatch[2]), color: '#f59e0b' })
        if (bonusMatch) matches.push({ label: 'Bonus', value: parseInt(bonusMatch[1] || bonusMatch[2]), color: '#f97316' })
        if (paygradeMatch) matches.push({ label: 'Paygrade', value: parseInt(paygradeMatch[1] || paygradeMatch[2]), color: '#ef4444' })
      }
      
      return { type: 'mismatch', data: matches, description: text }
    }
    
    if (cardTitle.includes('INVESTIGATION')) {
      // Look for KEY_METRICS line first
      const keyMetricsMatch = text.match(/KEY_METRICS:.*?Cleared=(\d+).*?Flagged=(\d+).*?Ongoing=(\d+)/i)
      
      if (keyMetricsMatch) {
        return {
          type: 'investigation',
          data: [
            { label: 'Cleared', value: parseInt(keyMetricsMatch[1]), color: '#10b981', icon: '✓' },
            { label: 'Flagged', value: parseInt(keyMetricsMatch[2]), color: '#f59e0b', icon: '⚠' },
            { label: 'Ongoing', value: parseInt(keyMetricsMatch[3]), color: '#ef4444', icon: '🔍' }
          ],
          description: text
        }
      }
      
      // Fallback parsing
      const clearedMatch = text.match(/Cleared=(\d+)|Past Cleared.*?(\d+)/i)
      const flaggedMatch = text.match(/Flagged=(\d+)|Past Flagged.*?(\d+)/i)
      const ongoingMatch = text.match(/Ongoing=(\d+)|Ongoing.*?(\d+)/i)
      
      return {
        type: 'investigation',
        data: [
          clearedMatch && { label: 'Cleared', value: parseInt(clearedMatch[1] || clearedMatch[2]), color: '#10b981', icon: '✓' },
          flaggedMatch && { label: 'Flagged', value: parseInt(flaggedMatch[1] || flaggedMatch[2]), color: '#f59e0b', icon: '⚠' },
          ongoingMatch && { label: 'Ongoing', value: parseInt(ongoingMatch[1] || ongoingMatch[2]), color: '#ef4444', icon: '🔍' }
        ].filter(Boolean),
        description: text
      }
    }
    
    // Schema validation results (Node 2) - show as quality metrics
    if (cardTitle.includes('VALIDATION RESULTS')) {
      // Look for KEY_METRICS line first
      const keyMetricsMatch = text.match(/KEY_METRICS:.*?Integrity=(\d+).*?Records=(\d+).*?Nulls=(\d+)/i)
      
      if (keyMetricsMatch) {
        return {
          type: 'validation',
          integrity: parseInt(keyMetricsMatch[1]),
          records: keyMetricsMatch[2],
          nulls: parseInt(keyMetricsMatch[3]),
          description: text
        }
      }
      
      // Fallback parsing
      const integrityMatch = text.match(/Integrity=(\d+)|(\d+)%.*?Data Integrity/i)
      const totalRecordsMatch = text.match(/Records=(\d+)|Total Records.*?(\d+,?\d*)/i)
      const nullsMatch = text.match(/Nulls=(\d+)|Null Values.*?(\d+)/i)
      
      return {
        type: 'validation',
        integrity: integrityMatch ? parseInt(integrityMatch[1] || integrityMatch[2]) : 0,
        records: totalRecordsMatch ? (totalRecordsMatch[1] || totalRecordsMatch[2]) : '0',
        nulls: nullsMatch ? parseInt(nullsMatch[1] || nullsMatch[2]) : 0,
        description: text
      }
    }
    
    // Policy compliance check (Node 4) - show as compliance gauge
    // Exclude "COMPREHENSIVE COMPLIANCE CHECKS" - that should be a table, not visual
    if ((cardTitle.includes('LEAVE POLICY') || (cardTitle.includes('COMPLIANCE') && !cardTitle.includes('COMPREHENSIVE')))) {
      // Look for KEY_METRICS line first
      const keyMetricsMatch = text.match(/KEY_METRICS:.*?Compliant=(\d+).*?Violations=(\d+).*?Rate=([\d.]+)/i)
      
      if (keyMetricsMatch) {
        return {
          type: 'compliance',
          rate: parseFloat(keyMetricsMatch[3]),
          violations: parseInt(keyMetricsMatch[2]),
          compliant: parseInt(keyMetricsMatch[1]),
          description: text
        }
      }
      
      // Fallback parsing
      const rateMatch = text.match(/Rate=([\d.]+)|([\d.]+)%.*?Compliance Rate/i)
      const violationsMatch = text.match(/Violations=(\d+)|(\d+).*?Violations/i)
      const compliantMatch = text.match(/Compliant=(\d+)|(\d+).*?Compliant/i)
      
      return {
        type: 'compliance',
        rate: rateMatch ? parseFloat(rateMatch[1] || rateMatch[2]) : 0,
        violations: violationsMatch ? parseInt(violationsMatch[1] || violationsMatch[2]) : 0,
        compliant: compliantMatch ? parseInt(compliantMatch[1] || compliantMatch[2]) : 0,
        description: text
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
            {detailsContent && (
              <button 
                className="action-btn show-details-btn"
                onClick={(e) => {
                  e.stopPropagation()
                  onShowDetails && onShowDetails()
                }}
                title="Show Details"
                style={{ marginLeft: 'auto' }}
              >
                🔍
              </button>
            )}
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
            <div style={{ flex: 1 }}>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Scans CSV for repeated employee records (same ID + Name)</div>
            </div>
            {detailsContent && (
              <button 
                className="action-btn show-details-btn"
                onClick={(e) => {
                  e.stopPropagation()
                  onShowDetails && onShowDetails()
                }}
                title="Show Details"
              >
                🔍
              </button>
            )}
          </div>
          <div className="quality-grid">
            <MetricCard 
              value={visualData.duplicates} 
              label="Duplicates Found" 
              icon="🔴" 
              color="red"
              description="Same person appearing multiple times in CSV"
            />
            <MetricCard 
              value={visualData.unique} 
              label="Unique Employees" 
              icon="✓" 
              color="green"
              description="Actual distinct people (real headcount)"
            />
            <ProgressCircle 
              percentage={visualData.percent} 
              label="Data Quality Score" 
              color="#10b981" 
            />
          </div>
          <div className="visual-explanation">
            <strong>What this means:</strong> If John Doe appears 3 times → 2 duplicates. We keep 1, remove 2. 
            Quality Score = {visualData.percent}% = {visualData.unique} unique / total rows. Higher is better!
          </div>
        </div>
      )
    }
    
    if (visualData.type === 'mismatch') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <div style={{ flex: 1 }}>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Finds employees with conflicting Position/Bonus/Paygrade in CSV</div>
            </div>
            {detailsContent && (
              <button 
                className="action-btn show-details-btn"
                onClick={(e) => {
                  e.stopPropagation()
                  onShowDetails && onShowDetails()
                }}
                title="Show Details"
              >
                🔍
              </button>
            )}
          </div>
          <SimpleBarChart data={visualData.data} />
          <div className="visual-explanation">
            <strong>What this means:</strong> If Alice (E001) has 2 rows - one says Position='Analyst', other says 'Manager' → That's 1 Position mismatch! 
            Managers of affected employees receive email alerts to fix the data errors.
          </div>
        </div>
      )
    }
    
    if (visualData.type === 'investigation') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <div style={{ flex: 1 }}>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Tracks employee compliance and past HR investigations</div>
            </div>
            {detailsContent && (
              <button 
                className="action-btn show-details-btn"
                onClick={(e) => {
                  e.stopPropagation()
                  onShowDetails && onShowDetails()
                }}
                title="Show Details"
              >
                🔍
              </button>
            )}
          </div>
          <StatGrid stats={visualData.data} />
          <div className="visual-explanation">
            <strong>How determined:</strong> HR assigns these statuses after completing formal investigations into policy violations or ethics complaints.
            <br/>
            <strong>Categories:</strong> Cleared = investigated & innocent | Flagged = had past issues, now resolved | Ongoing = currently under investigation | No History = clean record
          </div>
        </div>
      )
    }
    
    if (visualData.type === 'validation') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <div style={{ flex: 1 }}>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Validates data quality after schema standardization</div>
            </div>
            {detailsContent && (
              <button 
                className="action-btn show-details-btn"
                onClick={(e) => {
                  e.stopPropagation()
                  onShowDetails && onShowDetails()
                }}
                title="Show Details"
              >
                🔍
              </button>
            )}
          </div>
          <div className="quality-grid">
            <ProgressCircle 
              percentage={visualData.integrity} 
              label="Data Integrity" 
              color="#10b981" 
            />
            <MetricCard 
              value={visualData.records} 
              label="Records Validated" 
              icon="📊" 
              color="blue"
              description="Total employee rows processed"
            />
            <MetricCard 
              value={visualData.nulls} 
              label="Null Values" 
              icon="⚠️" 
              color={visualData.nulls > 0 ? 'orange' : 'green'}
              description="Missing data points across all fields"
            />
          </div>
          <div className="visual-explanation">
            <strong>What this means:</strong> Data Integrity = {visualData.integrity}% = cells with data / total cells × 100. 
            If you have {visualData.records} rows × 9 columns, system checks how many cells are filled vs empty. 
            <strong>Nulls</strong> = missing values like blank bonus or empty department fields.
          </div>
        </div>
      )
    }
    
    if (visualData.type === 'compliance') {
      return (
        <div className={`scratch-card-visual ${type}`}>
          <div className="visual-card-header">
            <span className="visual-icon">{icon}</span>
            <div style={{ flex: 1 }}>
              <div className="visual-title">{title}</div>
              <div className="visual-subtitle">Checks 'leave_days_max_streak' column in CSV against policy limit</div>
            </div>
            {detailsContent && (
              <button 
                className="action-btn show-details-btn"
                onClick={(e) => {
                  e.stopPropagation()
                  onShowDetails && onShowDetails()
                }}
                title="Show Details"
              >
                🔍
              </button>
            )}
          </div>
          <div className="compliance-grid">
            <ProgressCircle percentage={visualData.rate} label="Compliance Rate" color="#3b82f6" />
            <MetricCard 
              value={visualData.violations} 
              label="Policy Violations" 
              icon="⚠️" 
              color="red"
              description="Employees exceeding 20-day limit"
            />
          </div>
          <div className="visual-explanation">
            <strong>Where this comes from:</strong> The 'leave_days_max_streak' field in your CSV for each employee.
            <br/>
            <strong>Policy Rule:</strong> Max 20 consecutive leave days allowed. If CSV shows leave_days_max_streak &gt; 20 → Violation!
            <br/>
            <strong>Example:</strong> If Bob has leave_days_max_streak = 25 in CSV → That's 5 days over limit = Policy breach. Manager receives alert email.
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
          {detailsContent && (
            <button 
              className="action-btn show-details-btn"
              onClick={(e) => {
                e.stopPropagation()
                onShowDetails && onShowDetails()
              }}
              title="Show Details"
            >
              🔍
            </button>
          )}
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
    let fullContent = lines.slice(1).join('\n').trim()
    
    // Extract ##DETAILS## section if present
    let mainContent = fullContent
    let detailsContent = ''
    const detailsMatch = fullContent.match(/##DETAILS##([\s\S]*?)##END_DETAILS##/)
    if (detailsMatch) {
      detailsContent = detailsMatch[1].trim()
      mainContent = fullContent.replace(/##DETAILS##[\s\S]*?##END_DETAILS##/, '').trim()
    }
    
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
      content: mainContent,
      detailsContent: detailsContent,
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
              detailsContent={card.detailsContent}
              onShowDetails={() => setPopupData({ title: `${card.title} - Details`, content: card.detailsContent })}
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

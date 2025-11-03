import React from 'react'

export function MetricCard({ value, label, icon, color = 'blue', trend, subtitle, description }) {
  const colors = {
    blue: { bg: '#eff6ff', border: '#60a5fa', text: '#1e40af', light: '#dbeafe' },
    green: { bg: '#f0fdf4', border: '#4ade80', text: '#166534', light: '#dcfce7' },
    orange: { bg: '#fff7ed', border: '#fb923c', text: '#9a3412', light: '#fed7aa' },
    red: { bg: '#fef2f2', border: '#f87171', text: '#991b1b', light: '#fecaca' },
    purple: { bg: '#faf5ff', border: '#c084fc', text: '#6b21a8', light: '#f3e8ff' },
    cyan: { bg: '#ecfeff', border: '#22d3ee', text: '#155e75', light: '#cffafe' }
  }
  
  const c = colors[color] || colors.blue
  
  return (
    <div className="metric-card-v2" style={{ 
      background: `linear-gradient(135deg, ${c.bg} 0%, ${c.light} 100%)`,
      border: `1px solid ${c.border}`,
      borderLeft: `3px solid ${c.border}`
    }}>
      <div className="metric-icon" style={{ color: c.text, opacity: 0.8 }}>
        {icon}
      </div>
      <div className="metric-content">
        <div className="metric-value" style={{ color: c.text }}>{value}</div>
        <div className="metric-label">{label}</div>
        {description && <div className="metric-description">{description}</div>}
        {subtitle && <div className="metric-subtitle">{subtitle}</div>}
        {trend && (
          <div className="metric-trend" style={{ color: trend > 0 ? '#10b981' : '#ef4444' }}>
            {trend > 0 ? '↑' : '↓'} {Math.abs(trend)}%
          </div>
        )}
      </div>
    </div>
  )
}

export function ProgressCircle({ percentage, label, color = '#3b82f6' }) {
  const radius = 35
  const circumference = 2 * Math.PI * radius
  const offset = circumference - (percentage / 100) * circumference
  
  return (
    <div className="progress-circle-wrapper">
      <svg width="100" height="100" className="progress-circle">
        <circle cx="50" cy="50" r={radius} stroke="#e5e7eb" strokeWidth="8" fill="none" />
        <circle 
          cx="50" 
          cy="50" 
          r={radius} 
          stroke={color} 
          strokeWidth="8" 
          fill="none"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          transform="rotate(-90 50 50)"
        />
        <text x="50" y="50" textAnchor="middle" dy="7" fontSize="18" fontWeight="700" fill={color}>
          {percentage}%
        </text>
      </svg>
      <div className="progress-label">{label}</div>
    </div>
  )
}

export function StatGrid({ stats }) {
  return (
    <div className="stat-grid">
      {stats.map((stat, i) => (
        <div key={i} className="stat-item" style={{ borderLeftColor: stat.color }}>
          <div className="stat-icon">{stat.icon}</div>
          <div className="stat-value" style={{ color: stat.color }}>{stat.value}</div>
          <div className="stat-label">{stat.label}</div>
        </div>
      ))}
    </div>
  )
}

export function SimpleBarChart({ data, maxValue }) {
  const max = maxValue || Math.max(...data.map(d => d.value))
  
  return (
    <div className="simple-bar-chart">
      {data.map((item, i) => (
        <div key={i} className="bar-item">
          <div className="bar-label">{item.label}</div>
          <div className="bar-wrapper">
            <div 
              className="bar-fill" 
              style={{ 
                width: `${(item.value / max) * 100}%`,
                background: item.color || '#3b82f6'
              }}
            >
              <span className="bar-value">{item.value}</span>
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}

export function StatusBadge({ status, count }) {
  const config = {
    success: { bg: '#d1fae5', color: '#065f46', icon: '✓' },
    warning: { bg: '#fed7aa', color: '#92400e', icon: '⚠' },
    error: { bg: '#fecaca', color: '#991b1b', icon: '✗' },
    info: { bg: '#dbeafe', color: '#1e40af', icon: 'ℹ' }
  }
  
  const c = config[status] || config.info
  
  return (
    <div className="status-badge-v2" style={{ background: c.bg, color: c.color }}>
      <span className="badge-icon">{c.icon}</span>
      <span className="badge-count">{count}</span>
    </div>
  )
}

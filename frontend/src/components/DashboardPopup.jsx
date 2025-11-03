import React, { useEffect, useRef } from 'react'
import { Chart } from 'chart.js/auto'

export default function DashboardPopup({ runId, summary, onClose }) {
  const pieChartRef = useRef(null)
  const barChartRef = useRef(null)
  const lineChartRef = useRef(null)
  const doughnutChartRef = useRef(null)
  
  const pieChartInstance = useRef(null)
  const barChartInstance = useRef(null)
  const lineChartInstance = useRef(null)
  const doughnutChartInstance = useRef(null)

  useEffect(() => {
    if (!summary) return

    // Investigations Pie Chart
    if (pieChartRef.current) {
      pieChartInstance.current = new Chart(pieChartRef.current, {
        type: 'pie',
        data: {
          labels: ['Past Cleared', 'Past Flagged', 'Ongoing'],
          datasets: [{
            data: [
              summary.charts?.investigations?.past_cleared || 0,
              summary.charts?.investigations?.past_flagged || 0,
              summary.charts?.investigations?.ongoing || 0
            ],
            backgroundColor: ['#10b981', '#f59e0b', '#ef4444']
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: 'bottom', labels: { color: '#e6e8ef' } }
          }
        }
      })
    }

    // Mismatches Bar Chart
    if (barChartRef.current) {
      const mismatches = summary.findings?.mismatch_counts || {}
      barChartInstance.current = new Chart(barChartRef.current, {
        type: 'bar',
        data: {
          labels: Object.keys(mismatches),
          datasets: [{
            label: 'Mismatches',
            data: Object.values(mismatches),
            backgroundColor: ['#3b82f6', '#06b6d4', '#a855f7']
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { labels: { color: '#e6e8ef' } }
          },
          scales: {
            y: { 
              beginAtZero: true,
              ticks: { color: '#8a93a6' },
              grid: { color: '#1f2a44' }
            },
            x: { 
              ticks: { color: '#8a93a6' },
              grid: { color: '#1f2a44' }
            }
          }
        }
      })
    }

    // Data Quality Trend (Mock data)
    if (lineChartRef.current) {
      lineChartInstance.current = new Chart(lineChartRef.current, {
        type: 'line',
        data: {
          labels: ['Week 1', 'Week 2', 'Week 3', 'Week 4'],
          datasets: [{
            label: 'Data Quality Score',
            data: [75, 82, 88, 92],
            borderColor: '#10b981',
            backgroundColor: 'rgba(16, 185, 129, 0.1)',
            tension: 0.4,
            fill: true
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { labels: { color: '#e6e8ef' } }
          },
          scales: {
            y: { 
              beginAtZero: true,
              max: 100,
              ticks: { color: '#8a93a6' },
              grid: { color: '#1f2a44' }
            },
            x: { 
              ticks: { color: '#8a93a6' },
              grid: { color: '#1f2a44' }
            }
          }
        }
      })
    }

    // Issues Breakdown Doughnut
    if (doughnutChartRef.current) {
      const findings = summary.findings || {}
      doughnutChartInstance.current = new Chart(doughnutChartRef.current, {
        type: 'doughnut',
        data: {
          labels: ['Duplicates', 'Policy Violations', 'Total Mismatches'],
          datasets: [{
            data: [
              findings.duplicates || 0,
              findings.policy_violations || 0,
              Object.values(findings.mismatch_counts || {}).reduce((a, b) => a + b, 0)
            ],
            backgroundColor: ['#ef4444', '#f59e0b', '#3b82f6']
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: 'bottom', labels: { color: '#e6e8ef' } }
          }
        }
      })
    }

    return () => {
      pieChartInstance.current?.destroy()
      barChartInstance.current?.destroy()
      lineChartInstance.current?.destroy()
      doughnutChartInstance.current?.destroy()
    }
  }, [summary])

  if (!summary) return null

  const findings = summary.findings || {}
  const totalIssues = (findings.duplicates || 0) + 
                       (findings.policy_violations || 0) +
                       Object.values(findings.mismatch_counts || {}).reduce((a, b) => a + b, 0)

  return (
    <div className="dashboard-popup-overlay" onClick={onClose}>
      <div className="dashboard-popup" onClick={(e) => e.stopPropagation()}>
        <div className="dashboard-header">
          <div className="dashboard-title">📊 Comprehensive Audit Dashboard</div>
          <button className="dashboard-close" onClick={onClose}>✕ Close</button>
        </div>

        {/* Key Metrics */}
        <div className="dashboard-metrics">
          <div className="dashboard-metric">
            <div className="dashboard-metric-value" style={{ color: '#ef4444' }}>
              {totalIssues}
            </div>
            <div className="dashboard-metric-label">Total Issues</div>
          </div>
          <div className="dashboard-metric">
            <div className="dashboard-metric-value" style={{ color: '#3b82f6' }}>
              {findings.duplicates || 0}
            </div>
            <div className="dashboard-metric-label">Duplicates</div>
          </div>
          <div className="dashboard-metric">
            <div className="dashboard-metric-value" style={{ color: '#f59e0b' }}>
              {findings.policy_violations || 0}
            </div>
            <div className="dashboard-metric-label">Policy Violations</div>
          </div>
          <div className="dashboard-metric">
            <div className="dashboard-metric-value" style={{ color: '#10b981' }}>
              {summary.charts?.rows_after_dedup || 0}
            </div>
            <div className="dashboard-metric-label">Clean Records</div>
          </div>
        </div>

        {/* Charts Grid */}
        <div className="dashboard-grid">
          {/* Investigations Distribution */}
          <div className="dashboard-card">
            <div className="dashboard-card-header">
              <div className="dashboard-card-icon" style={{ background: 'rgba(59, 130, 246, 0.15)', color: '#3b82f6' }}>
                🔎
              </div>
              <div className="dashboard-card-title">Investigation Status Distribution</div>
            </div>
            <div className="dashboard-chart">
              <canvas ref={pieChartRef}></canvas>
            </div>
          </div>

          {/* Mismatches Breakdown */}
          <div className="dashboard-card">
            <div className="dashboard-card-header">
              <div className="dashboard-card-icon" style={{ background: 'rgba(6, 182, 212, 0.15)', color: '#06b6d4' }}>
                ⚠️
              </div>
              <div className="dashboard-card-title">Mismatch Categories</div>
            </div>
            <div className="dashboard-chart">
              <canvas ref={barChartRef}></canvas>
            </div>
          </div>

          {/* Data Quality Trend */}
          <div className="dashboard-card">
            <div className="dashboard-card-header">
              <div className="dashboard-card-icon" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981' }}>
                📈
              </div>
              <div className="dashboard-card-title">Data Quality Trend</div>
            </div>
            <div className="dashboard-chart">
              <canvas ref={lineChartRef}></canvas>
            </div>
          </div>

          {/* Issues Breakdown */}
          <div className="dashboard-card">
            <div className="dashboard-card-header">
              <div className="dashboard-card-icon" style={{ background: 'rgba(239, 68, 68, 0.15)', color: '#ef4444' }}>
                🎯
              </div>
              <div className="dashboard-card-title">Issues Breakdown</div>
            </div>
            <div className="dashboard-chart">
              <canvas ref={doughnutChartRef}></canvas>
            </div>
          </div>

          {/* Risks */}
          <div className="dashboard-card">
            <div className="dashboard-card-header">
              <div className="dashboard-card-icon" style={{ background: 'rgba(239, 68, 68, 0.15)', color: '#ef4444' }}>
                ⚡
              </div>
              <div className="dashboard-card-title">Key Risks Identified</div>
            </div>
            <div style={{ padding: '10px 0' }}>
              {(summary.risks || []).map((risk, i) => (
                <div key={i} style={{ 
                  padding: '10px 12px', 
                  background: 'rgba(239, 68, 68, 0.1)', 
                  border: '1px solid rgba(239, 68, 68, 0.3)',
                  borderRadius: '8px',
                  marginBottom: '8px',
                  fontSize: '13px'
                }}>
                  • {risk}
                </div>
              ))}
            </div>
          </div>

          {/* Recommendations */}
          <div className="dashboard-card">
            <div className="dashboard-card-header">
              <div className="dashboard-card-icon" style={{ background: 'rgba(16, 185, 129, 0.15)', color: '#10b981' }}>
                💡
              </div>
              <div className="dashboard-card-title">Recommendations</div>
            </div>
            <div style={{ padding: '10px 0' }}>
              {(summary.recommendations || []).map((rec, i) => (
                <div key={i} style={{ 
                  padding: '10px 12px', 
                  background: 'rgba(16, 185, 129, 0.1)', 
                  border: '1px solid rgba(16, 185, 129, 0.3)',
                  borderRadius: '8px',
                  marginBottom: '8px',
                  fontSize: '13px'
                }}>
                  ✓ {rec}
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Export Button */}
        <div style={{ marginTop: '24px', textAlign: 'center' }}>
          <button className="dashboard-button">
            📥 Export Full Report (PDF)
          </button>
        </div>
      </div>
    </div>
  )
}

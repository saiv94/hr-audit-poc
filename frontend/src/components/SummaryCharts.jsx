import React, { useMemo } from 'react'
import { Bar, Doughnut } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  BarElement,
  CategoryScale,
  LinearScale,
  ArcElement,
  Tooltip,
  Legend
} from 'chart.js'

ChartJS.register(BarElement, CategoryScale, LinearScale, ArcElement, Tooltip, Legend)

export default function SummaryCharts({ summary, runId }) {
  const inv = summary?.charts?.investigations || { past_cleared: 0, past_flagged: 0, ongoing: 0 }
  const findings = summary?.findings || {}

  // Show data quality issues breakdown instead of deduplication
  const barData = useMemo(() => ({
    labels: ['Position Conflicts', 'Bonus Conflicts', 'Paygrade Conflicts'],
    datasets: [{
      label: 'Data Mismatches by Type',
      data: [
        findings.mismatch_counts?.position || 0,
        findings.mismatch_counts?.bonus || 0,
        findings.mismatch_counts?.paygrade || 0
      ],
      backgroundColor: ['#f59e0b', '#f97316', '#ef4444']
    }]
  }), [findings])

  const doughnutData = useMemo(() => ({
    labels: ['Past Cleared','Past Flagged','Ongoing'],
    datasets: [{
      data: [inv.past_cleared, inv.past_flagged, inv.ongoing],
      backgroundColor: ['#10b981','#ef4444','#f59e0b']
    }]
  }), [inv])

  return (
    <div className="summary">
      <h3>Executive Summary</h3>
      <div className="summary-cards">
        <div className="card">
          <div className="metric">{summary.findings.duplicates}</div>
          <div className="label">Duplicate People Found</div>
        </div>
        <div className="card">
          <div className="metric">{summary.findings.policy_violations}</div>
          <div className="label">Time-Off Policy Violations</div>
        </div>
        <div className="card">
          <div className="metric">
            {Object.values(findings.mismatch_counts || {}).reduce((a, b) => a + b, 0)}
          </div>
          <div className="label">Data Conflicts Found</div>
        </div>
      </div>

      <div className="charts">
        <div className="chart">
          <h4 style={{ marginBottom: '12px', fontSize: '14px', color: '#64748b' }}>
            Conflicting Information by Category
          </h4>
          <Bar data={barData} />
        </div>
        <div className="chart">
          <h4 style={{ marginBottom: '12px', fontSize: '14px', color: '#64748b' }}>
            Compliance Investigation Status
          </h4>
          <Doughnut data={doughnutData} />
        </div>
      </div>

      <div className="lists">
        <div>
          <h4>Risks</h4>
          <ul>
            {summary.risks.map((r,i)=>(<li key={i}>{r}</li>))}
          </ul>
        </div>
        <div>
          <h4>Recommendations</h4>
          <ul>
            {summary.recommendations.map((r,i)=>(<li key={i}>{r}</li>))}
          </ul>
        </div>
      </div>
    </div>
  )
}

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

  const barData = useMemo(() => ({
    labels: ['After De-duplication'],
    datasets: [{
      label: 'Rows',
      data: [summary?.charts?.rows_after_dedup || 0],
      backgroundColor: '#2563eb'
    }]
  }), [summary])

  const doughnutData = useMemo(() => ({
    labels: ['Past Cleared','Past Flagged','Ongoing'],
    datasets: [{
      data: [inv.past_cleared, inv.past_flagged, inv.ongoing],
      backgroundColor: ['#10b981','#ef4444','#f59e0b']
    }]
  }), [inv])

  return (
    <div className="summary">
      <h3>Summary</h3>
      <div className="summary-cards">
        <div className="card">
          <div className="metric">{summary.findings.duplicates}</div>
          <div className="label">Duplicate Records</div>
        </div>
        <div className="card">
          <div className="metric">{summary.findings.policy_violations}</div>
          <div className="label">Policy Violations</div>
        </div>
      </div>

      <div className="charts">
        <div className="chart"><Bar data={barData} /></div>
        <div className="chart"><Doughnut data={doughnutData} /></div>
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

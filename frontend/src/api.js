const BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

export async function getRuns() {
  const r = await fetch(`${BASE}/runs`)
  const j = await r.json()
  return j.runs || []
}

export async function createRun(payload) {
  const r = await fetch(`${BASE}/runs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  return r.json()
}

export async function getStatus(runId) {
  const r = await fetch(`${BASE}/runs/${runId}/status`)
  return r.json()
}

export async function getNodes(runId) {
  const r = await fetch(`${BASE}/runs/${runId}/nodes`)
  const j = await r.json()
  return j.nodes || []
}

export async function getScratchpad(runId, nodeId) {
  const r = await fetch(`${BASE}/runs/${runId}/nodes/${nodeId}/scratchpad`)
  const j = await r.json()
  return j.scratchpad || ''
}

export async function getArtifact(runId, name) {
  const r = await fetch(`${BASE}/runs/${runId}/artifacts/${name}`)
  return r.json()
}

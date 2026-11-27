// Relative paths only, so nginx or the service can serve this build.
async function json(path, options) {
  const response = await fetch(path, options);
  const body = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(body.detail || `${path} returned ${response.status}`);
  }

  return body;
}

export function listLibraries() {
  return json('/api/libraries');
}

export function health() {
  return json('/api/health');
}

export function search({ smiles, library, n }) {
  const query = new URLSearchParams({ smiles, n: String(n) });

  if (library) {
    query.set('library', library);
  }

  // A GET so a result page can be linked and cached; POST for long SMILES.
  if (smiles.length < 1800) {
    return json(`/api/search?${query}`);
  }

  return json('/api/search', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ smiles, library, num_neighbors: n }),
  });
}

// Relative paths only, so nginx or the service can serve this build.

/** FastAPI sends `detail` as a string, or as a list of validation errors. */
function describe(body, path, status) {
  const detail = body?.detail;

  if (typeof detail === 'string') {
    return detail;
  }

  if (Array.isArray(detail) && detail.length) {
    return detail.map((item) => {
      const field = item.loc?.[item.loc.length - 1];
      return field ? `${field}: ${item.msg}` : item.msg;
    }).join('; ');
  }

  return `${path} returned ${status}`;
}

async function json(path, options) {
  let response;

  try {
    response = await fetch(path, options);
  } catch {
    throw new Error('Could not reach the search service. Is it running?');
  }

  const body = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(describe(body, path, response.status));
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

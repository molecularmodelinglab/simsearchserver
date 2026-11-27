import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from 'react';
import About from './components/About';
import Boundary from './components/Boundary';
import Options from './components/Options';
import QueryPanel from './components/QueryPanel';
import ResultGrid from './components/ResultGrid';
import StatusLine from './components/StatusLine';
import { listLibraries, search } from './api';
import { rdkit } from './rdkit';

// Ketcher carries Indigo as WASM, so it loads after the page rather than with it.
const Sketcher = lazy(() => import('./components/Sketcher'));

const EXAMPLE = 'CC(C)NCC(O)COc1cccc2ccccc12';

function initial() {
  const params = new URLSearchParams(window.location.search);

  return {
    smiles: params.get('smiles') || EXAMPLE,
    library: params.get('lib') || null,
    count: Number(params.get('n')) || 24,
    searchOnLoad: Boolean(params.get('smiles')),
  };
}

export default function App() {
  const start = initial();

  const [smiles, setSmiles] = useState(start.smiles);
  const [library, setLibrary] = useState(start.library);
  const [count, setCount] = useState(start.count);
  const [libraries, setLibraries] = useState([]);

  const [status, setStatus] = useState('idle');
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);
  const [depicted, setDepicted] = useState(0);

  const [sort, setSort] = useState('similarity');
  const [dark, setDark] = useState(
    () => window.matchMedia?.('(prefers-color-scheme: dark)').matches ?? false);

  useEffect(() => {
    document.documentElement.dataset.theme = dark ? 'dark' : 'light';
  }, [dark]);

  useEffect(() => {
    rdkit().catch(() => {});
  }, []);

  useEffect(() => {
    listLibraries()
      .then((body) => {
        setLibraries(body.libraries);
        setLibrary((chosen) => chosen || body.default);
      })
      .catch((e) => setError(`Could not reach the search service: ${e.message}`));
  }, []);

  const run = useCallback(async (querySmiles, queryLibrary, queryCount) => {
    const trimmed = (querySmiles || '').trim();

    if (!trimmed) {
      setError('Draw a structure or enter a SMILES.');
      return;
    }

    setStatus('searching');
    setError(null);
    setDepicted(0);

    const params = new URLSearchParams({ smiles: trimmed, n: String(queryCount) });
    if (queryLibrary) {
      params.set('lib', queryLibrary);
    }
    window.history.replaceState(null, '', `?${params}`);

    try {
      const body = await search({ smiles: trimmed, library: queryLibrary, n: queryCount });
      setResult(body);
      setStatus('done');
    } catch (e) {
      setError(e.message);
      setStatus('idle');
    }
  }, []);

  // A link carrying a query searches on arrival.
  useEffect(() => {
    if (start.searchOnLoad && library) {
      run(start.smiles, library, start.count);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [library]);

  const hits = useMemo(() => {
    if (!result) {
      return [];
    }

    if (sort === 'distance') {
      return [...result.hits].sort((a, b) => a.distance - b.distance);
    }

    return result.hits;
  }, [result, sort]);

  function handleExport(kind) {
    if (!hits.length) {
      return;
    }

    if (kind === 'ids') {
      navigator.clipboard?.writeText(hits.map((h) => h.identifier).join('\n'));
      return;
    }

    const rows = [['identifier', 'smiles', 'similarity', 'distance'].join(',')];
    hits.forEach((hit) => rows.push([
      hit.identifier, hit.smiles,
      hit.similarity === undefined ? '' : hit.similarity.toFixed(4),
      hit.distance.toFixed(4),
    ].join(',')));

    const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = 'smallsa-analogs.csv';
    link.click();
    URL.revokeObjectURL(link.href);
  }

  return (
    <div className="page">
      <header>
        <img src="/logo.png" alt="" className="logo" />
        <div className="titles">
          <h1>SmallSA</h1>
          <p className="tagline">Chemical similarity search over billion-scale libraries</p>
        </div>
        <button className="theme" onClick={() => setDark((on) => !on)}
                title="Toggle dark mode" aria-label="Toggle dark mode">
          {dark ? '☀' : '☾'}
        </button>
      </header>

      <About open={!result} />

      <div className="workspace">
        <aside className="sidebar">
          <Options
            libraries={libraries}
            library={library}
            onLibrary={(name) => {
              setLibrary(name);
              if (result) {
                run(smiles, name, count);
              }
            }}
            count={count}
            onCount={(n) => {
              setCount(n);
              if (result) {
                run(smiles, library, n);
              }
            }}
            sort={sort}
            onSort={setSort}
            onExport={handleExport}
            disabled={status === 'searching'}
            hasResults={hits.length > 0}
          />

          <button className="search" disabled={status === 'searching'}
                  onClick={() => run(smiles, library, count)}>
            {status === 'searching' ? 'Searching…' : 'Find analogs'}
          </button>

          {error && <p className="error">{error}</p>}

          <StatusLine
            status={status}
            timing={result?.timing}
            depicted={depicted}
            total={hits.length}
            cached={result?.cached}
          />
        </aside>

        <section className="editor">
          <Boundary>
            <Suspense fallback={<div className="sketcher sketcher-loading">
              loading structure editor…
            </div>}>
              <Sketcher smiles={smiles} onSmiles={setSmiles} />
            </Suspense>
          </Boundary>

          <form className="smiles-row" onSubmit={(e) => {
            e.preventDefault();
            run(smiles, library, count);
          }}>
            <input
              value={smiles}
              onChange={(e) => setSmiles(e.target.value)}
              placeholder="Enter or paste a SMILES"
              spellCheck={false}
              aria-label="SMILES"
            />
          </form>
        </section>
      </div>

      {(status === 'searching' || status === 'done') && (
        <section className="results">
          <div className="results-head">
            <QueryPanel smiles={result?.query.smiles || smiles}
                        canonical={result?.query.canonical} />

            <div className="results-meta">
              <h2>{status === 'done' ? `Results (${hits.length})` : 'Searching…'}</h2>
              <p className="results-hint">
                Ranked by atom-pair Tanimoto similarity to the query. Click an
                identifier or SMILES to copy it.
              </p>
              {result && (
                <p className="results-source">
                  <span>searched <b>
                    {libraries.find((l) => l.name === result.query.library)?.display
                      || result.query.library}
                  </b></span>
                  <span><b>{(result.timing.index_ms).toFixed(1)} ms</b> in the index</span>
                  <span><b>{result.timing.total_ms.toFixed(0)} ms</b> end to end</span>
                  {result.cached && <span>served from cache</span>}
                </p>
              )}
            </div>
          </div>

          <div className="grid-scroll">
            {status === 'searching' ? (
              <div className="grid">
                {Array.from({ length: Math.min(count, 12) }).map((_, i) => (
                  <div key={i} className="card skeleton-card" />
                ))}
              </div>
            ) : (
              <ResultGrid hits={hits} onProgress={setDepicted} />
            )}
          </div>
        </section>
      )}
    </div>
  );
}

import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import About from './components/About';
import Boundary from './components/Boundary';
import Header from './components/Header';
import QueryPanel from './components/QueryPanel';
import ResultGrid, { SkeletonGrid } from './components/ResultGrid';
import ResultsSummary from './components/ResultsSummary';
import SearchSettings from './components/SearchSettings';
import { listLibraries, search } from './api';
import { canonicalize, rdkit } from './rdkit';

// Ketcher carries Indigo as WASM, so it loads after the page rather than with it.
const Sketcher = lazy(() => import('./components/Sketcher'));

const EXAMPLE = 'CC(C)NCC(O)COc1cccc2ccccc12';
const DEFAULT_COUNT = 24;
// Used until /api/libraries answers; the service's own limits replace them.
const FALLBACK_LIMITS = { max_num_neighbors: 96, max_smiles_length: 150 };
const THEME_KEY = 'smallsa-theme';

function readUrl() {
  const params = new URLSearchParams(window.location.search);
  const n = Number.parseInt(params.get('n'), 10);

  return {
    smiles: params.get('smiles') || '',
    library: params.get('lib') || null,
    count: Number.isFinite(n) ? n : null,
    searchOnLoad: Boolean(params.get('smiles')),
  };
}

function clamp(n, max) {
  return Math.min(max, Math.max(1, n));
}

function csvField(value) {
  const text = value === undefined || value === null ? '' : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function savedTheme() {
  try {
    return localStorage.getItem(THEME_KEY) === 'dark';
  } catch {
    return false;
  }
}

export default function App() {
  // Read once: after the first search the URL reflects the page, not the visit.
  const [start] = useState(readUrl);
  const sketcher = useRef(null);

  const [smiles, setSmiles] = useState(start.smiles || EXAMPLE);
  // The SMILES field was edited by hand and the editor does not show it yet.
  const [edited, setEdited] = useState(false);
  const [library, setLibrary] = useState(start.library);
  const [count, setCount] = useState(clamp(start.count ?? DEFAULT_COUNT,
                                           FALLBACK_LIMITS.max_num_neighbors));
  const [libraries, setLibraries] = useState([]);
  const [limits, setLimits] = useState(FALLBACK_LIMITS);
  const [loaded, setLoaded] = useState(false);

  const [searching, setSearching] = useState(false);
  const [error, setError] = useState(null);
  // About the page's setup rather than the last action, so a search keeps it.
  const [notice, setNotice] = useState(null);
  const [result, setResult] = useState(null);
  // What the latest search asked for, to show while it runs and to spot stale settings.
  const [asked, setAsked] = useState(null);
  const [searchId, setSearchId] = useState(0);
  const [exact, setExact] = useState(() => new Set());

  const [sort, setSort] = useState('similarity');
  const [about, setAbout] = useState(false);
  const [dark, setDark] = useState(savedTheme);

  useEffect(() => {
    document.documentElement.dataset.theme = dark ? 'dark' : 'light';
    try {
      localStorage.setItem(THEME_KEY, dark ? 'dark' : 'light');
    } catch {
      // Private windows may refuse storage; the theme still applies.
    }
  }, [dark]);

  useEffect(() => {
    rdkit().catch(() => {});
  }, []);

  useEffect(() => {
    listLibraries()
      .then((body) => {
        const max = body.limits?.max_num_neighbors ?? FALLBACK_LIMITS.max_num_neighbors;

        setLibraries(body.libraries);
        setLimits({ ...FALLBACK_LIMITS, ...body.limits });
        setCount((n) => clamp(start.count ?? n, max));
        const wanted = start.library;
        const known = wanted && body.libraries.some((entry) => entry.name === wanted);

        if (wanted && !known) {
          setNotice(`This link names a library "${wanted}" that this server does not have, so the default library is selected.`);
        }

        setLibrary(known ? wanted : body.default);
        setLoaded(true);
      })
      .catch((e) => {
        setError(`Could not load the library list: ${e.message}`);
        setLoaded(true);
      });
  }, [start.count, start.library]);

  const run = useCallback(async ({ smiles: query, library: lib, count: n }) => {
    const trimmed = (query || '').trim();

    if (!trimmed) {
      setError('Draw a structure or enter a SMILES first.');
      return;
    }

    if (trimmed.length > limits.max_smiles_length) {
      setError(`That SMILES is ${trimmed.length} characters; the limit is ${limits.max_smiles_length}.`);
      return;
    }

    setSearching(true);
    setError(null);
    setAsked({ smiles: trimmed, library: lib, count: n });

    const params = new URLSearchParams({ smiles: trimmed, n: String(n) });
    if (lib) {
      params.set('lib', lib);
    }
    window.history.replaceState(null, '', `?${params}`);

    try {
      const body = await search({ smiles: trimmed, library: lib, n });
      setResult({ ...body, requested: n });
      setSearchId((id) => id + 1);
    } catch (e) {
      // The previous results stay on screen; only the error is new.
      setError(e.message);
      setAsked(null);
    } finally {
      setSearching(false);
    }
  }, [limits.max_smiles_length]);

  // A link carrying a query searches on arrival, once the libraries are known.
  const arrived = useRef(false);
  useEffect(() => {
    if (loaded && start.searchOnLoad && !arrived.current) {
      arrived.current = true;
      run({ smiles: start.smiles, library, count });
    }
  }, [loaded, start, library, count, run]);

  // Exact matches are found by comparing canonical SMILES in the browser.
  useEffect(() => {
    let live = true;
    setExact(new Set());

    if (!result) {
      return undefined;
    }

    rdkit().then((module) => {
      const query = canonicalize(module, result.query.smiles);
      const found = new Set();

      if (query) {
        result.hits.forEach((hit) => {
          // A shared structure always scores 1.0, so only those need parsing.
          if (hit.similarity !== undefined && hit.similarity < 0.999) {
            return;
          }
          if (canonicalize(module, hit.smiles) === query) {
            found.add(hit.identifier);
          }
        });
      }

      if (live) {
        setExact(found);
      }
    }).catch(() => {});

    return () => {
      live = false;
    };
  }, [result]);

  const hits = useMemo(() => {
    if (!result) {
      return [];
    }

    if (sort === 'distance') {
      return [...result.hits].sort((a, b) => a.distance - b.distance);
    }

    return result.hits;
  }, [result, sort]);

  const libraryName = (name) => libraries.find((l) => l.name === name)?.display || name;

  async function showInEditor() {
    const trimmed = smiles.trim();

    if (!trimmed) {
      setError('Enter a SMILES to show in the editor.');
      return;
    }

    // Ketcher accepts unreadable SMILES silently, so RDKit vets it first.
    try {
      const module = await rdkit();
      if (!canonicalize(module, trimmed)) {
        setError('That SMILES could not be read. Check it for typos.');
        return;
      }
    } catch {
      // Without RDKit, let the editor try on its own.
    }

    try {
      await sketcher.current?.show(trimmed);
      setEdited(false);
      setError(null);
    } catch {
      setError('The editor could not read that SMILES. Check it for typos.');
    }
  }

  async function handleDrawn(drawn) {
    let text = drawn;

    // Ketcher's SMILES are valid but awkward to read; RDKit's canonical form
    // is tidier. CXSMILES extensions (after '|') are kept as Ketcher wrote them.
    if (drawn && !drawn.includes('|')) {
      try {
        text = canonicalize(await rdkit(), drawn) || drawn;
      } catch {
        text = drawn;
      }
    }

    setSmiles(text);
    setEdited(false);
  }

  function downloadCsv() {
    const header = ['rank', 'identifier', 'smiles', 'similarity', 'distance', 'exact_match'];
    const rows = hits.map((hit, i) => [
      i + 1, hit.identifier, hit.smiles,
      hit.similarity === undefined ? '' : hit.similarity.toFixed(4),
      hit.distance.toFixed(4),
      exact.has(hit.identifier) ? 'yes' : 'no',
    ].map(csvField).join(','));

    const blob = new Blob([[header.join(','), ...rows].join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `smallsa-${result?.query.library || 'results'}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    // Revoked later: some browsers still read the URL after click() returns.
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  }

  const shownSmiles = (searching && asked?.smiles) || result?.query.smiles;
  const stale = Boolean(result)
    && (result.query.library !== library || result.requested !== count);

  return (
    <div className="page">
      <Header dark={dark} onTheme={() => setDark((on) => !on)} onAbout={() => setAbout(true)} />
      <About open={about} onClose={() => setAbout(false)} />

      <main>
        <section className="panel query-card" aria-label="Query structure">
          <div className="query-card-head">
            <h2 className="panel-label">Query structure</h2>
            <p className="hint">
              Draw in the editor, or paste a SMILES straight into it. The SMILES
              field below follows the drawing.
            </p>
          </div>

          <Boundary>
            <Suspense fallback={<div className="sketcher sketcher-loading">loading structure editor…</div>}>
              <Sketcher ref={sketcher} initial={start.smiles || EXAMPLE} onDrawn={handleDrawn} />
            </Suspense>
          </Boundary>

          <form className="smiles-row" onSubmit={(e) => {
            e.preventDefault();
            run({ smiles, library, count });
          }}>
            <div className={`smiles-field${error ? ' has-error' : ''}`}>
              <input
                value={smiles}
                onChange={(e) => {
                  setSmiles(e.target.value);
                  setEdited(true);
                }}
                placeholder="SMILES, e.g. CC(=O)Oc1ccccc1C(=O)O"
                spellCheck={false}
                autoComplete="off"
                aria-label="SMILES"
                aria-invalid={Boolean(error)}
              />
              <span className={`smiles-length${smiles.trim().length > limits.max_smiles_length ? ' is-over' : ''}`}>
                {smiles.trim().length}/{limits.max_smiles_length}
              </span>
            </div>
            <button type="button" className={`btn btn-secondary${edited ? ' is-pending' : ''}`}
                    onClick={showInEditor}
                    title="Draw this SMILES in the editor">
              Show in editor
            </button>
            <button type="submit" className="btn btn-primary" disabled={searching}>
              <svg viewBox="0 0 24 24" width="17" height="17" aria-hidden="true">
                <circle cx="10.5" cy="10.5" r="6.5" fill="none" stroke="currentColor" strokeWidth="2.2" />
                <path d="M15.5 15.5L21 21" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" />
              </svg>
              {searching ? 'Searching…' : 'Find the needle'}
            </button>
          </form>

          {error && <p className="error" role="alert">{error}</p>}
          {!error && edited && (
            <p className="hint hint-pending">
              The editor does not show this SMILES yet. Searching uses the field as typed.
            </p>
          )}

          {notice && <p className="stale">{notice}</p>}

          <SearchSettings
            libraries={libraries}
            library={library}
            onLibrary={setLibrary}
            count={count}
            onCount={setCount}
            max={limits.max_num_neighbors}
            disabled={searching}
          />
        </section>

        {(result || searching) && (
          <section className="results" aria-label="Results">
            <div className="results-top">
              <QueryPanel smiles={shownSmiles} canonical={searching ? null : result?.query.canonical} />
              <ResultsSummary
                searching={searching}
                result={result}
                hits={hits}
                libraryName={libraryName(result?.query.library)}
                sort={sort}
                onSort={setSort}
                exactCount={exact.size}
                stale={stale}
                onDownload={downloadCsv}
                idsText={() => hits.map((h) => h.identifier).join('\n')}
              />
            </div>

            <div className="panel grid-panel">
              <div className="grid-scroll">
                {searching
                  ? <SkeletonGrid count={Math.min(asked?.count ?? count, 12)} />
                  : <ResultGrid key={`${searchId}-${sort}`} hits={hits} sort={sort} exact={exact} />}
              </div>
            </div>
          </section>
        )}
      </main>

      <footer className="footer">
        <span>SmallSA · <a href="https://molecularmodelinglab.github.io/" target="_blank" rel="noreferrer">Molecular Modeling Lab</a>, UNC</span>
        <span>Structure editing by Ketcher · depiction by RDKit</span>
      </footer>
    </div>
  );
}

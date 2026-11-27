import InfoHover from './InfoHover';
import { useCopy } from '../clipboard';

function ms(value) {
  return value >= 100 ? `${Math.round(value)} ms` : `${value.toFixed(1)} ms`;
}

const SORTS = [
  ['similarity', 'Similarity'],
  ['distance', 'Embedding distance'],
];

export default function ResultsSummary({
  searching, result, hits, libraryName, sort, onSort, exactCount, stale,
  onDownload, idsText,
}) {
  const [flashed, copy] = useCopy();
  const timing = result?.timing;
  const ranked = hits.some((hit) => hit.similarity !== undefined);

  let headline = 'Searching…';
  if (!searching && result) {
    if (exactCount > 0) {
      headline = 'Found it';
    } else {
      headline = hits.length ? 'Closest neighbors' : 'No results';
    }
  }

  return (
    <section className="panel summary" aria-live="polite">
      <div className="summary-head">
        <h2 className="panel-label">Results</h2>
        {!searching && result && (
          <span className="count-pill">{hits.length} {hits.length === 1 ? 'hit' : 'hits'}</span>
        )}
      </div>

      <p className={`headline${exactCount > 0 && !searching ? ' is-found' : ''}`}>
        {headline}
      </p>
      {!searching && result && (
        <p className="summary-text">
          {exactCount > 0
            ? `The query is in ${libraryName}${exactCount > 1 ? ` (${exactCount} entries)` : ''}, marked below, followed by its nearest neighbors.`
            : `The query is not in ${libraryName}. These are its nearest neighbors.`}
          {' '}
          {sort === 'distance'
            ? 'Ordered by embedding distance, as the index returned them.'
            : ranked
              ? 'Ranked by atom-pair Tanimoto similarity to the query.'
              : 'This library stores no fingerprints, so results are in embedding-distance order.'}
        </p>
      )}

      {timing && !searching && (
        <dl className="stats">
          <div><dt>Library</dt><dd>{libraryName}</dd></div>
          <div><dt>Index search</dt><dd>{ms(timing.index_ms)}</dd></div>
          <div><dt>Embedding</dt><dd>{ms(timing.embed_ms)}</dd></div>
          <div>
            <dt>End to end</dt>
            <dd>{ms(timing.total_ms)}</dd>
          </div>
        </dl>
      )}

      {stale && !searching && (
        <p className="stale">Settings changed since this search. Search again to apply them.</p>
      )}

      <div className="summary-controls">
        <div className="control">
          <span className="control-label">
            Sort by
            <InfoHover text="Similarity is the structural ranking and the default. Embedding distance is the order the index returned, before structural re-ranking." />
          </span>
          <div className="segmented" role="radiogroup" aria-label="Sort by">
            {SORTS.map(([value, label]) => (
              <button key={value} type="button" role="radio" aria-checked={sort === value}
                      className={sort === value ? 'is-on' : ''}
                      onClick={() => onSort(value)} disabled={searching}>
                {label}
              </button>
            ))}
          </div>
        </div>

        <div className="control">
          <span className="control-label">Export</span>
          <div className="button-row">
            <button type="button" className="btn btn-quiet" disabled={searching || !hits.length}
                    onClick={() => copy('ids', idsText())}>
              {flashed === 'ids' ? 'Copied ✓' : flashed === 'ids-failed' ? 'Copy failed' : 'Copy IDs'}
            </button>
            <button type="button" className="btn btn-quiet" disabled={searching || !hits.length}
                    onClick={onDownload}>
              Download CSV
            </button>
          </div>
        </div>
      </div>

      <p className="note">
        IDs may be shortened on the cards. Hover a card for the full ID and SMILES,
        or download the CSV for every detail.
      </p>
    </section>
  );
}

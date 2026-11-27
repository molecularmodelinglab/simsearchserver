import InfoHover from './InfoHover';

const COUNTS = [8, 24, 48, 96];

export default function Options({
  libraries, library, onLibrary,
  count, onCount,
  sort, onSort,
  onExport, disabled, hasResults,
}) {
  const chosen = libraries.find((entry) => entry.name === library);

  return (
    <div className="options">
      <label className="option">
        <span>
          Library
          <InfoHover text={chosen
            ? `${chosen.description || chosen.display}. Larger libraries hold more analogs and take longer to search.`
            : 'Which compound collection to search.'} />
        </span>
        <select value={library || ''} onChange={(e) => onLibrary(e.target.value)}
                disabled={disabled || libraries.length === 0}>
          {libraries.map((entry) => (
            <option key={entry.name} value={entry.name}>{entry.display}</option>
          ))}
        </select>
        {chosen?.num_records ? (
          <small>
            {chosen.num_records.toLocaleString()} compounds
            {chosen.built_at && ` · indexed ${chosen.built_at.slice(0, 10)}`}
          </small>
        ) : null}
      </label>

      <label className="option">
        <span>
          Results
          <InfoHover text="How many analogs to display. The number searched is fixed; this only changes how many of the best are shown." />
        </span>
        <select value={count} onChange={(e) => onCount(Number(e.target.value))}
                disabled={disabled}>
          {COUNTS.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
      </label>

      <label className="option">
        <span>
          Sort by
          <InfoHover text="Similarity is the structural ranking and the default. Embedding distance is the order the index returned, before structural re-ranking." />
        </span>
        <select value={sort} onChange={(e) => onSort(e.target.value)} disabled={disabled}>
          <option value="similarity">similarity</option>
          <option value="distance">embedding distance</option>
        </select>
      </label>

      <div className="option">
        <span>
          Export
          <InfoHover text="Copy the identifiers, or download every displayed analog with its similarity and SMILES." />
        </span>
        <div className="export">
          <button onClick={() => onExport('ids')} disabled={disabled || !hasResults}>
            Copy IDs
          </button>
          <button onClick={() => onExport('csv')} disabled={disabled || !hasResults}>
            CSV
          </button>
        </div>
      </div>
    </div>
  );
}

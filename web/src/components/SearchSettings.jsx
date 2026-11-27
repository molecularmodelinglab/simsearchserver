import InfoHover from './InfoHover';

export default function SearchSettings({
  libraries, library, onLibrary, count, onCount, max, disabled,
}) {
  const chosen = libraries.find((entry) => entry.name === library);

  function set(value) {
    const n = Math.round(Number(value));

    if (Number.isFinite(n)) {
      onCount(Math.min(max, Math.max(1, n)));
    }
  }

  return (
    <div className="settings">
      <label className="setting setting-library">
        <span className="control-label">
          Library
          <InfoHover text={chosen
            ? `${chosen.description || chosen.display}. Larger libraries hold more compounds and take longer to search.`
            : 'Which compound collection to search.'} />
        </span>
        <select value={library || ''} onChange={(e) => onLibrary(e.target.value)}
                disabled={disabled || libraries.length === 0}>
          {libraries.length === 0 && <option value="">loading…</option>}
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

      <div className="setting setting-count">
        <label className="control-label" htmlFor="result-count">
          Results to show
          <InfoHover text={`How many of the best matches to return, from 1 to ${max}. The search itself always looks at the same candidate set.`} />
        </label>
        <div className="slider-row">
          <input
            id="result-count"
            type="range"
            min={1}
            max={max}
            value={count}
            onChange={(e) => set(e.target.value)}
            disabled={disabled}
            style={{ '--fill': `${((count - 1) / Math.max(1, max - 1)) * 100}%` }}
          />
          <input
            type="number"
            className="count-input"
            min={1}
            max={max}
            value={count}
            onChange={(e) => set(e.target.value)}
            disabled={disabled}
            aria-label="Results to show"
          />
        </div>
      </div>
    </div>
  );
}

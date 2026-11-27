import Depiction from './Depiction';
import { useCopy } from '../clipboard';

function Metric({ label, short, value, active }) {
  return (
    <div className={`metric${active ? ' is-active' : ''}`}>
      <span className="metric-label">
        <span className="label-long">{label}</span>
        <span className="label-short">{short}</span>
      </span>
      <span className="metric-value">{value}</span>
    </div>
  );
}

export default function ResultCard({ hit, rank, sort, exact }) {
  const [flashed, copy] = useCopy();

  const similarity = hit.similarity === undefined ? '—' : hit.similarity.toFixed(3);

  return (
    <article className={`result${exact ? ' is-exact' : ''}`} tabIndex={0}
             aria-label={`Result ${rank}: ${hit.identifier}`}>
      <div className="result-top">
        <span className="result-rank">#{rank}</span>
        {exact && <span className="exact-badge">exact match</span>}
      </div>

      <div className="metrics">
        <Metric label="Similarity" short="Sim." value={similarity} active={sort === 'similarity'} />
        <Metric label="Distance" short="Dist." value={hit.distance.toFixed(3)} active={sort === 'distance'} />
      </div>

      <Depiction smiles={hit.smiles} />

      <div className="result-id" title={hit.identifier}>{hit.identifier}</div>

      {/* Revealed on hover or focus, floating over the cards below. */}
      <div className="result-detail">
        <span className="detail-label">ID</span>
        <code className="detail-value">{hit.identifier}</code>
        <span className="detail-label">SMILES</span>
        <code className="detail-value">{hit.smiles}</code>
        <div className="detail-actions">
          <button type="button" className="chip-button" onClick={() => copy('id', hit.identifier)}>
            {flashed === 'id' ? 'Copied ✓' : flashed === 'id-failed' ? 'Copy failed' : 'Copy ID'}
          </button>
          <button type="button" className="chip-button" onClick={() => copy('smiles', hit.smiles)}>
            {flashed === 'smiles' ? 'Copied ✓' : flashed === 'smiles-failed' ? 'Copy failed' : 'Copy SMILES'}
          </button>
        </div>
      </div>
    </article>
  );
}

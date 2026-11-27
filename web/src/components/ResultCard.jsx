import { useState } from 'react';
import Depiction from './Depiction';

export default function ResultCard({ hit }) {
  const [copied, setCopied] = useState(null);

  function copy(what, value) {
    navigator.clipboard?.writeText(value).then(() => {
      setCopied(what);
      setTimeout(() => setCopied(null), 1200);
    });
  }

  return (
    <div className="card">
      <div className="card-top">
        {hit.similarity === undefined ? (
          <span className="metric metric-muted">rank by distance</span>
        ) : (
          <span className="metric">{hit.similarity.toFixed(3)}</span>
        )}
        <span className="metric-label">similarity</span>
      </div>

      <Depiction smiles={hit.smiles} />

      <button className="identifier" onClick={() => copy('id', hit.identifier)}
              title="Copy identifier">
        {copied === 'id' ? 'copied' : hit.identifier}
      </button>

      <div className="card-foot">
        <span className="distance">d {hit.distance.toFixed(3)}</span>
        <button className="smiles-copy" onClick={() => copy('smiles', hit.smiles)}
                title={hit.smiles}>
          {copied === 'smiles' ? 'copied' : 'SMILES'}
        </button>
      </div>
    </div>
  );
}

import { useEffect, useState } from 'react';
import ResultCard from './ResultCard';

// Cards mount in batches so the first row appears before the last is drawn.
// The parent remounts this grid (via `key`) for each new result set, so the
// batching restarts from the first row rather than from the old count.
const BATCH = 12;

export default function ResultGrid({ hits, sort, exact }) {
  const [shown, setShown] = useState(BATCH);

  useEffect(() => {
    if (shown >= hits.length) {
      return undefined;
    }

    const timer = setTimeout(() => setShown((n) => n + BATCH), 16);

    return () => clearTimeout(timer);
  }, [shown, hits.length]);

  return (
    <div className="grid">
      {hits.slice(0, shown).map((hit, i) => (
        <ResultCard
          key={hit.identifier + hit.smiles}
          hit={hit}
          rank={i + 1}
          sort={sort}
          exact={exact.has(hit.identifier)}
        />
      ))}
    </div>
  );
}

export function SkeletonGrid({ count }) {
  return (
    <div className="grid" aria-hidden="true">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="result skeleton-card" />
      ))}
    </div>
  );
}

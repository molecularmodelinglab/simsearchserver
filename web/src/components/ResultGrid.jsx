import { useEffect, useState } from 'react';
import ResultCard from './ResultCard';

// Cards mount in batches so the first row appears before the last is drawn.
const BATCH = 8;

export default function ResultGrid({ hits, onProgress }) {
  const [shown, setShown] = useState(BATCH);

  useEffect(() => {
    setShown(BATCH);
  }, [hits]);

  useEffect(() => {
    if (shown >= hits.length) {
      onProgress?.(hits.length);
      return;
    }

    onProgress?.(shown);
    const timer = setTimeout(() => setShown((n) => n + BATCH), 16);

    return () => clearTimeout(timer);
  }, [shown, hits, onProgress]);

  return (
    <div className="grid">
      {hits.slice(0, shown).map((hit) => (
        <ResultCard key={hit.identifier + hit.smiles} hit={hit} />
      ))}
    </div>
  );
}

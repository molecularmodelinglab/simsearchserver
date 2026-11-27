function ms(value) {
  return value >= 100 ? `${Math.round(value)} ms` : `${value.toFixed(1)} ms`;
}

export default function StatusLine({ status, timing, depicted, total, cached }) {
  if (status === 'idle') {
    return null;
  }

  const stages = [];

  if (timing) {
    stages.push(`embedded ${ms(timing.embed_ms)}`);
    stages.push(`search ${ms(timing.index_ms)}`);
  }

  if (status === 'searching') {
    stages.push('searching…');
  } else if (depicted < total) {
    stages.push(`depicting ${depicted}/${total}`);
  } else if (timing) {
    stages.push(`depicted ${total}`);
    stages.push(`total ${ms(timing.total_ms)}`);
  }

  if (cached) {
    stages.push('cached');
  }

  return <div className="status" role="status">{stages.join(' · ')}</div>;
}

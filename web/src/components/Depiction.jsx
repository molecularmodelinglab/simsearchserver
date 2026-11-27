import { useEffect, useState } from 'react';
import { depict } from '../rdkit';

export default function Depiction({ smiles, width = 190, height = 150 }) {
  const [svg, setSvg] = useState(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let live = true;
    setSvg(null);
    setFailed(false);

    depict(smiles, { width, height })
      .then((drawn) => live && (drawn ? setSvg(drawn) : setFailed(true)))
      .catch(() => live && setFailed(true));

    return () => {
      live = false;
    };
  }, [smiles, width, height]);

  if (failed) {
    return <div className="depiction depiction-failed" style={{ width, height }}>
      no structure
    </div>;
  }

  if (!svg) {
    return <div className="depiction skeleton" style={{ width, height }} />;
  }

  return (
    <div
      className="depiction"
      style={{ width, height }}
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}

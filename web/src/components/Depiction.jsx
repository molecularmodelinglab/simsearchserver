import { useEffect, useState } from 'react';
import { depict } from '../rdkit';

/**
 * `width`/`height` set the drawing's resolution and aspect; the box itself is
 * sized by CSS, and the SVG scales to fill it.
 */
export default function Depiction({ smiles, width = 188, height = 150, className = '' }) {
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

  const style = { aspectRatio: `${width} / ${height}` };

  if (failed) {
    return <div className={`depiction depiction-failed ${className}`} style={style}>
      no structure
    </div>;
  }

  if (!svg) {
    return <div className={`depiction skeleton ${className}`} style={style} />;
  }

  return (
    <div
      className={`depiction ${className}`}
      style={style}
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}

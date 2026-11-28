import { useEffect, useState } from 'react';
import Depiction from './Depiction';
import { rdkit } from '../rdkit';

const SHOWN = [
  ['MW', ['amw', 'exactmw'], (v) => v.toFixed(1)],
  ['cLogP', ['CrippenClogP'], (v) => v.toFixed(2)],
  ['TPSA', ['tpsa'], (v) => v.toFixed(1)],
  ['HBA', ['NumHBA', 'lipinskiHBA'], (v) => String(v)],
  ['HBD', ['NumHBD', 'lipinskiHBD'], (v) => String(v)],
  ['Rot. bonds', ['NumRotatableBonds'], (v) => String(v)],
];

export default function QueryPanel({ smiles, canonical }) {
  const [properties, setProperties] = useState([]);

  useEffect(() => {
    let live = true;
    setProperties([]);

    rdkit().then((module) => {
      const molecule = module.get_mol(smiles);

      if (!molecule) {
        return;
      }

      try {
        const all = JSON.parse(molecule.get_descriptors());
        const rows = [];

        SHOWN.forEach(([label, keys, format]) => {
          const key = keys.find((candidate) => typeof all[candidate] === 'number');
          if (key) {
            rows.push([label, format(all[key])]);
          }
        });

        if (live) {
          setProperties(rows);
        }
      } finally {
        molecule.delete();
      }
    }).catch(() => {});

    return () => {
      live = false;
    };
  }, [smiles]);

  return (
    <section className="panel query-panel" aria-label="Query">
      <h2 className="panel-label">Query</h2>
      <Depiction smiles={smiles} width={240} height={176} className="query-depiction" />
      <code className="query-smiles" title={canonical || smiles}>{canonical || smiles}</code>
      {properties.length > 0 && (
        <dl className="query-props">
          {properties.map(([label, value]) => (
            <div key={label}>
              <dt>{label}</dt>
              <dd>{value}</dd>
            </div>
          ))}
        </dl>
      )}
    </section>
  );
}

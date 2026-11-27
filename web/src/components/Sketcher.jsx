import '../ketcher-shim';
import { useEffect, useRef } from 'react';
import { Editor } from 'ketcher-react';
import { StandaloneStructServiceProvider } from 'ketcher-standalone';
import 'ketcher-react/dist/index.css';

// Indigo runs as WASM in the page, so there is no structure service to deploy.
const provider = new StandaloneStructServiceProvider();

export default function Sketcher({ smiles, onSmiles }) {
  const ketcher = useRef(null);
  const applied = useRef(null);

  // `applied` is what the canvas already shows, so drawing here does not bounce back.
  useEffect(() => {
    if (!ketcher.current || !smiles || smiles === applied.current) {
      return;
    }

    applied.current = smiles;
    ketcher.current.setMolecule(smiles).catch(() => {});
  }, [smiles]);

  function handleInit(instance) {
    ketcher.current = instance;
    // KetcherLogger reads this global, and throws during init without it.
    window.ketcher = instance;

    if (smiles) {
      applied.current = smiles;
      instance.setMolecule(smiles).catch(() => {});
    }
  }

  async function handleChange() {
    if (!ketcher.current) {
      return;
    }

    try {
      const drawn = await ketcher.current.getSmiles();

      if (drawn !== applied.current) {
        applied.current = drawn;
        onSmiles(drawn);
      }
    } catch {
      // A partially drawn structure has no SMILES yet.
    }
  }

  return (
    <div className="sketcher">
      <Editor
        staticResourcesUrl=""
        structServiceProvider={provider}
        errorHandler={() => {}}
        onInit={handleInit}
        onChange={handleChange}
      />
    </div>
  );
}

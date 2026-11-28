// Depiction only: the query fingerprint needs bits identical to the stored column.
let pending = null;

export function rdkit() {
  if (!pending) {
    pending = (async () => {
      if (typeof window.initRDKitModule !== 'function') {
        throw new Error('RDKit did not load');
      }

      return window.initRDKitModule({ locateFile: () => '/RDKit_minimal.wasm' });
    })().catch((error) => {
      pending = null;
      throw error;
    });
  }

  return pending;
}

const cache = new Map();
const LIMIT = 600;

export async function depict(smiles, { width, height }) {
  const key = `${smiles}|${width}x${height}`;

  if (cache.has(key)) {
    return cache.get(key);
  }

  const module = await rdkit();
  const molecule = module.get_mol(smiles);

  if (!molecule) {
    return null;
  }

  try {
    const details = { width, height, backgroundColour: [0, 0, 0, 0] };

    const svg = molecule.get_svg_with_highlights(JSON.stringify(details));

    if (cache.size > LIMIT) {
      cache.clear();
    }

    cache.set(key, svg);

    return svg;
  } finally {
    molecule.delete();
  }
}

/**
 * Canonical SMILES from this RDKit build, so two strings for the same molecule
 * compare equal. Both sides must come from here: canonical forms can differ
 * between RDKit versions, so the server's canonical SMILES is not comparable.
 */
export function canonicalize(module, smiles) {
  const molecule = module.get_mol(smiles);

  if (!molecule) {
    return null;
  }

  try {
    return molecule.get_smiles();
  } finally {
    molecule.delete();
  }
}

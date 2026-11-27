// Depiction only: the query fingerprint needs bits identical to the stored column.
let pending = null;

export function rdkit() {
  if (!pending) {
    pending = window
      .initRDKitModule({ locateFile: () => '/RDKit_minimal.wasm' })
      .catch((error) => {
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

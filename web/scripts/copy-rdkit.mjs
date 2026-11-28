import { copyFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const from = resolve(here, '../node_modules/@rdkit/rdkit/dist');
const to = resolve(here, '../public');

mkdirSync(to, { recursive: true });

for (const name of ['RDKit_minimal.js', 'RDKit_minimal.wasm']) {
  copyFileSync(resolve(from, name), resolve(to, name));
}

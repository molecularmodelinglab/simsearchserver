import { useEffect, useRef } from 'react';

export default function About({ open, onClose }) {
  const dialog = useRef(null);

  useEffect(() => {
    const node = dialog.current;

    if (!node) {
      return;
    }

    if (open && !node.open) {
      node.showModal();
    } else if (!open && node.open) {
      node.close();
    }
  }, [open]);

  return (
    <dialog
      ref={dialog}
      className="about"
      onClose={onClose}
      // A click on the backdrop lands on the dialog element itself.
      onClick={(event) => event.target === dialog.current && onClose()}
    >
      <div className="about-body">
        <div className="about-head">
          <h2>About SmallSA</h2>
          <button type="button" className="icon-button" onClick={onClose} aria-label="Close">
            <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
              <path d="M6 6l12 12M18 6L6 18" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
            </svg>
          </button>
        </div>

        <p>
          SmallSA looks up a query structure in billion-scale chemical libraries.
          If the library holds the molecule, it comes back first; if not, its
          closest neighbors by graph structure do.
        </p>
        <p>
          It encodes SMILES as low-dimensional vectors with SALSA, the
          Structurally Aware Latent Space Autoencoder, which is trained so that
          molecules with similar graph structure land near each other. The
          search returns whichever library compounds sit closest to the query in
          that space, which takes milliseconds over hundreds of millions of
          compounds. The retrieved set is then re-ranked by atom-pair Tanimoto
          similarity, because the embedding retrieves well but orders only
          approximately.
        </p>

        <div className="about-columns">
          <section>
            <h3>Cite</h3>
            <ul>
              <li><a href="https://doi.org/10.48550/arXiv.2402.07970" target="_blank" rel="noreferrer">SmallSA</a> — arXiv:2402.07970</li>
              <li><a href="https://doi.org/10.1609/aaai.v38i12.29221" target="_blank" rel="noreferrer">SALSA</a> — AAAI 2024</li>
            </ul>
          </section>
          <section>
            <h3>Code</h3>
            <ul>
              <li><a href="https://github.com/molecularmodelinglab/simsearchserver" target="_blank" rel="noreferrer">simsearchserver</a></li>
              <li><a href="https://github.com/molecularmodelinglab/SALSA" target="_blank" rel="noreferrer">SALSA</a></li>
            </ul>
          </section>
        </div>

        <p className="credits">
          Developed by Enes Kelestemur, Travis Maxfield, James Wellnitz, Josh
          Hochuli and Kat Kirchoff at the{' '}
          <a href="https://molecularmodelinglab.github.io/" target="_blank" rel="noreferrer">Molecular Modeling Lab, UNC</a>.
          Structure editing by{' '}
          <a href="https://github.com/epam/ketcher" target="_blank" rel="noreferrer">Ketcher</a>, depiction by{' '}
          <a href="https://www.rdkit.org/" target="_blank" rel="noreferrer">RDKit</a>. © 2026
        </p>
      </div>
    </dialog>
  );
}

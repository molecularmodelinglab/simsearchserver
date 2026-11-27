export default function About({ open }) {
  return (
    <details className="about" open={open}>
      <summary>About SmallSA</summary>

      <p>
        SmallSA finds low graph-edit-distance analogs for a query structure in
        billion-scale chemical libraries. It encodes SMILES as low-dimensional
        vectors with SALSA, the Structurally Aware Latent Space Autoencoder,
        which is trained so that molecules with similar graph structure land
        near each other. Analogs are then whichever library compounds sit
        closest to the query in that space, which takes milliseconds over
        hundreds of millions of compounds. The retrieved set is re-ranked by
        atom-pair Tanimoto similarity, because the embedding retrieves well but
        orders only approximately.
      </p>

      <p className="cite">
        Please cite{' '}
        <a href="https://doi.org/10.48550/arXiv.2402.07970">SmallSA</a>
        {' '}and{' '}
        <a href="https://doi.org/10.1609/aaai.v38i12.29221">SALSA</a>. Code:{' '}
        <a href="https://github.com/molecularmodelinglab/simsearchserver">simsearchserver</a>
        {' · '}
        <a href="https://github.com/molecularmodelinglab/SALSA">SALSA</a>.
      </p>

      <p className="credits">
        Developed by Enes Kelestemur, Travis Maxfield, James Wellnitz,
        Josh Hochuli and Kat Kirchoff —{' '}
        <a href="https://molecularmodelinglab.github.io/">Molecular Modeling Lab at UNC</a>
      </p>

      <p className="credits">
        Structure editing by{' '}
        <a href="https://github.com/epam/ketcher">Ketcher</a>, depiction by{' '}
        <a href="https://www.rdkit.org/">RDKit</a>. © 2026
      </p>
    </details>
  );
}

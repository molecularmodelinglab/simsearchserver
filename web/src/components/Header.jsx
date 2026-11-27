const PAPER = 'https://doi.org/10.48550/arXiv.2402.07970';
const CODE = 'https://github.com/molecularmodelinglab/simsearchserver';

function Sun() {
  return (
    <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
      <circle cx="12" cy="12" r="4.2" fill="none" stroke="currentColor" strokeWidth="1.8" />
      <g stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
        <path d="M12 2.5v2.2M12 19.3v2.2M2.5 12h2.2M19.3 12h2.2M5.3 5.3l1.6 1.6M17.1 17.1l1.6 1.6M5.3 18.7l1.6-1.6M17.1 6.9l1.6-1.6" />
      </g>
    </svg>
  );
}

function Moon() {
  return (
    <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
      <path d="M20 14.6A8.2 8.2 0 0 1 9.4 4a8.2 8.2 0 1 0 10.6 10.6Z"
            fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
    </svg>
  );
}

export default function Header({ dark, onTheme, onAbout }) {
  return (
    <header className="masthead">
      <a className="brand" href="/" aria-label="SmallSA home">
        <span className="logo-tile"><img src="/logo.svg" alt="Molecular Modeling Lab" className="logo" /></span>
        <span className="brand-text">
          <span className="brand-name">SmallSA</span>
          <span className="brand-tagline">Find any molecule in billion-scale libraries, in milliseconds</span>
        </span>
      </a>

      <nav className="masthead-nav">
        <button type="button" className="nav-link" onClick={onAbout}>About</button>
        <a className="nav-link" href={PAPER} target="_blank" rel="noreferrer">Paper</a>
        <a className="nav-link" href={CODE} target="_blank" rel="noreferrer">Code</a>
        <button type="button" className="icon-button" onClick={onTheme}
                title={dark ? 'Switch to light mode' : 'Switch to dark mode'}
                aria-label={dark ? 'Switch to light mode' : 'Switch to dark mode'}>
          {dark ? <Sun /> : <Moon />}
        </button>
      </nav>
    </header>
  );
}

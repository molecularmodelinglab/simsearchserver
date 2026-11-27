// ketcher-react calls require('raphael') at module scope; imported before it.
import raphael from 'raphael';

if (typeof window !== 'undefined' && typeof window.require === 'undefined') {
  window.require = (name) => {
    if (name === 'raphael') {
      return raphael;
    }

    throw new Error(`unsupported require(${name})`);
  };
}

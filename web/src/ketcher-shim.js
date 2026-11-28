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

// Ketcher's bundled Node polyfills read `process`. The production build
// resolves those references, the dev server does not, so dev gets a stub.
if (import.meta.env.DEV && typeof globalThis.process === 'undefined') {
  globalThis.process = { env: {}, browser: true, nextTick: (fn, ...args) => queueMicrotask(() => fn(...args)) };
}

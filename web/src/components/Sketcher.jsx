import '../ketcher-shim';
import { forwardRef, useEffect, useImperativeHandle, useRef } from 'react';
import { Editor } from 'ketcher-react';
import { StandaloneStructServiceProvider } from 'ketcher-standalone';
import 'ketcher-react/dist/index.css';

// Indigo runs as WASM in the page, so there is no structure service to deploy.
const provider = new StandaloneStructServiceProvider();

// Drawing fires a burst of change events; the SMILES is read once it settles.
const SETTLE_MS = 250;

/**
 * Sync is one way: whatever is drawn (or pasted) on the canvas is reported via
 * `onDrawn`. The canvas only changes from outside through `show(smiles)`, which
 * the parent calls on an explicit request, so half-typed SMILES never reach it.
 */
const Sketcher = forwardRef(function Sketcher({ initial, onDrawn }, ref) {
  const ketcher = useRef(null);
  // The SMILES of what the canvas holds, as Ketcher writes it.
  const baseline = useRef(null);
  // True while `show` replaces the canvas, so its own events are not reported back.
  const showing = useRef(false);
  // Resolves the wait in `show` once the canvas reports the new molecule.
  const settled = useRef(null);
  const timer = useRef(null);
  const subscription = useRef(null);
  const report = useRef(onDrawn);
  report.current = onDrawn;

  async function show(smiles) {
    const instance = ketcher.current;

    if (!instance) {
      throw new Error('The structure editor is still loading.');
    }

    showing.current = true;

    try {
      // setMolecule resolves before the canvas updates; the change event
      // marks the update. The timeout covers a molecule that changes nothing.
      const updated = new Promise((resolve) => {
        settled.current = resolve;
        setTimeout(resolve, 1500);
      });

      await instance.setMolecule(smiles);
      await updated;
      await new Promise((resolve) => setTimeout(resolve, 60));

      baseline.current = await instance.getSmiles().catch(() => null);
    } finally {
      settled.current = null;
      showing.current = false;
    }
  }

  useImperativeHandle(ref, () => ({ show }), []);

  useEffect(() => () => {
    clearTimeout(timer.current);

    if (ketcher.current && subscription.current) {
      ketcher.current.editor.unsubscribe('change', subscription.current);
    }
  }, []);

  function handleInit(instance) {
    ketcher.current = instance;
    // KetcherLogger reads this global, and throws during init without it.
    window.ketcher = instance;

    subscription.current = instance.editor.subscribe('change', () => {
      settled.current?.();
      clearTimeout(timer.current);

      timer.current = setTimeout(async () => {
        if (showing.current) {
          return;
        }

        try {
          const drawn = await instance.getSmiles();

          if (drawn !== baseline.current) {
            baseline.current = drawn;
            report.current(drawn);
          }
        } catch {
          // A partially drawn structure has no SMILES yet.
        }
      }, SETTLE_MS);
    });

    if (initial) {
      show(initial).catch(() => {});
    }
  }

  return (
    <div className="sketcher">
      <Editor
        staticResourcesUrl=""
        structServiceProvider={provider}
        errorHandler={() => {}}
        onInit={handleInit}
      />
    </div>
  );
});

export default Sketcher;

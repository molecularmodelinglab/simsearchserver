import { useEffect, useRef, useState } from 'react';

/**
 * Copies text, falling back to execCommand where the async clipboard API is
 * missing: it only exists on HTTPS and localhost, not a plain-HTTP deployment.
 */
export async function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Fall through to the legacy path.
    }
  }

  const area = document.createElement('textarea');
  area.value = text;
  area.setAttribute('readonly', '');
  area.style.position = 'fixed';
  area.style.opacity = '0';
  document.body.appendChild(area);
  area.select();

  let copied = false;
  try {
    copied = document.execCommand('copy');
  } catch {
    copied = false;
  }

  area.remove();

  return copied;
}

/** `[flashed, copy]`: `flashed` names what was just copied, for a moment. */
export function useCopy(duration = 1400) {
  const [flashed, setFlashed] = useState(null);
  const timer = useRef(null);

  useEffect(() => () => clearTimeout(timer.current), []);

  async function copy(what, text) {
    const copied = await copyText(text);
    clearTimeout(timer.current);
    setFlashed(copied ? what : `${what}-failed`);
    timer.current = setTimeout(() => setFlashed(null), duration);
  }

  return [flashed, copy];
}

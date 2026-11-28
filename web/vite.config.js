import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  // ketcher reaches for Node globals that a browser bundle has to supply.
  define: { global: 'globalThis' },
  // Relative /api keeps who serves the static build a config choice.
  server: { proxy: { '/api': 'http://127.0.0.1:8000' } },
  build: { outDir: 'dist', chunkSizeWarningLimit: 4096 },
});

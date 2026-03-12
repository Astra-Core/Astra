import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 4173,
    proxy: {
      '/api': 'http://127.0.0.1:8080',
      '/health': 'http://127.0.0.1:8080',
      '/ready': 'http://127.0.0.1:8080',
      '/version': 'http://127.0.0.1:8080'
    }
  },
  preview: {
    host: '127.0.0.1',
    port: 4173
  }
});

import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import { fileURLToPath, URL } from 'node:url'

const frontendRoot = fileURLToPath(new URL('.', import.meta.url))
const brandRoot = fileURLToPath(new URL('../brand', import.meta.url))

export default defineConfig({
  plugins: [react()],
  // Only explicitly imported approved assets are emitted into dist.
  publicDir: false,
  server: {
    fs: { allow: [frontendRoot, brandRoot] },
    proxy: {
      '/api': 'http://127.0.0.1:8000',
    },
  },
  build: {
    rollupOptions: {
      input: {
        app: fileURLToPath(new URL('./index.html', import.meta.url)),
        music: fileURLToPath(new URL('./music.html', import.meta.url)),
      },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
  },
})

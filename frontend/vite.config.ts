import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { readFileSync } from 'node:fs'
import { fileURLToPath, URL } from 'node:url'

const frontendRoot = fileURLToPath(new URL('.', import.meta.url))
const brandRoot = fileURLToPath(new URL('../brand', import.meta.url))
// The release the app shows on screen: see src/version.ts.
const { version } = JSON.parse(
  readFileSync(new URL('./package.json', import.meta.url), 'utf-8'),
) as { version: string }

export default defineConfig({
  plugins: [react()],
  define: {
    __APP_VERSION__: JSON.stringify(version),
  },
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
})

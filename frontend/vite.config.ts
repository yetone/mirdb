/// <reference types="vitest" />
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import type { Plugin } from 'vite'

// Security headers configuration
const securityHeaders = {
  'Content-Security-Policy': [
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline'",
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data: https:",
    "font-src 'self'",
    "connect-src 'self'",
    "frame-ancestors 'none'",
    "base-uri 'self'",
    "form-action 'self'"
  ].join('; '),
  'X-Frame-Options': 'DENY',
  'X-Content-Type-Options': 'nosniff',
  'X-XSS-Protection': '1; mode=block',
  'Referrer-Policy': 'strict-origin-when-cross-origin',
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains'
}

// HTTPS redirect middleware plugin
function httpsRedirectPlugin(): Plugin {
  return {
    name: 'https-redirect',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        // Check if request is HTTP and should be redirected to HTTPS
        const proto = req.headers['x-forwarded-proto']
        if (proto === 'http') {
          const host = req.headers.host || 'localhost'
          const httpsUrl = `https://${host}${req.url}`
          res.writeHead(301, { Location: httpsUrl })
          res.end()
          return
        }
        next()
      })
    }
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), httpsRedirectPlugin()],
  server: {
    headers: securityHeaders
  },
  preview: {
    headers: securityHeaders
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    css: true,
    include: ['src/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}'],
  },
})

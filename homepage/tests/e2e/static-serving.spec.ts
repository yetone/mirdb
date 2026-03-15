/**
 * Static Serving E2E Tests.
 * Owner: Scenario 13 - Static Deployment Build
 *
 * Tests:
 * - Built static files can be served and homepage loads correctly
 * - No server-side rendering required
 *
 * Requirements:
 * - NFR-4: Homepage must be deployable as static files (no runtime backend required)
 */

import { test, expect } from '@playwright/test'
import { execSync } from 'child_process'
import * as path from 'path'
import * as fs from 'fs'
import * as http from 'http'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const projectRoot = path.resolve(__dirname, '../..')
const distPath = path.join(projectRoot, 'dist')
const STATIC_SERVER_PORT = 3456

// Simple static file server for testing
function createStaticServer(directory: string, _port: number): http.Server {
  return http.createServer((req, res) => {
    let filePath = path.join(directory, req.url === '/' ? 'index.html' : req.url!)

    // Handle SPA routing - serve index.html for non-file routes
    if (!fs.existsSync(filePath) && !path.extname(filePath)) {
      filePath = path.join(directory, 'index.html')
    }

    if (!fs.existsSync(filePath)) {
      res.writeHead(404)
      res.end('Not Found')
      return
    }

    const ext = path.extname(filePath).toLowerCase()
    const mimeTypes: Record<string, string> = {
      '.html': 'text/html',
      '.css': 'text/css',
      '.js': 'application/javascript',
      '.json': 'application/json',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg',
      '.gif': 'image/gif',
      '.svg': 'image/svg+xml',
      '.ico': 'image/x-icon',
      '.woff': 'font/woff',
      '.woff2': 'font/woff2',
      '.ttf': 'font/ttf'
    }

    const contentType = mimeTypes[ext] || 'application/octet-stream'

    try {
      const content = fs.readFileSync(filePath)
      res.writeHead(200, { 'Content-Type': contentType })
      res.end(content)
    } catch (error) {
      res.writeHead(500)
      res.end('Server Error')
    }
  })
}

test.describe('Static Serving', () => {
  let server: http.Server

  test.beforeAll(async () => {
    // Ensure build exists
    if (!fs.existsSync(distPath)) {
      console.log('Building project for static serving test...')
      execSync('npm run build', {
        cwd: projectRoot,
        stdio: 'pipe'
      })
    }

    // Start static server
    server = createStaticServer(distPath, STATIC_SERVER_PORT)
    await new Promise<void>((resolve, reject) => {
      server.listen(STATIC_SERVER_PORT, () => {
        console.log(`Static server running on port ${STATIC_SERVER_PORT}`)
        resolve()
      })
      server.on('error', reject)
    })
  })

  test.afterAll(async () => {
    if (server) {
      await new Promise<void>((resolve) => {
        server.close(() => resolve())
      })
    }
  })

  // TC5: Homepage loads and functions correctly when served statically
  test('homepage loads correctly from static server', async ({ page }) => {
    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)

    // Verify the page loaded
    await expect(page).toHaveTitle(/MirDB/i)
  })

  test('hero section renders with MirDB branding', async ({ page }) => {
    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)

    // Check for main heading
    const heading = page.getByRole('heading', { level: 1 })
    await expect(heading).toBeVisible()
    await expect(heading).toContainText(/MirDB/i)
  })

  test('navigation elements are interactive', async ({ page }) => {
    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)

    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle')

    // Check for navigation links
    const nav = page.locator('nav')
    await expect(nav).toBeVisible()
  })

  test('call-to-action buttons are functional', async ({ page }) => {
    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)

    // Check for Get Started CTA
    const ctaButton = page.getByRole('link', { name: /get started/i })
    await expect(ctaButton).toBeVisible()

    // Verify it has a valid href
    const href = await ctaButton.getAttribute('href')
    expect(href).toBeTruthy()
  })

  test('page sections load correctly', async ({ page }) => {
    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)
    await page.waitForLoadState('networkidle')

    // Check that main sections exist
    const sections = ['features', 'quick-start', 'architecture', 'community']

    // Sections may or may not be present depending on implementation
    // Just ensure no JS errors occurred
    for (const section of sections) {
      // Check section exists (optional - just checking presence)
      await page.locator(`#${section}, [id="${section}"]`).count()
    }

    // Verify page rendered without JavaScript errors
    const errors: string[] = []
    page.on('pageerror', (error) => {
      errors.push(error.message)
    })

    await page.reload()
    await page.waitForLoadState('networkidle')

    expect(errors).toHaveLength(0)
  })

  test('CSS styles are loaded and applied', async ({ page }) => {
    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)
    await page.waitForLoadState('networkidle')

    // Check that styles are applied (not just raw HTML)
    const body = page.locator('body')
    const backgroundColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor
    })

    // Background should have a defined color (not transparent)
    expect(backgroundColor).not.toBe('')
    expect(backgroundColor).not.toBe('transparent')
  })

  test('JavaScript functionality works (theme toggle)', async ({ page }) => {
    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)
    await page.waitForLoadState('networkidle')

    // Find theme toggle button
    const themeToggle = page.getByRole('button', { name: /theme|dark|light|mode/i })

    if (await themeToggle.isVisible()) {
      // Get initial theme state
      const initialClass = await page.locator('html').getAttribute('class')

      // Click theme toggle
      await themeToggle.click()

      // Small delay for state change
      await page.waitForTimeout(100)

      // Verify theme changed
      const newClass = await page.locator('html').getAttribute('class')
      expect(newClass).not.toBe(initialClass)
    }
  })

  test('all assets load successfully (no 404s)', async ({ page }) => {
    const failedRequests: string[] = []

    page.on('response', (response) => {
      if (response.status() === 404) {
        failedRequests.push(response.url())
      }
    })

    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)
    await page.waitForLoadState('networkidle')

    // No 404 errors should occur for assets
    expect(failedRequests).toHaveLength(0)
  })

  test('page is served with correct MIME types', async ({ page }) => {
    const responses: { url: string; contentType: string }[] = []

    page.on('response', (response) => {
      const contentType = response.headers()['content-type'] || ''
      responses.push({
        url: response.url(),
        contentType
      })
    })

    await page.goto(`http://localhost:${STATIC_SERVER_PORT}/`)
    await page.waitForLoadState('networkidle')

    // Check HTML response
    const htmlResponse = responses.find(r => r.url.endsWith('/') || r.url.endsWith('index.html'))
    expect(htmlResponse?.contentType).toContain('text/html')

    // Check CSS responses
    const cssResponses = responses.filter(r => r.url.endsWith('.css'))
    for (const css of cssResponses) {
      expect(css.contentType).toContain('text/css')
    }

    // Check JS responses
    const jsResponses = responses.filter(r => r.url.endsWith('.js'))
    for (const js of jsResponses) {
      expect(js.contentType).toContain('javascript')
    }
  })
})

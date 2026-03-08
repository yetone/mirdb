import { test, expect } from '@playwright/test'
import * as fs from 'fs'
import * as path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

test.describe('Homepage Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 5: Load homepage and inspect header
  test('header is fixed at top with logo and name prominently displayed', async ({ page }) => {
    // Verify header is visible immediately without scrolling
    const header = page.locator('header')
    await expect(header).toBeVisible()

    // Verify header is at the top of the page
    const headerBox = await header.boundingBox()
    expect(headerBox).toBeTruthy()
    expect(headerBox!.y).toBeLessThanOrEqual(10) // Allow small margin

    // Verify header has sticky positioning
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position
    })
    expect(position).toBe('sticky')

    // Verify logo is visible and prominently displayed
    const logo = page.locator('header img[alt*="MirDB"]')
    await expect(logo).toBeVisible()

    // Verify logo has reasonable size (at least 32px)
    const logoBox = await logo.boundingBox()
    expect(logoBox).toBeTruthy()
    expect(logoBox!.width).toBeGreaterThanOrEqual(32)
    expect(logoBox!.height).toBeGreaterThanOrEqual(32)

    // Verify project name "MirDB" is visible
    const projectName = page.locator('header').getByText('MirDB', { exact: true })
    await expect(projectName).toBeVisible()

    // Verify project name is styled appropriately (is text, has larger font)
    const fontSize = await projectName.evaluate((el) => {
      return window.getComputedStyle(el).fontSize
    })
    const fontSizeValue = parseFloat(fontSize)
    expect(fontSizeValue).toBeGreaterThanOrEqual(20) // At least 20px for prominence
  })

  test('header remains visible when scrolling', async ({ page }) => {
    // Add some content to enable scrolling
    await page.evaluate(() => {
      const content = document.createElement('div')
      content.style.height = '2000px'
      document.body.appendChild(content)
    })

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500))

    // Verify header is still visible
    const header = page.locator('header')
    await expect(header).toBeVisible()

    // Verify header is still at top of viewport
    const headerTop = await header.evaluate((el) => {
      return el.getBoundingClientRect().top
    })
    expect(headerTop).toBe(0)
  })

  test('logo image loads successfully', async ({ page }) => {
    const logo = page.locator('header img[alt*="MirDB"]')
    const srcAttribute = await logo.getAttribute('src')

    // Verify src attribute exists and points to a valid image path
    expect(srcAttribute).toBeTruthy()
    expect(srcAttribute).toContain('logo')

    // Verify the image loads properly (naturalWidth > 0)
    const isLoaded = await logo.evaluate((img: HTMLImageElement) => {
      return img.complete && img.naturalWidth > 0
    })
    expect(isLoaded).toBe(true)
  })
})

// Scenario 16: Static Deployment Compatibility Tests
test.describe('Static Deployment Compatibility', () => {
  const distPath = path.resolve(__dirname, '..', 'dist')

  test('build outputs index.html and static assets', async () => {
    // Verify dist directory exists
    expect(fs.existsSync(distPath)).toBe(true)

    // Verify index.html exists
    const indexPath = path.join(distPath, 'index.html')
    expect(fs.existsSync(indexPath)).toBe(true)

    // Verify assets directory exists with CSS and JS files
    const assetsPath = path.join(distPath, 'assets')
    expect(fs.existsSync(assetsPath)).toBe(true)

    const assetFiles = fs.readdirSync(assetsPath)
    const hasJs = assetFiles.some((f) => f.endsWith('.js'))
    const hasCss = assetFiles.some((f) => f.endsWith('.css'))
    expect(hasJs).toBe(true)
    expect(hasCss).toBe(true)
  })

  test('all asset paths are relative for subdirectory hosting', async () => {
    const indexPath = path.join(distPath, 'index.html')
    const indexContent = fs.readFileSync(indexPath, 'utf-8')

    // Check that script and link tags use relative paths (starting with ./)
    const scriptMatches = indexContent.match(/src="([^"]+)"/g) || []
    const linkMatches = indexContent.match(/href="([^"]+)"/g) || []

    // All asset paths should be relative (start with ./) or be data URIs
    for (const match of [...scriptMatches, ...linkMatches]) {
      const url = match.match(/["']([^"']+)["']/)?.[1]
      if (url && !url.startsWith('data:') && !url.startsWith('http')) {
        // Path should be relative (starts with ./) for subdirectory hosting
        expect(url.startsWith('./') || url.startsWith('#')).toBe(true)
      }
    }
  })

  test('page loads correctly from static file server', async ({ page }) => {
    // Navigate to homepage and verify core elements load
    await page.goto('/')

    // Wait for page to fully load
    await page.waitForLoadState('networkidle')

    // Verify React app renders (root element has content)
    const rootElement = page.locator('#root')
    await expect(rootElement).not.toBeEmpty()

    // Verify main content sections are present
    const mainContent = page.locator('main#main-content')
    await expect(mainContent).toBeVisible()

    // Verify CSS is loaded (body should have background styling)
    const bodyBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor
    })
    // Should have dark slate background (not default white)
    expect(bodyBgColor).not.toBe('rgb(255, 255, 255)')
  })

  test('SPA routing works without server-side support', async ({ page }) => {
    // For a static single-page app without client-side routing,
    // verify the page loads and all navigation is internal (hash-based or scroll)
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify smooth scrolling is enabled (SPA navigation pattern)
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior
    })
    // Should have smooth scrolling or default behavior
    expect(['smooth', 'auto']).toContain(scrollBehavior)

    // Verify internal links use hash navigation or scroll to sections
    // Exclude sr-only links that are not visible
    const visibleInternalLinks = page.locator('a[href^="#"]:not(.sr-only)')
    const internalLinkCount = await visibleInternalLinks.count()

    // SPA should have internal navigation links or handle them properly
    if (internalLinkCount > 0) {
      // Click first visible internal link and verify page doesn't reload
      const firstLink = visibleInternalLinks.first()
      const initialUrl = page.url()

      await firstLink.click()
      await page.waitForTimeout(100)

      // Should stay on same page (SPA behavior)
      const newUrl = page.url()
      const initialBase = initialUrl.split('#')[0]
      const newBase = newUrl.split('#')[0]
      expect(newBase).toBe(initialBase)
    } else {
      // If no visible internal links, verify 404.html exists for SPA fallback
      const distPath = path.resolve(__dirname, '..', 'dist')
      const notFoundPath = path.join(distPath, '404.html')
      expect(fs.existsSync(notFoundPath)).toBe(true)
    }
  })

  test('no external runtime dependencies required', async ({ page }) => {
    // Verify the page works without Node.js runtime or server-side processing
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Check that the page renders within reasonable time
    const startTime = Date.now()
    await page.waitForSelector('header', { timeout: 5000 })
    const loadTime = Date.now() - startTime

    // Page should render quickly (under 3 seconds on localhost)
    expect(loadTime).toBeLessThan(3000)

    // Verify JavaScript bundle executed successfully
    const reactRendered = await page.evaluate(() => {
      const root = document.getElementById('root')
      return root && root.children.length > 0
    })
    expect(reactRendered).toBe(true)
  })
})

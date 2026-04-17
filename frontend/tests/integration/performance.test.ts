/**
 * Performance Integration Tests
 * Owner: Scenario 13 - Performance and Loading
 *
 * Tests bundle size and build optimization.
 * These tests analyze the production build output.
 *
 * Related requirements: NFR-1, NFR-2
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { execSync } from 'child_process'
import { readdirSync, statSync, readFileSync, existsSync } from 'fs'
import { join } from 'path'
import { gzipSync } from 'zlib'

const DIST_DIR = join(__dirname, '../../dist')
const MAX_BUNDLE_SIZE_GZIPPED = 250 * 1024 // 250KB gzipped

describe('Performance Integration Tests - Scenario 13', () => {
  beforeAll(() => {
    // Run the production build (use vite build directly to skip tsc on test files)
    try {
      execSync('npx vite build', {
        cwd: join(__dirname, '../..'),
        encoding: 'utf-8',
        timeout: 120000, // 2 minute timeout for build
      })
    } catch (error) {
      console.error('Build failed:', error)
      throw error
    }
  }, 180000) // 3 minute timeout for beforeAll

  // Test Case 4: Analyze main JavaScript bundle size
  it('main JavaScript bundle is under 250KB gzipped', () => {
    expect(existsSync(DIST_DIR)).toBe(true)

    const assetsDir = join(DIST_DIR, 'assets')
    expect(existsSync(assetsDir)).toBe(true)

    // Find all JS files in the build output
    const jsFiles = readdirSync(assetsDir).filter((file) => file.endsWith('.js'))
    expect(jsFiles.length).toBeGreaterThan(0)

    // Find the main bundle (usually the largest or index file)
    let mainBundlePath = ''
    let mainBundleSize = 0

    for (const file of jsFiles) {
      const filePath = join(assetsDir, file)
      const stats = statSync(filePath)

      // Track the largest bundle
      if (stats.size > mainBundleSize) {
        mainBundleSize = stats.size
        mainBundlePath = filePath
      }
    }

    // Read and gzip the main bundle
    const content = readFileSync(mainBundlePath)
    const gzipped = gzipSync(content)
    const gzippedSize = gzipped.length

    console.log(`Main bundle: ${mainBundlePath}`)
    console.log(`Original size: ${(mainBundleSize / 1024).toFixed(2)}KB`)
    console.log(`Gzipped size: ${(gzippedSize / 1024).toFixed(2)}KB`)

    // Main bundle should be under 250KB gzipped
    expect(gzippedSize).toBeLessThan(MAX_BUNDLE_SIZE_GZIPPED)
  })

  // Test: Total bundle size is reasonable
  it('total JavaScript bundle size is reasonable', () => {
    const assetsDir = join(DIST_DIR, 'assets')
    const jsFiles = readdirSync(assetsDir).filter((file) => file.endsWith('.js'))

    let totalGzippedSize = 0

    for (const file of jsFiles) {
      const filePath = join(assetsDir, file)
      const content = readFileSync(filePath)
      const gzipped = gzipSync(content)
      totalGzippedSize += gzipped.length
    }

    console.log(`Total JS gzipped size: ${(totalGzippedSize / 1024).toFixed(2)}KB`)

    // Total JS should be under 500KB gzipped
    expect(totalGzippedSize).toBeLessThan(500 * 1024)
  })

  // Test: CSS bundle size is reasonable
  it('CSS bundle is appropriately sized', () => {
    const assetsDir = join(DIST_DIR, 'assets')
    const cssFiles = readdirSync(assetsDir).filter((file) => file.endsWith('.css'))

    let totalCssSize = 0

    for (const file of cssFiles) {
      const filePath = join(assetsDir, file)
      const content = readFileSync(filePath)
      const gzipped = gzipSync(content)
      totalCssSize += gzipped.length
    }

    console.log(`Total CSS gzipped size: ${(totalCssSize / 1024).toFixed(2)}KB`)

    // CSS should be under 50KB gzipped (Tailwind is tree-shaken)
    expect(totalCssSize).toBeLessThan(50 * 1024)
  })

  // Test Case 5: Check for render-blocking resources in build output
  it('HTML is optimized for fast loading', () => {
    const indexPath = join(DIST_DIR, 'index.html')
    expect(existsSync(indexPath)).toBe(true)

    const html = readFileSync(indexPath, 'utf-8')

    // Scripts should be modules (deferred by default)
    expect(html).toMatch(/<script[^>]*type="module"/)

    // Check that CSS is linked (Vite extracts CSS)
    const hasLinkCss = html.includes('<link rel="stylesheet"') ||
                        html.includes('rel="stylesheet"')

    // If there's external CSS, it should be properly linked
    // Vite generates CSS that's loaded alongside JS
    if (hasLinkCss) {
      console.log('External CSS is linked in HTML')
    }

    // No render-blocking scripts without defer/async/module
    const hasBlockingScript = /<script(?![^>]*(?:async|defer|type="module"))[^>]*src=/i.test(html)
    expect(hasBlockingScript).toBe(false)
  })

  // Test: Build produces valid output
  it('build produces valid HTML with correct structure', () => {
    const indexPath = join(DIST_DIR, 'index.html')
    const html = readFileSync(indexPath, 'utf-8')

    // Check for required HTML structure
    expect(html).toContain('<!DOCTYPE html>')
    expect(html).toContain('<html')
    expect(html).toContain('<head>')
    expect(html).toContain('<body>')
    expect(html).toContain('<div id="root">')

    // Check for meta tags (SEO)
    expect(html).toContain('<meta charset=')
    expect(html).toContain('<meta name="viewport"')
    expect(html).toContain('<meta name="description"')
  })
})

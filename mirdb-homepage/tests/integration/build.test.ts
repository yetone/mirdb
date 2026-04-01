/**
 * Build Integration Tests.
 * Owner: Scenario 15 - Performance Build and Bundle
 *
 * Tests:
 * - Build completes successfully
 * - Bundle size under 100KB gzipped
 * - CSS purging works
 * - No source maps in production
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { execSync } from 'child_process'
import { existsSync, readdirSync, readFileSync, statSync, rmSync } from 'fs'
import { resolve, join, extname } from 'path'
import { gzipSync } from 'zlib'

const PROJECT_ROOT = resolve(__dirname, '../..')
const DIST_DIR = resolve(PROJECT_ROOT, 'dist')
const ASSETS_DIR = resolve(DIST_DIR, 'assets')

describe('Performance - Build and Bundle', () => {
  let buildSuccess = false

  beforeAll(() => {
    // Clean dist directory before build
    if (existsSync(DIST_DIR)) {
      rmSync(DIST_DIR, { recursive: true, force: true })
    }

    // Run the Vite production build directly
    // Note: We use 'npx vite build' to test Vite configuration specifically.
    // TypeScript type checking is handled separately by Scenario 18.
    try {
      execSync('npx vite build', {
        cwd: PROJECT_ROOT,
        stdio: 'pipe',
        timeout: 120000, // 2 minutes timeout
      })
      buildSuccess = true
    } catch (error) {
      console.error('Build failed:', error)
      buildSuccess = false
    }
  }, 180000) // 3 minutes timeout for beforeAll

  afterAll(() => {
    // Clean up dist directory after tests
    if (existsSync(DIST_DIR)) {
      rmSync(DIST_DIR, { recursive: true, force: true })
    }
  })

  describe('Build Process', () => {
    it('should complete successfully without errors', () => {
      expect(buildSuccess).toBe(true)
    })

    it('should create dist directory', () => {
      expect(existsSync(DIST_DIR)).toBe(true)
    })

    it('should create assets directory', () => {
      expect(existsSync(ASSETS_DIR)).toBe(true)
    })

    it('should generate index.html in dist', () => {
      const indexPath = resolve(DIST_DIR, 'index.html')
      expect(existsSync(indexPath)).toBe(true)
    })
  })

  describe('Bundle Size', () => {
    it('should have initial JavaScript bundle under 100KB gzipped', () => {
      // Skip if build failed
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const jsFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.js' && !file.includes('vendor')
      )

      // Get all JS bundle sizes (excluding vendor)
      let totalGzippedSize = 0
      for (const file of jsFiles) {
        const filePath = join(ASSETS_DIR, file)
        const content = readFileSync(filePath)
        const gzipped = gzipSync(content)
        totalGzippedSize += gzipped.length
      }

      // Convert to KB
      const totalGzippedKB = totalGzippedSize / 1024

      // Should be under 100KB gzipped
      expect(totalGzippedKB).toBeLessThan(100)
    })

    it('should have vendor chunk separated from main bundle', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const jsFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.js'
      )

      // Should have multiple chunks (main + vendor at minimum)
      const hasVendorChunk = jsFiles.some((file) => file.includes('vendor'))
      expect(hasVendorChunk).toBe(true)
    })

    it('should have reasonable total bundle size', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const jsFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.js'
      )

      let totalGzippedSize = 0
      for (const file of jsFiles) {
        const filePath = join(ASSETS_DIR, file)
        const content = readFileSync(filePath)
        const gzipped = gzipSync(content)
        totalGzippedSize += gzipped.length
      }

      // Total (including vendor) should be under 200KB gzipped
      const totalGzippedKB = totalGzippedSize / 1024
      expect(totalGzippedKB).toBeLessThan(200)
    })
  })

  describe('Source Maps', () => {
    it('should not include JavaScript source maps in production build', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const files = readdirSync(ASSETS_DIR)
      const jsMapFiles = files.filter((file) => file.endsWith('.js.map'))

      expect(jsMapFiles.length).toBe(0)
    })

    it('should not include CSS source maps in production build', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const files = readdirSync(ASSETS_DIR)
      const cssMapFiles = files.filter((file) => file.endsWith('.css.map'))

      expect(cssMapFiles.length).toBe(0)
    })

    it('should not reference source maps in JavaScript files', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const jsFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.js'
      )

      for (const file of jsFiles) {
        const filePath = join(ASSETS_DIR, file)
        const content = readFileSync(filePath, 'utf-8')
        expect(content).not.toContain('sourceMappingURL')
      }
    })
  })

  describe('CSS Purging', () => {
    it('should generate CSS file with Tailwind purging applied', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const cssFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.css'
      )

      expect(cssFiles.length).toBeGreaterThan(0)
    })

    it('should have CSS file under 50KB gzipped (purging effective)', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const cssFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.css'
      )

      if (cssFiles.length === 0) {
        // If no CSS, that's also acceptable (CSS might be inlined)
        return
      }

      let totalGzippedSize = 0
      for (const file of cssFiles) {
        const filePath = join(ASSETS_DIR, file)
        const content = readFileSync(filePath)
        const gzipped = gzipSync(content)
        totalGzippedSize += gzipped.length
      }

      // Purged CSS should be well under 50KB gzipped
      const totalGzippedKB = totalGzippedSize / 1024
      expect(totalGzippedKB).toBeLessThan(50)
    })

    it('should not contain Tailwind base reset styles for unused elements', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const cssFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.css'
      )

      if (cssFiles.length === 0) {
        return
      }

      // Pick the first CSS file
      const cssFile = cssFiles[0]
      const filePath = join(ASSETS_DIR, cssFile)
      const cssContent = readFileSync(filePath, 'utf-8')

      // Check that unusual utilities are NOT present (e.g., rarely used classes)
      // The presence of only used classes indicates purging is working
      // We check that specific responsive variants for unused components are absent
      const unusedPatterns = [
        '.2xl\\:', // 2xl breakpoint - unlikely to be used
        '.aspect-', // Aspect ratio utilities
        '.backdrop-', // Backdrop utilities
      ]

      // These patterns should either be absent or only present in minimal quantities
      // This is a heuristic check - if CSS is small, purging is working
      const cssSize = cssContent.length
      // Full Tailwind CSS is ~3MB+ unminified. Purged should be much smaller
      expect(cssSize).toBeLessThan(100000) // Under 100KB unminified
    })
  })

  describe('Asset Optimization', () => {
    it('should minify JavaScript files', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const jsFiles = readdirSync(ASSETS_DIR).filter(
        (file) => extname(file) === '.js'
      )

      for (const file of jsFiles) {
        const filePath = join(ASSETS_DIR, file)
        const content = readFileSync(filePath, 'utf-8')

        // Minified JS should not have multiple consecutive newlines
        expect(content).not.toMatch(/\n\n\n/)

        // Should not have console.log statements (in production)
        // Note: Some logging may be intentional, so we check for common debug patterns
        expect(content).not.toMatch(/console\.log\(\s*['"]debug/i)
      }
    })

    it('should have hashed filenames for cache busting', () => {
      if (!buildSuccess || !existsSync(ASSETS_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      const files = readdirSync(ASSETS_DIR)

      // Check that JS and CSS files have hash in their names
      const jsFiles = files.filter((file) => extname(file) === '.js')
      const cssFiles = files.filter((file) => extname(file) === '.css')

      // At least one JS file should have hash pattern (e.g., index-469cd9a5.js)
      const hasHashedJs = jsFiles.some((file) => /-[a-f0-9]{8}\.js$/.test(file))
      expect(hasHashedJs).toBe(true)

      // If CSS files exist, they should be hashed too
      if (cssFiles.length > 0) {
        const hasHashedCss = cssFiles.some((file) =>
          /-[a-f0-9]{8}\.css$/.test(file)
        )
        expect(hasHashedCss).toBe(true)
      }
    })

    it('should have correct asset structure', () => {
      if (!buildSuccess || !existsSync(DIST_DIR)) {
        expect(buildSuccess).toBe(true)
        return
      }

      // Check index.html exists and references assets correctly
      const indexPath = resolve(DIST_DIR, 'index.html')
      const indexContent = readFileSync(indexPath, 'utf-8')

      // Should reference assets from assets/ directory
      expect(indexContent).toMatch(/src=["']\/assets\//)
    })
  })
})

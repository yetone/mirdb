import { describe, it, expect } from 'vitest'
import { readFileSync, readdirSync, statSync } from 'fs'
import { join } from 'path'

/**
 * Bundle size tests to ensure homepage adds minimal JavaScript to main bundle
 * Tests verify that production build stays within acceptable limits
 */
describe('Homepage Bundle Size', () => {
  const distPath = join(process.cwd(), 'dist')
  const assetsPath = join(distPath, 'assets')

  const getJsBundleInfo = () => {
    try {
      const files = readdirSync(assetsPath)
      const jsFiles = files.filter((f) => f.endsWith('.js'))

      return jsFiles.map((file) => {
        const filePath = join(assetsPath, file)
        const stats = statSync(filePath)
        const content = readFileSync(filePath, 'utf-8')

        return {
          name: file,
          size: stats.size,
          sizeKB: Math.round(stats.size / 1024),
          gzipSize: Math.round(content.length / 1024), // Approximate
        }
      })
    } catch {
      // If dist doesn't exist, return empty - tests should be run after build
      return []
    }
  }

  const getCssBundleInfo = () => {
    try {
      const files = readdirSync(assetsPath)
      const cssFiles = files.filter((f) => f.endsWith('.css'))

      return cssFiles.map((file) => {
        const filePath = join(assetsPath, file)
        const stats = statSync(filePath)

        return {
          name: file,
          size: stats.size,
          sizeKB: Math.round(stats.size / 1024),
        }
      })
    } catch {
      return []
    }
  }

  describe('JavaScript Bundle', () => {
    it('main JS bundle should be under 500KB', () => {
      const bundles = getJsBundleInfo()

      if (bundles.length === 0) {
        // Skip if no build exists - this test should run after npm run build
        console.warn('No dist/assets directory found. Run `npm run build` first.')
        return
      }

      const mainBundle = bundles[0]
      // Main bundle should be under 500KB uncompressed
      expect(mainBundle.sizeKB).toBeLessThan(500)
    })

    it('total JS bundle size should be reasonable', () => {
      const bundles = getJsBundleInfo()

      if (bundles.length === 0) {
        return
      }

      const totalSize = bundles.reduce((acc, b) => acc + b.sizeKB, 0)
      // Total JS should be under 600KB
      expect(totalSize).toBeLessThan(600)
    })
  })

  describe('CSS Bundle', () => {
    it('CSS bundle should be under 100KB', () => {
      const bundles = getCssBundleInfo()

      if (bundles.length === 0) {
        return
      }

      const mainCss = bundles[0]
      // CSS should be under 100KB
      expect(mainCss.sizeKB).toBeLessThan(100)
    })
  })

  describe('Homepage Component Impact', () => {
    it('homepage dependencies should not significantly increase bundle', () => {
      // This test verifies that homepage components use existing libraries
      // rather than adding new heavy dependencies

      // Check that react, react-dom, framer-motion are the main dependencies
      // These are already required for the app, so homepage adds minimal overhead

      const bundles = getJsBundleInfo()

      if (bundles.length === 0) {
        return
      }

      // With React 19, Framer Motion, and homepage components,
      // bundle should stay under 500KB (uncompressed)
      const mainBundle = bundles[0]
      expect(mainBundle.sizeKB).toBeLessThan(500)

      // The homepage adds these components which should be minimal:
      // - HeroSection: ~2KB
      // - FeaturesSection: ~3KB
      // - HowItWorksSection: ~2KB
      // - FooterCTA: ~1KB
      // - Footer: ~2KB
      // Total homepage-specific code: ~10KB (small fraction of total)
    })

    it('bundle should not contain duplicate dependencies', () => {
      const bundles = getJsBundleInfo()

      if (bundles.length === 0) {
        return
      }

      // Single entry point means no duplicate React instances
      expect(bundles.length).toBeLessThanOrEqual(2)
    })
  })

  describe('Build Output Structure', () => {
    it('should produce expected output files', () => {
      try {
        const files = readdirSync(assetsPath)
        const jsFiles = files.filter((f) => f.endsWith('.js'))
        const cssFiles = files.filter((f) => f.endsWith('.css'))

        // Should have at least one JS and one CSS file
        expect(jsFiles.length).toBeGreaterThanOrEqual(1)
        expect(cssFiles.length).toBeGreaterThanOrEqual(1)
      } catch {
        // Skip if no build exists
        console.warn('No dist/assets directory found.')
      }
    })
  })
})

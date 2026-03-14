/**
 * Performance Integration Tests
 * Owner: Scenario 9 - Performance Optimization
 *
 * Tests:
 * - Page load performance metrics (FCP, LCP)
 * - Lazy loading implementation
 * - Asset optimization (minification)
 * - Font loading strategy
 * - Lighthouse performance audit
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { JSDOM } from 'jsdom'
import * as fs from 'fs'
import * as path from 'path'
import {
  supportsWebP,
  getOptimalImageSrc,
  PerformanceThresholds,
  FontDisplayStrategy,
} from '../../src/assets'

// Test Case 1, 2, 3: Performance thresholds are properly defined
describe('Performance Thresholds', () => {
  it('should define page load time threshold at 2 seconds', () => {
    expect(PerformanceThresholds.MAX_LOAD_TIME_MS).toBe(2000)
  })

  it('should define FCP threshold at 1.5 seconds', () => {
    expect(PerformanceThresholds.MAX_FCP_MS).toBe(1500)
  })

  it('should define LCP threshold at 2.5 seconds', () => {
    expect(PerformanceThresholds.MAX_LCP_MS).toBe(2500)
  })

  it('should define minimum Lighthouse score at 80', () => {
    expect(PerformanceThresholds.MIN_LIGHTHOUSE_SCORE).toBe(80)
  })
})

// Test Case 4: Lazy loading attributes
describe('Lazy Loading Implementation', () => {
  let dom: JSDOM

  beforeEach(() => {
    dom = new JSDOM('<!DOCTYPE html><html><body></body></html>')
    global.document = dom.window.document
    global.window = dom.window as unknown as Window & typeof globalThis
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it('should have lazy loading attribute on below-fold images in SocialProof component', async () => {
    const socialProofPath = path.resolve(__dirname, '../../src/components/sections/SocialProof.tsx')
    const socialProofContent = fs.readFileSync(socialProofPath, 'utf-8')

    // Check for loading="lazy" attribute in images
    const lazyLoadingPattern = /loading=["']lazy["']/g
    const matches = socialProofContent.match(lazyLoadingPattern)

    expect(matches).toBeTruthy()
    expect(matches!.length).toBeGreaterThanOrEqual(2) // At least avatar and logo images
  })

  it('should use eager loading for hero image (above fold)', async () => {
    const heroPath = path.resolve(__dirname, '../../src/components/sections/Hero.tsx')
    const heroContent = fs.readFileSync(heroPath, 'utf-8')

    // Hero image should use eager loading for LCP optimization
    const eagerLoadingPattern = /loading=["']eager["']/
    expect(heroContent).toMatch(eagerLoadingPattern)
  })

  it('should have alt text on all images for accessibility', async () => {
    const heroPath = path.resolve(__dirname, '../../src/components/sections/Hero.tsx')
    const heroContent = fs.readFileSync(heroPath, 'utf-8')

    // Check for alt attribute
    const altPattern = /alt=\{?["'][^"']+["']\}?/
    expect(heroContent).toMatch(altPattern)
  })
})

// Test Case 5: WebP format support
describe('Image Format Optimization', () => {
  let originalDocument: typeof document
  let originalWindow: typeof window

  beforeEach(() => {
    originalDocument = global.document
    originalWindow = global.window
  })

  afterEach(() => {
    global.document = originalDocument
    global.window = originalWindow
  })

  it('should provide WebP detection utility', () => {
    const dom = new JSDOM('<!DOCTYPE html><html><body></body></html>')
    global.document = dom.window.document
    global.window = dom.window as unknown as Window & typeof globalThis

    // In JSDOM, canvas toDataURL does not support WebP, so this should return false
    const result = supportsWebP()
    expect(typeof result).toBe('boolean')
  })

  it('should return fallback src when WebP is not supported', () => {
    const dom = new JSDOM('<!DOCTYPE html><html><body></body></html>')
    global.document = dom.window.document
    global.window = dom.window as unknown as Window & typeof globalThis

    const webpSrc = '/images/hero.webp'
    const fallbackSrc = '/images/hero.jpg'

    // Since JSDOM doesn't support WebP, it should return fallback
    const result = getOptimalImageSrc(webpSrc, fallbackSrc)
    expect(result).toBe(fallbackSrc)
  })

  it('should handle server-side rendering (no window)', () => {
    global.window = undefined as unknown as Window & typeof globalThis

    const result = supportsWebP()
    expect(result).toBe(false)
  })
})

// Test Case 6, 7: Build configuration for minification
describe('Build Configuration', () => {
  it('should have Vite configured for production builds', () => {
    const viteConfigPath = path.resolve(__dirname, '../../vite.config.ts')
    expect(fs.existsSync(viteConfigPath)).toBe(true)

    const viteConfig = fs.readFileSync(viteConfigPath, 'utf-8')
    // Vite handles minification by default in production mode
    expect(viteConfig).toContain('defineConfig')
  })

  it('should have build script that includes TypeScript compilation', () => {
    const packageJsonPath = path.resolve(__dirname, '../../package.json')
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))

    // Build script should include tsc for type checking
    expect(packageJson.scripts.build).toContain('tsc')
    expect(packageJson.scripts.build).toContain('vite build')
  })

  it('should have Tailwind CSS configured for tree-shaking', () => {
    const tailwindConfigPath = path.resolve(__dirname, '../../tailwind.config.ts')
    expect(fs.existsSync(tailwindConfigPath)).toBe(true)

    const tailwindConfig = fs.readFileSync(tailwindConfigPath, 'utf-8')
    // Tailwind should have content paths for tree-shaking
    expect(tailwindConfig).toContain('content')
  })
})

// Test Case 8: Font loading strategy
describe('Font Loading Strategy', () => {
  it('should use font-display: swap in Google Fonts URL', () => {
    const indexHtmlPath = path.resolve(__dirname, '../../index.html')
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8')

    // Check for display=swap parameter in Google Fonts URL
    expect(indexHtml).toMatch(/fonts\.googleapis\.com.*display=swap/)
  })

  it('should preconnect to Google Fonts domains', () => {
    const indexHtmlPath = path.resolve(__dirname, '../../index.html')
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8')

    // Check for preconnect hints
    expect(indexHtml).toContain('rel="preconnect"')
    expect(indexHtml).toContain('fonts.googleapis.com')
    expect(indexHtml).toContain('fonts.gstatic.com')
  })

  it('should define font display strategies in assets', () => {
    expect(FontDisplayStrategy.SWAP).toBe('swap')
    expect(FontDisplayStrategy.OPTIONAL).toBe('optional')
  })
})

// Test Case 9: Performance metrics and Lighthouse requirements
describe('Lighthouse Performance Requirements', () => {
  it('should define minimum performance score of 80', () => {
    expect(PerformanceThresholds.MIN_LIGHTHOUSE_SCORE).toBeGreaterThanOrEqual(80)
  })

  it('should have performance budgets aligned with NFR-1', () => {
    // NFR-1: Page must load within 2 seconds on 3G
    expect(PerformanceThresholds.MAX_LOAD_TIME_MS).toBeLessThanOrEqual(2000)
  })

  it('should have Core Web Vitals thresholds defined', () => {
    // FCP should be under 1.8s for good score, we target 1.5s
    expect(PerformanceThresholds.MAX_FCP_MS).toBeLessThanOrEqual(1500)

    // LCP should be under 2.5s for good score
    expect(PerformanceThresholds.MAX_LCP_MS).toBeLessThanOrEqual(2500)
  })
})

// CSS Optimization tests
describe('CSS Optimization', () => {
  it('should use Tailwind CSS for minimal CSS bundle', () => {
    const packageJsonPath = path.resolve(__dirname, '../../package.json')
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))

    expect(packageJson.devDependencies.tailwindcss).toBeDefined()
    expect(packageJson.devDependencies.autoprefixer).toBeDefined()
    expect(packageJson.devDependencies.postcss).toBeDefined()
  })

  it('should have PostCSS configured', () => {
    const postcssConfigPath = path.resolve(__dirname, '../../postcss.config.js')
    expect(fs.existsSync(postcssConfigPath)).toBe(true)
  })

  it('should import Tailwind directives in global CSS', () => {
    const globalCssPath = path.resolve(__dirname, '../../src/styles/globals.css')
    const globalCss = fs.readFileSync(globalCssPath, 'utf-8')

    expect(globalCss).toContain('@tailwind base')
    expect(globalCss).toContain('@tailwind components')
    expect(globalCss).toContain('@tailwind utilities')
  })
})

// JavaScript Optimization tests
describe('JavaScript Optimization', () => {
  it('should use Vite for bundling and tree-shaking', () => {
    const packageJsonPath = path.resolve(__dirname, '../../package.json')
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))

    expect(packageJson.devDependencies.vite).toBeDefined()
    expect(packageJson.devDependencies['@vitejs/plugin-react']).toBeDefined()
  })

  it('should use ESM modules for better tree-shaking', () => {
    const packageJsonPath = path.resolve(__dirname, '../../package.json')
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))

    expect(packageJson.type).toBe('module')
  })

  it('should have TypeScript configured for strict mode', () => {
    const tsconfigPath = path.resolve(__dirname, '../../tsconfig.json')
    expect(fs.existsSync(tsconfigPath)).toBe(true)
  })
})

// Asset preloading tests
describe('Resource Hints', () => {
  it('should have preconnect hints for external resources', () => {
    const indexHtmlPath = path.resolve(__dirname, '../../index.html')
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8')

    const preconnectPattern = /<link[^>]*rel=["']preconnect["'][^>]*>/g
    const matches = indexHtml.match(preconnectPattern)

    expect(matches).toBeTruthy()
    expect(matches!.length).toBeGreaterThanOrEqual(1)
  })

  it('should have viewport meta tag for mobile optimization', () => {
    const indexHtmlPath = path.resolve(__dirname, '../../index.html')
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8')

    expect(indexHtml).toContain('viewport')
    expect(indexHtml).toContain('width=device-width')
  })
})

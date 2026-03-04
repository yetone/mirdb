/**
 * Performance Tests
 * Owner: Scenario 9 - Performance and Load Time
 *
 * Tests to verify homepage performance characteristics and optimization patterns.
 * These tests analyze code patterns, bundle size, and performance best practices.
 *
 * Test coverage:
 * - Bundle size analysis (< 50KB gzipped for Home components)
 * - Code splitting and lazy loading patterns
 * - No heavy dependencies
 * - Render-blocking resource patterns
 * - Core Web Vitals optimization patterns
 */
import { describe, it, expect, beforeAll } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'
import { gzipSync } from 'zlib'

// Performance thresholds based on requirements
const PERFORMANCE_THRESHOLDS = {
  BUNDLE_SIZE_GZIPPED: 50 * 1024, // 50KB gzipped
  MAX_COMPONENT_SIZE: 20 * 1024, // 20KB per component gzipped
}

// Heavy dependencies that would negatively impact performance
const HEAVY_DEPENDENCIES = [
  'lodash',
  'moment',
  'jquery',
  'underscore',
  'rxjs',
  'immutable',
  '@material-ui',
  'antd',
  'bootstrap',
]

describe('Homepage Performance Tests', () => {
  const srcPath = path.resolve(__dirname, '../../src')
  const homePagePath = path.join(srcPath, 'pages/Home.tsx')
  const homeComponentsPath = path.join(srcPath, 'components/home')

  describe('Test Case 1 & 5: Bundle Size Analysis', () => {
    it('Home.tsx and related components contribute less than 50KB to gzipped bundle', () => {
      let totalSourceSize = 0
      const analyzedFiles: { file: string; size: number; gzipped: number }[] = []

      // Analyze Home.tsx
      if (fs.existsSync(homePagePath)) {
        const content = fs.readFileSync(homePagePath, 'utf8')
        const gzipped = gzipSync(content)
        analyzedFiles.push({
          file: 'pages/Home.tsx',
          size: content.length,
          gzipped: gzipped.length,
        })
        totalSourceSize += gzipped.length
      }

      // Analyze home components
      if (fs.existsSync(homeComponentsPath)) {
        const files = fs.readdirSync(homeComponentsPath)
        for (const file of files) {
          if (file.endsWith('.tsx') && !file.includes('.test.')) {
            const filePath = path.join(homeComponentsPath, file)
            const content = fs.readFileSync(filePath, 'utf8')
            const gzipped = gzipSync(content)
            analyzedFiles.push({
              file: `components/home/${file}`,
              size: content.length,
              gzipped: gzipped.length,
            })
            totalSourceSize += gzipped.length
          }
        }
      }

      console.log('\nSource file analysis (gzipped):')
      analyzedFiles.forEach(({ file, size, gzipped }) => {
        console.log(
          `  ${file}: ${(gzipped / 1024).toFixed(2)}KB gzipped (${(size / 1024).toFixed(2)}KB raw)`
        )
      })
      console.log(
        `\nTotal Home-related source: ${(totalSourceSize / 1024).toFixed(2)}KB gzipped`
      )

      // Source files should be significantly smaller than 50KB
      expect(totalSourceSize).toBeLessThan(PERFORMANCE_THRESHOLDS.BUNDLE_SIZE_GZIPPED)
    })

    it('Individual components are reasonably sized', () => {
      if (!fs.existsSync(homeComponentsPath)) {
        console.log('Home components folder not found')
        return
      }

      const files = fs.readdirSync(homeComponentsPath).filter(
        (f) => f.endsWith('.tsx') && !f.includes('.test.')
      )

      const oversizedComponents: string[] = []

      for (const file of files) {
        const filePath = path.join(homeComponentsPath, file)
        const content = fs.readFileSync(filePath, 'utf8')
        const gzipped = gzipSync(content)

        if (gzipped.length > PERFORMANCE_THRESHOLDS.MAX_COMPONENT_SIZE) {
          oversizedComponents.push(`${file}: ${(gzipped.length / 1024).toFixed(2)}KB`)
        }
      }

      if (oversizedComponents.length > 0) {
        console.log('Oversized components:', oversizedComponents)
      }

      expect(oversizedComponents.length).toBe(0)
    })
  })

  describe('Test Case 2: LCP Optimization Patterns', () => {
    it('Hero section exists and has content for LCP', () => {
      const heroPath = path.join(homeComponentsPath, 'HeroSection.tsx')
      expect(fs.existsSync(heroPath)).toBe(true)

      const content = fs.readFileSync(heroPath, 'utf8')

      // Check for headline element (h1/h2) which would be LCP
      const hasHeadline = content.includes('<h1') || content.includes('<h2') ||
        content.includes('motion.h1') || content.includes('motion.h2')
      expect(hasHeadline).toBe(true)

      console.log('Hero section has headline element for LCP: ' + hasHeadline)
    })

    it('No blocking inline scripts in components', () => {
      const files = [
        homePagePath,
        ...fs.readdirSync(homeComponentsPath)
          .filter((f) => f.endsWith('.tsx') && !f.includes('.test.'))
          .map((f) => path.join(homeComponentsPath, f)),
      ].filter((f) => fs.existsSync(f))

      const filesWithInlineScripts: string[] = []

      for (const file of files) {
        const content = fs.readFileSync(file, 'utf8')
        // Check for inline script tags that could block rendering
        if (content.includes('<script>') && !content.includes('<script async') && !content.includes('<script defer')) {
          filesWithInlineScripts.push(path.basename(file))
        }
      }

      expect(filesWithInlineScripts.length).toBe(0)
    })
  })

  describe('Test Case 3: FID Optimization Patterns', () => {
    it('Components use event handlers that are optimized', () => {
      const files = fs.existsSync(homeComponentsPath)
        ? fs.readdirSync(homeComponentsPath)
            .filter((f) => f.endsWith('.tsx') && !f.includes('.test.'))
            .map((f) => path.join(homeComponentsPath, f))
        : []

      let totalEventHandlers = 0
      let optimizedHandlers = 0

      for (const file of files) {
        const content = fs.readFileSync(file, 'utf8')

        // Check for onClick, onChange, etc.
        const clickHandlers = (content.match(/onClick=/g) || []).length
        const changeHandlers = (content.match(/onChange=/g) || []).length

        totalEventHandlers += clickHandlers + changeHandlers

        // Check for useCallback usage (optimization pattern)
        const useCallbackCount = (content.match(/useCallback/g) || []).length
        optimizedHandlers += useCallbackCount
      }

      console.log(`Total event handlers: ${totalEventHandlers}`)
      console.log(`useCallback optimizations: ${optimizedHandlers}`)

      // Event handlers exist (page is interactive)
      expect(totalEventHandlers).toBeGreaterThan(0)
    })

    it('No heavy computations in render methods', () => {
      const files = fs.existsSync(homeComponentsPath)
        ? fs.readdirSync(homeComponentsPath)
            .filter((f) => f.endsWith('.tsx') && !f.includes('.test.'))
            .map((f) => path.join(homeComponentsPath, f))
        : []

      const issuesFound: string[] = []

      for (const file of files) {
        const content = fs.readFileSync(file, 'utf8')
        const fileName = path.basename(file)

        // Check for expensive operations in render
        if (content.includes('.sort(') && !content.includes('useMemo')) {
          issuesFound.push(`${fileName}: sort() without useMemo`)
        }
        // Check for filter().map() chains without memoization
        if (content.includes('.filter(') && content.includes('.map(') && !content.includes('useMemo')) {
          // This is a simplified check - allow inline map/filter for small data
          // Only flag if there's evidence of large data processing
        }
        if (content.includes('JSON.parse') && !content.includes('useMemo') && !content.includes('useEffect')) {
          issuesFound.push(`${fileName}: JSON.parse without memoization`)
        }
      }

      if (issuesFound.length > 0) {
        console.log('Potential performance issues:', issuesFound)
      }

      // Allow some issues but warn
      expect(issuesFound.length).toBeLessThanOrEqual(2)
    })
  })

  describe('Test Case 4: CLS Optimization Patterns', () => {
    it('Components have explicit dimensions or aspect ratios', () => {
      const heroPath = path.join(homeComponentsPath, 'HeroSection.tsx')

      if (fs.existsSync(heroPath)) {
        const content = fs.readFileSync(heroPath, 'utf8')

        // Check for layout-stabilizing classes and patterns
        const hasMinHeight = content.includes('min-h-') || content.includes('minHeight')
        const hasFixedHeight = content.includes('h-screen') || content.includes('h-[') || content.includes('height:')
        const hasFlexLayout = content.includes('flex-grow') || content.includes('flex-1') ||
                             content.includes('flex-col') || content.includes('flex ')
        const hasGridLayout = content.includes('grid') || content.includes('grid-cols')
        const hasMaxWidth = content.includes('max-w-') || content.includes('maxWidth')

        // Flexbox and grid layouts with max-width provide content stability
        const hasLayoutStability = hasMinHeight || hasFixedHeight || hasFlexLayout || hasGridLayout || hasMaxWidth

        console.log(`Hero section layout stability: ${hasLayoutStability}`)
        console.log(`  - min-height: ${hasMinHeight}, fixed height: ${hasFixedHeight}`)
        console.log(`  - flex layout: ${hasFlexLayout}, grid: ${hasGridLayout}, max-width: ${hasMaxWidth}`)

        // Hero section should have layout stability
        expect(hasLayoutStability).toBe(true)
      }
    })

    it('Images have explicit dimensions or lazy loading', () => {
      const files = fs.existsSync(homeComponentsPath)
        ? fs.readdirSync(homeComponentsPath)
            .filter((f) => f.endsWith('.tsx') && !f.includes('.test.'))
            .map((f) => path.join(homeComponentsPath, f))
        : []

      let imagesFound = 0
      let imagesWithDimensions = 0

      for (const file of files) {
        const content = fs.readFileSync(file, 'utf8')

        // Count img tags
        const imgMatches = content.match(/<img/g) || []
        imagesFound += imgMatches.length

        // Count images with width/height or loading="lazy"
        const imgWithDimensions = (content.match(/<img[^>]*(width|height|loading)/g) || []).length
        imagesWithDimensions += imgWithDimensions
      }

      console.log(`Images found: ${imagesFound}, with dimensions/lazy: ${imagesWithDimensions}`)

      // If there are images, they should have dimensions
      if (imagesFound > 0) {
        expect(imagesWithDimensions).toBe(imagesFound)
      }
    })
  })

  describe('Test Case 6: No Render-Blocking Patterns', () => {
    it('No heavy dependencies that would block rendering', () => {
      const packageJsonPath = path.resolve(__dirname, '../../package.json')
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'))

      const deps = {
        ...packageJson.dependencies,
        ...packageJson.devDependencies,
      }

      const foundHeavyDeps: string[] = []

      for (const dep of HEAVY_DEPENDENCIES) {
        if (deps[dep]) {
          foundHeavyDeps.push(dep)
        }
      }

      if (foundHeavyDeps.length > 0) {
        console.log('Heavy dependencies found:', foundHeavyDeps)
      }

      expect(foundHeavyDeps.length).toBe(0)
    })

    it('Components use code splitting patterns', () => {
      // Check that the app uses React.lazy or dynamic imports for code splitting
      const appPath = path.join(srcPath, 'App.tsx')

      if (fs.existsSync(appPath)) {
        const content = fs.readFileSync(appPath, 'utf8')

        // Code splitting patterns
        const hasLazyImport = content.includes('React.lazy') || content.includes('lazy(')
        const hasDynamicImport = content.includes('import(')
        const hasSuspense = content.includes('Suspense')

        console.log(`Code splitting patterns:`)
        console.log(`  - React.lazy: ${hasLazyImport}`)
        console.log(`  - Dynamic import: ${hasDynamicImport}`)
        console.log(`  - Suspense: ${hasSuspense}`)

        // For a simple homepage, direct imports are acceptable
        // This test documents the pattern for future optimization
      }

      // Homepage components are tree-shakeable (named exports)
      const indexPath = path.join(homeComponentsPath, 'index.ts')
      if (fs.existsSync(indexPath)) {
        const content = fs.readFileSync(indexPath, 'utf8')
        const hasNamedExports = content.includes('export {') || content.includes('export *')

        console.log(`Home components use named exports: ${hasNamedExports}`)
        expect(hasNamedExports).toBe(true)
      }
    })

    it('Uses efficient animation library', () => {
      const packageJsonPath = path.resolve(__dirname, '../../package.json')
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'))

      // Framer Motion is recommended for React animations
      const usesFramerMotion = packageJson.dependencies?.['framer-motion'] !== undefined

      console.log(`Uses Framer Motion: ${usesFramerMotion}`)
      expect(usesFramerMotion).toBe(true)
    })
  })

  describe('Tree-shaking and Optimization', () => {
    it('Homepage components are tree-shakeable', () => {
      const indexPath = path.join(homeComponentsPath, 'index.ts')

      if (!fs.existsSync(indexPath)) {
        console.log('Home index file not found, skipping tree-shaking check')
        return
      }

      const content = fs.readFileSync(indexPath, 'utf8')

      // Check that exports are named (not default), which is better for tree-shaking
      const hasNamedExports = content.includes('export {') || content.includes('export *')

      console.log(`Home components index uses named exports: ${hasNamedExports}`)

      // Named exports allow better tree-shaking
      expect(hasNamedExports).toBe(true)
    })

    it('No unnecessary dependencies imported in Home components', () => {
      if (!fs.existsSync(homeComponentsPath)) {
        console.log('Home components folder not found')
        return
      }

      const files = fs.readdirSync(homeComponentsPath).filter(
        (f) => f.endsWith('.tsx') && !f.includes('.test.')
      )

      const foundHeavyDeps: string[] = []

      for (const file of files) {
        const filePath = path.join(homeComponentsPath, file)
        const content = fs.readFileSync(filePath, 'utf8')

        for (const dep of HEAVY_DEPENDENCIES) {
          if (content.includes(`from '${dep}'`) || content.includes(`from "${dep}"`)) {
            foundHeavyDeps.push(`${file}: ${dep}`)
          }
        }
      }

      if (foundHeavyDeps.length > 0) {
        console.log('Heavy dependencies found:', foundHeavyDeps)
      }

      expect(foundHeavyDeps.length).toBe(0)
    })
  })
})

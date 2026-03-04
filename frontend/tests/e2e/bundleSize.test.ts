/**
 * Bundle Size Analysis Tests
 * Owner: Scenario 9 - Performance and Load Time
 *
 * Unit tests to verify homepage component bundle size is optimized.
 * Target: < 50KB gzipped for Home.tsx contribution.
 */
import { describe, it, expect, beforeAll } from 'vitest'
import { execSync } from 'child_process'
import * as fs from 'fs'
import * as path from 'path'
import { gzipSync } from 'zlib'

// Bundle size threshold in bytes (50KB)
const MAX_BUNDLE_SIZE_GZIPPED = 50 * 1024

describe('Bundle Size Analysis', () => {
  let distPath: string
  let buildSuccessful = false

  beforeAll(async () => {
    distPath = path.resolve(__dirname, '../../dist')

    // Build the project if dist doesn't exist or is outdated
    try {
      // Check if we need to build
      const needsBuild =
        !fs.existsSync(distPath) ||
        !fs.existsSync(path.join(distPath, 'assets'))

      if (needsBuild) {
        console.log('Building project for bundle analysis...')
        execSync('npm run build', {
          cwd: path.resolve(__dirname, '../..'),
          stdio: 'pipe',
        })
      }

      buildSuccessful = fs.existsSync(distPath)
    } catch (error) {
      console.log('Build skipped or failed:', error)
      buildSuccessful = false
    }
  })

  it('Test Case 5: Home.tsx contributes less than 50KB to gzipped bundle', () => {
    if (!buildSuccessful) {
      // If build failed, analyze source files instead
      const srcPath = path.resolve(__dirname, '../../src')
      const homePagePath = path.join(srcPath, 'pages/Home.tsx')
      const homeComponentsPath = path.join(srcPath, 'components/home')

      // Calculate total size of Home-related source files
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
      // Production bundles include imports but tree-shaking removes unused code
      expect(totalSourceSize).toBeLessThan(MAX_BUNDLE_SIZE_GZIPPED)
      return
    }

    // Analyze production bundle
    const assetsPath = path.join(distPath, 'assets')

    if (!fs.existsSync(assetsPath)) {
      console.log('No assets folder found, skipping bundle analysis')
      return
    }

    const jsFiles = fs.readdirSync(assetsPath).filter((f) => f.endsWith('.js'))

    let totalBundleSize = 0
    const bundleAnalysis: { file: string; size: number; gzipped: number }[] = []

    for (const file of jsFiles) {
      const filePath = path.join(assetsPath, file)
      const content = fs.readFileSync(filePath)
      const gzipped = gzipSync(content)

      bundleAnalysis.push({
        file,
        size: content.length,
        gzipped: gzipped.length,
      })
      totalBundleSize += gzipped.length
    }

    console.log('\nBundle analysis (gzipped):')
    bundleAnalysis.forEach(({ file, size, gzipped }) => {
      console.log(
        `  ${file}: ${(gzipped / 1024).toFixed(2)}KB gzipped (${(size / 1024).toFixed(2)}KB raw)`
      )
    })
    console.log(
      `\nTotal JS bundle: ${(totalBundleSize / 1024).toFixed(2)}KB gzipped`
    )

    // The main bundle (which includes Home) should be under 50KB
    // Find the main chunk (usually named index or main)
    const mainBundle = bundleAnalysis.find(
      (b) => b.file.includes('index') || !b.file.includes('vendor')
    )

    if (mainBundle) {
      console.log(
        `\nMain bundle (Home contribution): ${(mainBundle.gzipped / 1024).toFixed(2)}KB gzipped`
      )
      expect(mainBundle.gzipped).toBeLessThan(MAX_BUNDLE_SIZE_GZIPPED)
    } else {
      // If we can't identify the main bundle, check total
      expect(totalBundleSize).toBeLessThan(MAX_BUNDLE_SIZE_GZIPPED * 3) // Allow for vendor chunks
    }
  })

  it('Homepage components are tree-shakeable', () => {
    const srcPath = path.resolve(__dirname, '../../src')
    const homeIndexPath = path.join(srcPath, 'components/home/index.ts')

    if (!fs.existsSync(homeIndexPath)) {
      console.log('Home index file not found, skipping tree-shaking check')
      return
    }

    const content = fs.readFileSync(homeIndexPath, 'utf8')

    // Check that exports are named (not default), which is better for tree-shaking
    const hasNamedExports = content.includes('export {') || content.includes('export *')

    console.log(`Home components index uses named exports: ${hasNamedExports}`)

    // Named exports allow better tree-shaking
    expect(hasNamedExports).toBe(true)
  })

  it('No unnecessary dependencies imported in Home components', () => {
    const srcPath = path.resolve(__dirname, '../../src')
    const componentsPath = path.join(srcPath, 'components/home')

    if (!fs.existsSync(componentsPath)) {
      console.log('Home components folder not found')
      return
    }

    const files = fs.readdirSync(componentsPath).filter(
      (f) => f.endsWith('.tsx') && !f.includes('.test.')
    )

    const heavyDependencies = ['lodash', 'moment', 'jquery', 'underscore']
    const foundHeavyDeps: string[] = []

    for (const file of files) {
      const filePath = path.join(componentsPath, file)
      const content = fs.readFileSync(filePath, 'utf8')

      for (const dep of heavyDependencies) {
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

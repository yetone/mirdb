/**
 * Integration tests for performance budget validation.
 * Owner: Scenario 9 - Performance and Load Time
 *
 * Test coverage:
 * - JavaScript bundle size is under 200KB gzipped
 * - CSS bundle size is under 50KB gzipped
 * - Total image weight is under 300KB
 */

import { describe, test, expect, beforeAll } from 'vitest'
import { execSync } from 'child_process'
import * as fs from 'fs'
import * as path from 'path'
import { gzipSync } from 'zlib'

const DIST_DIR = path.resolve(__dirname, '../../dist')
const ASSETS_DIR = path.join(DIST_DIR, 'assets')

// Performance budget limits (in bytes)
const JS_BUDGET_GZIPPED = 200 * 1024 // 200KB
const CSS_BUDGET_GZIPPED = 50 * 1024 // 50KB
const IMAGE_BUDGET = 300 * 1024 // 300KB

/**
 * Helper function to calculate gzipped size of a file
 */
function getGzippedSize(filePath: string): number {
  const content = fs.readFileSync(filePath)
  const gzipped = gzipSync(content)
  return gzipped.length
}

/**
 * Helper function to get all files matching a pattern in a directory
 */
function getFilesWithExtension(dir: string, ext: string): string[] {
  if (!fs.existsSync(dir)) {
    return []
  }
  return fs.readdirSync(dir)
    .filter(file => file.endsWith(ext))
    .map(file => path.join(dir, file))
}

/**
 * Helper function to sum file sizes
 */
function getTotalSize(files: string[], useGzip = false): number {
  return files.reduce((total, file) => {
    if (useGzip) {
      return total + getGzippedSize(file)
    }
    return total + fs.statSync(file).size
  }, 0)
}

describe('Performance Budget', () => {
  beforeAll(() => {
    // Ensure production build exists
    if (!fs.existsSync(DIST_DIR)) {
      console.log('Building production bundle...')
      execSync('npm run build', {
        cwd: path.resolve(__dirname, '../..'),
        stdio: 'inherit'
      })
    }
  })

  test('JavaScript bundle size is under 200KB gzipped', () => {
    const jsFiles = getFilesWithExtension(ASSETS_DIR, '.js')
    expect(jsFiles.length).toBeGreaterThan(0)

    const totalGzippedSize = getTotalSize(jsFiles, true)
    const sizeKB = (totalGzippedSize / 1024).toFixed(2)

    console.log(`Total JS bundle size (gzipped): ${sizeKB}KB`)
    console.log(`Budget: ${JS_BUDGET_GZIPPED / 1024}KB`)

    expect(totalGzippedSize).toBeLessThan(JS_BUDGET_GZIPPED)
  })

  test('CSS bundle size is under 50KB gzipped', () => {
    const cssFiles = getFilesWithExtension(ASSETS_DIR, '.css')
    expect(cssFiles.length).toBeGreaterThan(0)

    const totalGzippedSize = getTotalSize(cssFiles, true)
    const sizeKB = (totalGzippedSize / 1024).toFixed(2)

    console.log(`Total CSS bundle size (gzipped): ${sizeKB}KB`)
    console.log(`Budget: ${CSS_BUDGET_GZIPPED / 1024}KB`)

    expect(totalGzippedSize).toBeLessThan(CSS_BUDGET_GZIPPED)
  })

  test('Total image weight is under 300KB', () => {
    // Check images in the dist directory
    const distImages = getFilesWithExtension(ASSETS_DIR, '.png')
      .concat(getFilesWithExtension(ASSETS_DIR, '.jpg'))
      .concat(getFilesWithExtension(ASSETS_DIR, '.jpeg'))
      .concat(getFilesWithExtension(ASSETS_DIR, '.webp'))
      .concat(getFilesWithExtension(ASSETS_DIR, '.gif'))
      .concat(getFilesWithExtension(ASSETS_DIR, '.svg'))

    // Also check for images in public directory if it exists
    const publicDir = path.resolve(__dirname, '../../public')
    let publicImages: string[] = []
    if (fs.existsSync(publicDir)) {
      publicImages = getFilesWithExtension(publicDir, '.png')
        .concat(getFilesWithExtension(publicDir, '.jpg'))
        .concat(getFilesWithExtension(publicDir, '.jpeg'))
        .concat(getFilesWithExtension(publicDir, '.webp'))
        .concat(getFilesWithExtension(publicDir, '.gif'))
        .concat(getFilesWithExtension(publicDir, '.svg'))
    }

    const allImages = [...distImages, ...publicImages]
    const totalImageSize = getTotalSize(allImages, false)
    const sizeKB = (totalImageSize / 1024).toFixed(2)

    console.log(`Total image weight: ${sizeKB}KB (${allImages.length} images)`)
    console.log(`Budget: ${IMAGE_BUDGET / 1024}KB`)

    // If no images exist, the test passes (0KB < 300KB)
    expect(totalImageSize).toBeLessThan(IMAGE_BUDGET)
  })
})

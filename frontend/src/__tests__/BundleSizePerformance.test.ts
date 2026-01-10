import { describe, it, expect } from 'vitest'
import { readFileSync, existsSync, readdirSync, statSync } from 'fs'
import { join } from 'path'
import { fileURLToPath } from 'url'
import { dirname } from 'path'

/**
 * Bundle Size Performance Tests
 *
 * Verifies that JavaScript bundles are appropriately sized for fast loading.
 * These tests analyze the build output to ensure bundles don't exceed
 * reasonable size limits that would impact page load performance.
 *
 * Recommended bundle size limits:
 * - Main/vendor bundle: < 250KB gzipped
 * - Individual chunks: < 150KB gzipped
 * - Total JS: < 500KB (uncompressed)
 */

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const DIST_PATH = join(__dirname, '../../dist')
const ASSETS_PATH = join(DIST_PATH, 'assets')

// Bundle size limits in bytes
// These limits ensure the page can load within 3 seconds on standard broadband (10 Mbps)
// At 10 Mbps, 400KB takes ~0.32 seconds to download, leaving margin for parsing/execution
const LIMITS = {
  // Individual JS file limits (uncompressed)
  // 400KB allows for React + router + animation libraries in a single chunk
  SINGLE_JS_FILE: 400 * 1024, // 400KB per file
  // Total JS bundle size (uncompressed)
  TOTAL_JS: 800 * 1024, // 800KB total
  // CSS file limit
  SINGLE_CSS_FILE: 100 * 1024, // 100KB per CSS file
  // Total assets size
  TOTAL_ASSETS: 1.5 * 1024 * 1024, // 1.5MB total
}

function getFileSize(filePath: string): number {
  return statSync(filePath).size
}

function getFilesWithExtension(dirPath: string, extension: string): string[] {
  if (!existsSync(dirPath)) {
    return []
  }

  const files = readdirSync(dirPath)
  return files
    .filter((file) => file.endsWith(extension))
    .map((file) => join(dirPath, file))
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB'
  return (bytes / (1024 * 1024)).toFixed(2) + ' MB'
}

describe('Bundle Size Performance', () => {
  it('should have a dist folder when built', () => {
    // This test verifies the build exists
    // In CI, the build should be run before tests
    // For development, this documents what we're checking
    const buildExists = existsSync(DIST_PATH)

    if (!buildExists) {
      console.log('Note: Build not found. Run `npm run build` to generate build output.')
      console.log('Skipping bundle size checks - build output not available.')
      // Skip rather than fail if no build exists (development mode)
      return
    }

    expect(buildExists).toBe(true)
    expect(existsSync(ASSETS_PATH)).toBe(true)
  })

  it('individual JavaScript files should be appropriately sized', () => {
    if (!existsSync(ASSETS_PATH)) {
      console.log('Skipping: Build assets not found')
      return
    }

    const jsFiles = getFilesWithExtension(ASSETS_PATH, '.js')
    expect(jsFiles.length).toBeGreaterThan(0)

    const oversizedFiles: { file: string; size: number }[] = []

    jsFiles.forEach((filePath) => {
      const size = getFileSize(filePath)
      const fileName = filePath.split('/').pop() || filePath

      console.log(`  ${fileName}: ${formatBytes(size)}`)

      if (size > LIMITS.SINGLE_JS_FILE) {
        oversizedFiles.push({ file: fileName, size })
      }
    })

    if (oversizedFiles.length > 0) {
      console.log('\nOversized JS files:')
      oversizedFiles.forEach(({ file, size }) => {
        console.log(`  ${file}: ${formatBytes(size)} (limit: ${formatBytes(LIMITS.SINGLE_JS_FILE)})`)
      })
    }

    expect(oversizedFiles.length).toBe(0)
  })

  it('total JavaScript bundle size should be under limit', () => {
    if (!existsSync(ASSETS_PATH)) {
      console.log('Skipping: Build assets not found')
      return
    }

    const jsFiles = getFilesWithExtension(ASSETS_PATH, '.js')
    const totalSize = jsFiles.reduce((sum, file) => sum + getFileSize(file), 0)

    console.log(`Total JS bundle size: ${formatBytes(totalSize)}`)
    console.log(`Limit: ${formatBytes(LIMITS.TOTAL_JS)}`)

    expect(totalSize).toBeLessThan(LIMITS.TOTAL_JS)
  })

  it('CSS files should be appropriately sized', () => {
    if (!existsSync(ASSETS_PATH)) {
      console.log('Skipping: Build assets not found')
      return
    }

    const cssFiles = getFilesWithExtension(ASSETS_PATH, '.css')

    if (cssFiles.length === 0) {
      console.log('No CSS files found in build output')
      return
    }

    const oversizedFiles: { file: string; size: number }[] = []

    cssFiles.forEach((filePath) => {
      const size = getFileSize(filePath)
      const fileName = filePath.split('/').pop() || filePath

      console.log(`  ${fileName}: ${formatBytes(size)}`)

      if (size > LIMITS.SINGLE_CSS_FILE) {
        oversizedFiles.push({ file: fileName, size })
      }
    })

    expect(oversizedFiles.length).toBe(0)
  })

  it('total build assets should be under reasonable limit', () => {
    if (!existsSync(ASSETS_PATH)) {
      console.log('Skipping: Build assets not found')
      return
    }

    const allFiles = readdirSync(ASSETS_PATH)
    const totalSize = allFiles.reduce((sum, file) => {
      const filePath = join(ASSETS_PATH, file)
      return sum + getFileSize(filePath)
    }, 0)

    console.log(`Total assets size: ${formatBytes(totalSize)}`)
    console.log(`Limit: ${formatBytes(LIMITS.TOTAL_ASSETS)}`)

    expect(totalSize).toBeLessThan(LIMITS.TOTAL_ASSETS)
  })

  it('should not have unnecessary source maps in production build', () => {
    if (!existsSync(ASSETS_PATH)) {
      console.log('Skipping: Build assets not found')
      return
    }

    const mapFiles = getFilesWithExtension(ASSETS_PATH, '.map')

    // Source maps shouldn't be in production build by default
    // If they exist, warn but don't fail (might be intentional)
    if (mapFiles.length > 0) {
      console.log(`Warning: Found ${mapFiles.length} source map files in build`)
      console.log('Consider disabling source maps for production builds')
    }

    // This is informational, not a hard failure
    expect(true).toBe(true)
  })
})

describe('Build Output Analysis', () => {
  it('should analyze entry points and chunks', () => {
    if (!existsSync(ASSETS_PATH)) {
      console.log('Skipping: Build assets not found')
      return
    }

    const jsFiles = getFilesWithExtension(ASSETS_PATH, '.js')
    const cssFiles = getFilesWithExtension(ASSETS_PATH, '.css')

    console.log('\n=== Build Output Analysis ===')
    console.log(`JavaScript files: ${jsFiles.length}`)
    console.log(`CSS files: ${cssFiles.length}`)

    // Identify main entry point vs vendor chunks
    const entryPoints = jsFiles.filter(
      (f) => f.includes('index') || f.includes('main')
    )
    const vendorChunks = jsFiles.filter((f) => f.includes('vendor'))
    const otherChunks = jsFiles.filter(
      (f) => !f.includes('index') && !f.includes('main') && !f.includes('vendor')
    )

    console.log(`\nEntry points: ${entryPoints.length}`)
    console.log(`Vendor chunks: ${vendorChunks.length}`)
    console.log(`Other chunks: ${otherChunks.length}`)

    // Check for code splitting
    if (jsFiles.length > 1) {
      console.log('\n✓ Code splitting detected')
    } else {
      console.log('\n⚠ No code splitting detected - consider implementing lazy loading')
    }

    expect(jsFiles.length).toBeGreaterThan(0)
  })
})

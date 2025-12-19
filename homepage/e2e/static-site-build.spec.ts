import { test, expect } from '@playwright/test'
import { execSync } from 'child_process'
import * as fs from 'fs'
import * as path from 'path'
import { fileURLToPath } from 'url'

/**
 * Static Site Generation Build Tests
 * Verifies NFR-5: Static site generation for easy hosting (GitHub Pages, Netlify, etc.)
 *
 * Test Case 1: Build completes successfully without errors
 * Test Case 2: Output contains index.html and necessary CSS/assets
 * Test Case 3: Site is fully functional when served statically
 */

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const DIST_DIR = path.resolve(__dirname, '../dist')

test.describe('Static Site Generation Build', () => {
  /**
   * Test Case 1: Build completes successfully without errors
   * Run static site build command and verify it succeeds
   */
  test('TC1: build completes successfully without errors', async () => {
    // Execute the build command
    let buildOutput: string
    let buildExitCode: number = 0

    try {
      buildOutput = execSync('npm run build', {
        cwd: path.resolve(__dirname, '..'),
        encoding: 'utf-8',
        stdio: ['pipe', 'pipe', 'pipe'],
      })
    } catch (error: any) {
      buildExitCode = error.status || 1
      buildOutput = error.stdout || error.message
    }

    // Verify build succeeded (exit code 0)
    expect(buildExitCode).toBe(0)

    // Verify build output indicates success
    expect(buildOutput).toContain('built in')

    // Verify no error messages in output
    expect(buildOutput.toLowerCase()).not.toContain('error')
    expect(buildOutput.toLowerCase()).not.toContain('failed')

    // Verify dist directory was created
    const distExists = fs.existsSync(DIST_DIR)
    expect(distExists).toBe(true)

    console.log('Build completed successfully')
    console.log(buildOutput)
  })

  /**
   * Test Case 2: Output contains index.html and necessary CSS/assets
   * Check that build produces HTML, CSS, and asset files
   */
  test('TC2: output contains index.html and necessary CSS/assets', async () => {
    // Ensure build has been run
    if (!fs.existsSync(DIST_DIR)) {
      execSync('npm run build', {
        cwd: path.resolve(__dirname, '..'),
        encoding: 'utf-8',
      })
    }

    // Check index.html exists
    const indexPath = path.join(DIST_DIR, 'index.html')
    expect(fs.existsSync(indexPath)).toBe(true)

    // Read and verify index.html content
    const indexContent = fs.readFileSync(indexPath, 'utf-8')

    // Verify HTML structure
    expect(indexContent).toContain('<!DOCTYPE html>')
    expect(indexContent).toContain('<html')
    expect(indexContent).toContain('</html>')
    expect(indexContent).toContain('<head>')
    expect(indexContent).toContain('<body>')

    // Verify it references CSS
    expect(indexContent).toMatch(/<link[^>]*\.css[^>]*>/i)

    // Verify it references JavaScript
    expect(indexContent).toMatch(/<script[^>]*\.js[^>]*>/i)

    // Check assets directory exists
    const assetsDir = path.join(DIST_DIR, 'assets')
    expect(fs.existsSync(assetsDir)).toBe(true)

    // Get list of asset files
    const assetFiles = fs.readdirSync(assetsDir)

    // Verify CSS file exists
    const cssFiles = assetFiles.filter(f => f.endsWith('.css'))
    expect(cssFiles.length).toBeGreaterThanOrEqual(1)

    // Verify JS file exists
    const jsFiles = assetFiles.filter(f => f.endsWith('.js'))
    expect(jsFiles.length).toBeGreaterThanOrEqual(1)

    console.log('Build output verification:')
    console.log('  - index.html:', fs.existsSync(indexPath) ? 'present' : 'missing')
    console.log('  - CSS files:', cssFiles.join(', '))
    console.log('  - JS files:', jsFiles.join(', '))
  })

  /**
   * Test Case 3: Site is fully functional when served statically
   * Serve build output with static file server and verify functionality
   */
  test('TC3: site is fully functional when served statically', async ({ page }) => {
    // The Playwright config already uses the preview server (vite preview)
    // which serves the static dist folder

    // Navigate to the statically served site
    await page.goto('/')

    // Verify the page loads successfully (no 404)
    expect(page.url()).not.toContain('404')

    // Verify main content is rendered
    const heading = page.locator('h1')
    await expect(heading).toBeVisible()
    await expect(heading).toHaveText('MirDB')

    // Verify hero section is visible
    const hero = page.locator('.hero')
    await expect(hero).toBeVisible()

    // Verify features section loads (using data-testid or section id)
    const features = page.locator('[data-testid="features-section"], #features')
    await expect(features.first()).toBeVisible()

    // Verify quick start section loads
    const quickStart = page.locator('[data-testid="quick-start-section"], #quick-start')
    await expect(quickStart.first()).toBeVisible()

    // Verify commands section loads
    const commands = page.locator('[data-testid="commands-section"], #commands')
    await expect(commands.first()).toBeVisible()

    // Verify configuration section loads
    const config = page.locator('[data-testid="configuration-section"], #configuration')
    await expect(config.first()).toBeVisible()

    // Verify footer loads (with GitHub link)
    const footer = page.locator('footer')
    await expect(footer).toBeVisible()

    // Verify CSS is applied (check that elements have proper styling)
    const heroStyles = await hero.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        padding: styles.padding,
      }
    })
    expect(heroStyles.display).not.toBe('none')

    // Verify JavaScript functionality works (code blocks are present)
    const codeBlocks = page.locator('[data-testid="code-block"], pre, .code-block')
    const codeBlockCount = await codeBlocks.count()
    expect(codeBlockCount).toBeGreaterThan(0)

    // Verify navigation or header links work (GitHub link in header)
    const headerLinks = page.locator('header a, nav a, .header-nav a')
    const headerLinkCount = await headerLinks.count()
    expect(headerLinkCount).toBeGreaterThanOrEqual(1)

    console.log('Static site functionality verified:')
    console.log('  - Main heading visible: true')
    console.log('  - All sections loaded: true')
    console.log('  - CSS styling applied: true')
    console.log('  - JavaScript functional: true')
  })

  /**
   * Additional verification: No server-side requirements
   * Confirm build output can be served by any static file server
   */
  test('TC3-additional: build output has no server-side dependencies', async () => {
    // Ensure build has been run
    if (!fs.existsSync(DIST_DIR)) {
      execSync('npm run build', {
        cwd: path.resolve(__dirname, '..'),
        encoding: 'utf-8',
      })
    }

    // List all files in dist directory recursively
    function getAllFiles(dir: string): string[] {
      const files: string[] = []
      const entries = fs.readdirSync(dir, { withFileTypes: true })
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name)
        if (entry.isDirectory()) {
          files.push(...getAllFiles(fullPath))
        } else {
          files.push(fullPath)
        }
      }
      return files
    }

    const allFiles = getAllFiles(DIST_DIR)
    const relativeFiles = allFiles.map(f => f.replace(DIST_DIR, ''))

    console.log('All build output files:', relativeFiles)

    // Verify only static file types are present
    const allowedExtensions = ['.html', '.css', '.js', '.json', '.ico', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.woff', '.woff2', '.ttf', '.eot', '.map']

    for (const file of allFiles) {
      const ext = path.extname(file).toLowerCase()
      if (ext) {
        expect(allowedExtensions).toContain(ext)
      }
    }

    // Verify no server-side files are present
    const serverSidePatterns = [
      '.php', '.py', '.rb', '.node', '.go', '.java', '.class',
      'package.json', 'node_modules', '.env', 'server.', 'api.'
    ]

    for (const file of relativeFiles) {
      for (const pattern of serverSidePatterns) {
        expect(file.toLowerCase()).not.toContain(pattern.toLowerCase())
      }
    }

    // Verify index.html doesn't require any server-side processing
    const indexContent = fs.readFileSync(path.join(DIST_DIR, 'index.html'), 'utf-8')

    // No PHP tags
    expect(indexContent).not.toContain('<?php')

    // No server-side includes
    expect(indexContent).not.toContain('<!--#include')

    // No templating syntax that requires server processing
    expect(indexContent).not.toMatch(/<%[^%].*%>/) // EJS
    expect(indexContent).not.toMatch(/\{\{[^}]*\}\}.*\{\{/) // Unless it's in noscript fallback

    console.log('Build output verified as fully static with no server-side dependencies')
  })
})

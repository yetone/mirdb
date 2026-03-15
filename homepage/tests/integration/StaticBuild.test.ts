/**
 * Static Deployment Build Integration Tests.
 * Owner: Scenario 13 - Static Deployment Build
 *
 * Tests:
 * - Build completes without errors
 * - Build output contains index.html
 * - Build output contains CSS bundle files
 * - Build output contains JS bundle files
 * - No runtime server dependencies in production
 *
 * Requirements:
 * - NFR-4: Homepage must be deployable as static files (no runtime backend required)
 */

import { describe, test, expect, beforeAll } from 'vitest'
import { execSync } from 'child_process'
import * as fs from 'fs'
import * as path from 'path'

const projectRoot = path.resolve(__dirname, '../..')
const distPath = path.join(projectRoot, 'dist')

describe('Static Deployment Build', () => {
  // TC1: Build completes without errors
  describe('Build Process', () => {
    let buildResult: { success: boolean; output: string; error: string }

    beforeAll(() => {
      // Clean dist directory before build
      if (fs.existsSync(distPath)) {
        fs.rmSync(distPath, { recursive: true, force: true })
      }

      try {
        const output = execSync('npm run build', {
          cwd: projectRoot,
          encoding: 'utf-8',
          stdio: ['pipe', 'pipe', 'pipe']
        })
        buildResult = { success: true, output, error: '' }
      } catch (error: unknown) {
        const execError = error as { stdout?: string; stderr?: string; message: string }
        buildResult = {
          success: false,
          output: execError.stdout || '',
          error: execError.stderr || execError.message
        }
      }
    })

    test('npm run build completes without errors', () => {
      expect(buildResult.success).toBe(true)
      if (!buildResult.success) {
        console.error('Build error:', buildResult.error)
      }
    })

    test('build creates dist directory', () => {
      expect(fs.existsSync(distPath)).toBe(true)
    })
  })

  // TC2: Build output contains index.html
  describe('HTML Output', () => {
    test('dist directory contains index.html file', () => {
      const indexPath = path.join(distPath, 'index.html')
      expect(fs.existsSync(indexPath)).toBe(true)
    })

    test('index.html is valid HTML document', () => {
      const indexPath = path.join(distPath, 'index.html')
      const content = fs.readFileSync(indexPath, 'utf-8')

      // Check for essential HTML structure
      expect(content).toContain('<!DOCTYPE html>')
      expect(content).toContain('<html')
      expect(content).toContain('<head>')
      expect(content).toContain('<body>')
      expect(content).toContain('</html>')
    })

    test('index.html references built assets', () => {
      const indexPath = path.join(distPath, 'index.html')
      const content = fs.readFileSync(indexPath, 'utf-8')

      // Should reference JS and CSS assets
      expect(content).toMatch(/src="[^"]*\.js"/)
      expect(content).toMatch(/href="[^"]*\.css"|<style/)
    })
  })

  // TC3: Build output contains CSS bundle files
  describe('CSS Bundle Output', () => {
    test('dist/assets directory contains CSS bundle file(s)', () => {
      const assetsPath = path.join(distPath, 'assets')
      expect(fs.existsSync(assetsPath)).toBe(true)

      const files = fs.readdirSync(assetsPath)
      const cssFiles = files.filter(f => f.endsWith('.css'))

      expect(cssFiles.length).toBeGreaterThan(0)
      console.log(`Found CSS files: ${cssFiles.join(', ')}`)
    })

    test('CSS bundles have content', () => {
      const assetsPath = path.join(distPath, 'assets')
      const files = fs.readdirSync(assetsPath)
      const cssFiles = files.filter(f => f.endsWith('.css'))

      for (const cssFile of cssFiles) {
        const filePath = path.join(assetsPath, cssFile)
        const stats = fs.statSync(filePath)
        expect(stats.size).toBeGreaterThan(0)
      }
    })
  })

  // TC4: Build output contains JS bundle files
  describe('JS Bundle Output', () => {
    test('dist/assets directory contains JS bundle file(s)', () => {
      const assetsPath = path.join(distPath, 'assets')
      expect(fs.existsSync(assetsPath)).toBe(true)

      const files = fs.readdirSync(assetsPath)
      const jsFiles = files.filter(f => f.endsWith('.js'))

      expect(jsFiles.length).toBeGreaterThan(0)
      console.log(`Found JS files: ${jsFiles.join(', ')}`)
    })

    test('JS bundles have content', () => {
      const assetsPath = path.join(distPath, 'assets')
      const files = fs.readdirSync(assetsPath)
      const jsFiles = files.filter(f => f.endsWith('.js'))

      for (const jsFile of jsFiles) {
        const filePath = path.join(assetsPath, jsFile)
        const stats = fs.statSync(filePath)
        expect(stats.size).toBeGreaterThan(0)
      }
    })

    test('JS bundles contain React application code', () => {
      const assetsPath = path.join(distPath, 'assets')
      const files = fs.readdirSync(assetsPath)
      const jsFiles = files.filter(f => f.endsWith('.js'))

      // At least one JS file should contain React-related code
      let foundReactCode = false
      for (const jsFile of jsFiles) {
        const filePath = path.join(assetsPath, jsFile)
        const content = fs.readFileSync(filePath, 'utf-8')
        if (content.includes('react') || content.includes('jsx') || content.includes('createElement')) {
          foundReactCode = true
          break
        }
      }

      expect(foundReactCode).toBe(true)
    })
  })

  // TC6: No runtime server dependencies in production
  describe('No Runtime Server Dependencies', () => {
    test('package.json has no server-side runtime dependencies', () => {
      const packageJsonPath = path.join(projectRoot, 'package.json')
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))
      const dependencies = packageJson.dependencies || {}

      // List of common server-side dependencies that should NOT be present
      const serverDependencies = [
        'express',
        'koa',
        'fastify',
        'hapi',
        'next',
        'nuxt',
        'nest',
        'socket.io',
        'ws',
        'http-server'
      ]

      for (const serverDep of serverDependencies) {
        expect(dependencies[serverDep]).toBeUndefined()
      }
    })

    test('dist directory contains only static files', () => {
      const staticExtensions = ['.html', '.css', '.js', '.map', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.woff', '.woff2', '.ttf', '.eot']

      function checkDirectory(dir: string): void {
        const entries = fs.readdirSync(dir, { withFileTypes: true })

        for (const entry of entries) {
          const fullPath = path.join(dir, entry.name)

          if (entry.isDirectory()) {
            checkDirectory(fullPath)
          } else if (entry.isFile()) {
            const ext = path.extname(entry.name).toLowerCase()
            // Allow files without extension in root (like CNAME for GitHub Pages)
            if (ext) {
              expect(staticExtensions).toContain(ext)
            }
          }
        }
      }

      checkDirectory(distPath)
    })

    test('no Node.js-specific files in dist', () => {
      // Check that no server-specific files are in the dist
      const forbiddenFiles = [
        'server.js',
        'server.ts',
        'app.js',
        'app.ts',
        'index.js', // Not index.html
        'package.json',
        'node_modules'
      ]

      const distFiles = fs.readdirSync(distPath)

      for (const forbidden of forbiddenFiles) {
        expect(distFiles).not.toContain(forbidden)
      }
    })
  })

  // Vite configuration check
  describe('Vite Configuration', () => {
    test('vite.config.ts configures static output directory', () => {
      const viteConfigPath = path.join(projectRoot, 'vite.config.ts')
      const content = fs.readFileSync(viteConfigPath, 'utf-8')

      // Should have build configuration with outDir
      expect(content).toContain('build')
      expect(content).toContain('outDir')
    })
  })
})

/**
 * Lighthouse Performance Tests
 *
 * This test script verifies that the homepage meets the performance
 * requirements specified in the PRD:
 * - Overall Lighthouse performance score of 80+
 * - LCP (Largest Contentful Paint) under 2.5 seconds
 * - CLS (Cumulative Layout Shift) under 0.1
 * - TBT/FID (Total Blocking Time as proxy for First Input Delay) under 100ms
 *
 * Usage: node lighthouse.test.js
 *
 * Prerequisites:
 * - Production build must exist (npm run build)
 * - Preview server must be running (npm run preview)
 * OR
 * - Script will build and start server automatically
 */

import lighthouse from 'lighthouse'
import * as chromeLauncher from 'chrome-launcher'
import { spawn, execSync } from 'child_process'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const LIGHTHOUSE_FLAGS = {
  logLevel: 'error',
  output: 'json',
  onlyCategories: ['performance'],
  formFactor: 'desktop',
  throttling: {
    rttMs: 40,
    throughputKbps: 10240,
    cpuSlowdownMultiplier: 1,
    requestLatencyMs: 0,
    downloadThroughputKbps: 0,
    uploadThroughputKbps: 0,
  },
  screenEmulation: {
    mobile: false,
    width: 1350,
    height: 940,
    deviceScaleFactor: 1,
    disabled: false,
  },
}

const PERFORMANCE_THRESHOLDS = {
  performanceScore: 80,
  lcpMs: 2500,
  cls: 0.1,
  tbtMs: 100,
}

class LighthouseTestRunner {
  constructor() {
    this.chrome = null
    this.previewServer = null
    this.serverUrl = 'http://localhost:4173'
    this.testResults = {
      passed: 0,
      failed: 0,
      tests: [],
    }
  }

  async build() {
    console.log('🔨 Building production bundle...')
    try {
      execSync('npm run build', {
        cwd: __dirname,
        stdio: 'inherit',
      })
      console.log('✅ Production build complete.\n')
    } catch (error) {
      console.error('❌ Build failed:', error.message)
      throw error
    }
  }

  async startPreviewServer() {
    console.log('🚀 Starting preview server...')
    return new Promise((resolve, reject) => {
      this.previewServer = spawn('npm', ['run', 'preview', '--', '--port', '4173', '--host'], {
        cwd: __dirname,
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: true,
      })

      const timeout = setTimeout(() => {
        reject(new Error('Preview server failed to start within 30 seconds'))
      }, 30000)

      this.previewServer.stdout.on('data', (data) => {
        const output = data.toString()
        if (output.includes('4173') || output.includes('Local:')) {
          clearTimeout(timeout)
          console.log('✅ Preview server running at', this.serverUrl, '\n')
          setTimeout(resolve, 2000)
        }
      })

      this.previewServer.stderr.on('data', (data) => {
        const output = data.toString()
        if (!output.includes('ExperimentalWarning')) {
          console.error('Server stderr:', output)
        }
      })

      this.previewServer.on('error', (err) => {
        clearTimeout(timeout)
        reject(err)
      })
    })
  }

  async launchChrome() {
    console.log('🌐 Launching Chrome...')
    this.chrome = await chromeLauncher.launch({
      chromeFlags: ['--headless', '--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage'],
    })
    console.log('✅ Chrome launched on port', this.chrome.port, '\n')
  }

  async runLighthouse() {
    if (!this.chrome) {
      throw new Error('Chrome not initialized')
    }

    console.log('🔍 Running Lighthouse audit...\n')
    const result = await lighthouse(this.serverUrl, {
      ...LIGHTHOUSE_FLAGS,
      port: this.chrome.port,
    })

    const lhr = result.lhr
    const performanceScore = (lhr.categories.performance?.score ?? 0) * 100

    const audits = lhr.audits
    const metrics = {
      performanceScore,
      lcpMs: audits['largest-contentful-paint']?.numericValue ?? 0,
      cls: audits['cumulative-layout-shift']?.numericValue ?? 0,
      tbtMs: audits['total-blocking-time']?.numericValue ?? 0,
      fcpMs: audits['first-contentful-paint']?.numericValue ?? 0,
      siMs: audits['speed-index']?.numericValue ?? 0,
    }

    console.log('═══════════════════════════════════════════')
    console.log('       LIGHTHOUSE PERFORMANCE RESULTS      ')
    console.log('═══════════════════════════════════════════')
    console.log(`  Performance Score: ${metrics.performanceScore.toFixed(0)}/100`)
    console.log(`  LCP:              ${metrics.lcpMs.toFixed(0)}ms`)
    console.log(`  CLS:              ${metrics.cls.toFixed(4)}`)
    console.log(`  TBT:              ${metrics.tbtMs.toFixed(0)}ms`)
    console.log(`  FCP:              ${metrics.fcpMs.toFixed(0)}ms`)
    console.log(`  Speed Index:      ${metrics.siMs.toFixed(0)}ms`)
    console.log('═══════════════════════════════════════════\n')

    return metrics
  }

  runTest(name, condition, actual, expected, comparison) {
    const passed = condition
    const status = passed ? '✅ PASS' : '❌ FAIL'
    const message = `${status}: ${name} (${actual} ${comparison} ${expected})`

    console.log(message)

    if (passed) {
      this.testResults.passed++
    } else {
      this.testResults.failed++
    }

    this.testResults.tests.push({
      name,
      passed,
      actual,
      expected,
      comparison,
    })

    return passed
  }

  async runAllTests() {
    const metrics = await this.runLighthouse()

    console.log('\n═══════════════════════════════════════════')
    console.log('           TEST RESULTS                    ')
    console.log('═══════════════════════════════════════════\n')

    this.runTest(
      'Overall performance score >= 80',
      metrics.performanceScore >= PERFORMANCE_THRESHOLDS.performanceScore,
      metrics.performanceScore.toFixed(0),
      PERFORMANCE_THRESHOLDS.performanceScore,
      '>='
    )

    this.runTest(
      'LCP (Largest Contentful Paint) < 2500ms',
      metrics.lcpMs < PERFORMANCE_THRESHOLDS.lcpMs,
      `${metrics.lcpMs.toFixed(0)}ms`,
      `${PERFORMANCE_THRESHOLDS.lcpMs}ms`,
      '<'
    )

    this.runTest(
      'CLS (Cumulative Layout Shift) < 0.1',
      metrics.cls < PERFORMANCE_THRESHOLDS.cls,
      metrics.cls.toFixed(4),
      PERFORMANCE_THRESHOLDS.cls,
      '<'
    )

    this.runTest(
      'TBT (Total Blocking Time / FID proxy) < 100ms',
      metrics.tbtMs < PERFORMANCE_THRESHOLDS.tbtMs,
      `${metrics.tbtMs.toFixed(0)}ms`,
      `${PERFORMANCE_THRESHOLDS.tbtMs}ms`,
      '<'
    )

    console.log('\n═══════════════════════════════════════════')
    console.log(`  Total: ${this.testResults.passed} passed, ${this.testResults.failed} failed`)
    console.log('═══════════════════════════════════════════\n')

    return {
      metrics,
      results: this.testResults,
      allPassed: this.testResults.failed === 0,
    }
  }

  async cleanup() {
    if (this.chrome) {
      console.log('🧹 Closing Chrome...')
      await this.chrome.kill()
    }

    if (this.previewServer) {
      console.log('🧹 Stopping preview server...')
      this.previewServer.kill('SIGTERM')
    }
  }

  async run() {
    try {
      await this.build()
      await this.startPreviewServer()
      await this.launchChrome()
      const results = await this.runAllTests()

      await this.cleanup()

      if (results.allPassed) {
        console.log('🎉 All Lighthouse performance tests passed!')
        process.exit(0)
      } else {
        console.log('❌ Some Lighthouse performance tests failed.')
        process.exit(1)
      }
    } catch (error) {
      console.error('❌ Error running tests:', error)
      await this.cleanup()
      process.exit(1)
    }
  }
}

const runner = new LighthouseTestRunner()
runner.run()

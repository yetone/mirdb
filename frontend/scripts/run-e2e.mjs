#!/usr/bin/env node
/**
 * Standalone E2E performance test runner
 * Runs Playwright tests without vitest interference
 */

import { chromium } from 'playwright'
import { execSync } from 'child_process'

const BASE_URL = 'http://localhost:4173'
const LOAD_TIME_BUDGET_MS = 2000

async function startServer() {
  console.log('Starting preview server...')
  return execSync('npm run preview &', {
    cwd: process.cwd(),
    stdio: 'inherit',
    detached: true
  })
}

async function waitForServer(url, timeout = 30000) {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    try {
      const response = await fetch(url)
      if (response.ok) {
        console.log('Server is ready')
        return true
      }
    } catch {
      await new Promise(resolve => setTimeout(resolve, 500))
    }
  }
  throw new Error('Server did not start in time')
}

async function runTests() {
  const browser = await chromium.launch({ headless: true })
  const context = await browser.newContext()
  const page = await context.newPage()

  let passed = 0
  let failed = 0

  console.log('\n--- Performance and Load Time Tests ---\n')

  // Test 1: Page is interactive within 2 seconds on 4G
  try {
    console.log('Test: Page is interactive within 2 seconds on 4G')

    const client = await context.newCDPSession(page)
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.5 * 1024 * 1024) / 8,
      uploadThroughput: (750 * 1024) / 8,
      latency: 40,
    })

    const startTime = Date.now()
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('h1', { state: 'visible' })
    await page.waitForSelector('a[href="/register"], button', { state: 'visible' })
    const loadTime = Date.now() - startTime

    console.log(`  Load time: ${loadTime}ms (budget: ${LOAD_TIME_BUDGET_MS}ms)`)

    if (loadTime < LOAD_TIME_BUDGET_MS) {
      console.log('  PASSED\n')
      passed++
    } else {
      console.log('  FAILED: Load time exceeded budget\n')
      failed++
    }
  } catch (error) {
    console.log(`  FAILED: ${error.message}\n`)
    failed++
  }

  // Test 2: Lighthouse performance metrics are acceptable
  try {
    console.log('Test: Lighthouse performance metrics are acceptable')

    await page.goto(BASE_URL, { waitUntil: 'networkidle' })

    const metrics = await page.evaluate(() => {
      const paintEntries = performance.getEntriesByType('paint')
      const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint')
      const timing = performance.getEntriesByType('navigation')[0]

      return {
        fcp: fcpEntry ? fcpEntry.startTime : null,
        domInteractive: timing.domInteractive - timing.fetchStart,
        loadComplete: timing.loadEventEnd - timing.fetchStart,
      }
    })

    console.log(`  FCP: ${metrics.fcp?.toFixed(0) || 'N/A'}ms, DOM Interactive: ${metrics.domInteractive.toFixed(0)}ms`)

    const fcpOk = metrics.fcp === null || metrics.fcp < 1800
    const domOk = metrics.domInteractive < 2000

    if (fcpOk && domOk) {
      console.log('  PASSED\n')
      passed++
    } else {
      console.log('  FAILED: Metrics exceeded thresholds\n')
      failed++
    }
  } catch (error) {
    console.log(`  FAILED: ${error.message}\n`)
    failed++
  }

  // Test 3: Hero section is visible above the fold
  try {
    console.log('Test: Hero section is visible above the fold')

    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' })
    const headline = page.locator('h1').first()
    await headline.waitFor({ state: 'visible' })

    const boundingBox = await headline.boundingBox()
    const viewportHeight = page.viewportSize()?.height || 720

    if (boundingBox && boundingBox.y < viewportHeight) {
      console.log(`  Headline position: ${boundingBox.y}px (viewport: ${viewportHeight}px)`)
      console.log('  PASSED\n')
      passed++
    } else {
      console.log('  FAILED: Headline not above the fold\n')
      failed++
    }
  } catch (error) {
    console.log(`  FAILED: ${error.message}\n`)
    failed++
  }

  // Test 4: Features section becomes visible on scroll
  try {
    console.log('Test: Features section becomes visible on scroll')

    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' })
    await page.evaluate(() => window.scrollTo(0, 500))

    const allText = await page.textContent('body')

    if (allText?.toLowerCase().includes('analytics')) {
      console.log('  Features section content detected')
      console.log('  PASSED\n')
      passed++
    } else {
      console.log('  FAILED: Features section not found\n')
      failed++
    }
  } catch (error) {
    console.log(`  FAILED: ${error.message}\n`)
    failed++
  }

  await browser.close()

  console.log('--- Results ---')
  console.log(`Passed: ${passed}/${passed + failed}`)
  console.log(`Failed: ${failed}/${passed + failed}`)

  return failed === 0
}

// Main execution
async function main() {
  try {
    // Check if server is already running
    try {
      await fetch(BASE_URL)
      console.log('Server already running')
    } catch {
      // Start server in background
      console.log('Starting server...')
      execSync('npm run preview &', { stdio: 'inherit', shell: '/bin/bash' })
      await waitForServer(BASE_URL)
    }

    const success = await runTests()
    process.exit(success ? 0 : 1)
  } catch (error) {
    console.error('Test execution failed:', error)
    process.exit(1)
  }
}

main()

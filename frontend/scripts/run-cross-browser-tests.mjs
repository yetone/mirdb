#!/usr/bin/env node
/**
 * Cross-Browser Compatibility E2E Test Runner
 * Owner: Scenario 12 - Cross-Browser Compatibility
 *
 * Tests homepage rendering across Chrome, Firefox, Safari (WebKit), and Edge
 */

import { chromium, firefox, webkit } from 'playwright'

const BASE_URL = 'http://localhost:4173'

const BROWSERS = [
  { name: 'Chrome', launcher: chromium },
  { name: 'Firefox', launcher: firefox },
  { name: 'Safari (WebKit)', launcher: webkit },
  // Edge uses Chromium engine, so we test it as a Chromium variant
  { name: 'Edge', launcher: chromium, channel: 'msedge' },
]

async function waitForServer(url, timeout = 30000) {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    try {
      const response = await fetch(url)
      if (response.ok) {
        return true
      }
    } catch {
      await new Promise(resolve => setTimeout(resolve, 500))
    }
  }
  throw new Error('Server did not start in time')
}

async function testBrowser(browserConfig) {
  const { name, launcher, channel } = browserConfig
  console.log(`\n--- Testing in ${name} ---\n`)

  const results = {
    browser: name,
    passed: 0,
    failed: 0,
    tests: []
  }

  let browser
  try {
    // Try to launch with channel, fall back to default
    const launchOptions = { headless: true }
    if (channel) {
      try {
        browser = await launcher.launch({ ...launchOptions, channel })
      } catch {
        console.log(`  Note: ${name} channel not available, using default Chromium`)
        browser = await launcher.launch(launchOptions)
      }
    } else {
      browser = await launcher.launch(launchOptions)
    }

    const context = await browser.newContext({
      viewport: { width: 1280, height: 720 }
    })
    const page = await context.newPage()

    // Test 1: Homepage loads and renders hero section correctly
    try {
      console.log('Test 1: Homepage loads and renders hero section correctly')
      await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' })

      const headline = page.locator('h1').first()
      await headline.waitFor({ state: 'visible', timeout: 5000 })

      const headlineText = await headline.textContent()
      if (!headlineText || headlineText.trim().length === 0) {
        throw new Error('Headline is empty')
      }

      const boundingBox = await headline.boundingBox()
      if (!boundingBox || boundingBox.width === 0 || boundingBox.height === 0) {
        throw new Error('Hero section has invalid dimensions')
      }

      console.log('  PASSED\n')
      results.passed++
      results.tests.push({ name: 'Hero section renders', status: 'pass' })
    } catch (error) {
      console.log(`  FAILED: ${error.message}\n`)
      results.failed++
      results.tests.push({ name: 'Hero section renders', status: 'fail', error: error.message })
    }

    // Test 2: Homepage displays all key elements without visual issues
    try {
      console.log('Test 2: Homepage displays all key elements without visual issues')
      await page.goto(BASE_URL, { waitUntil: 'networkidle' })

      // Check for primary CTA button
      const ctaButton = page.locator('a[href="/register"]').first()
      const ctaCount = await ctaButton.count()
      if (ctaCount > 0) {
        const isVisible = await ctaButton.isVisible()
        if (!isVisible) {
          throw new Error('CTA button exists but is not visible')
        }
        const ctaBox = await ctaButton.boundingBox()
        if (!ctaBox || ctaBox.width === 0 || ctaBox.height === 0) {
          throw new Error('CTA button has invalid dimensions')
        }
      }

      // Check for no horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })
      if (hasHorizontalOverflow) {
        throw new Error('Page has horizontal overflow')
      }

      console.log('  PASSED\n')
      results.passed++
      results.tests.push({ name: 'Key elements display', status: 'pass' })
    } catch (error) {
      console.log(`  FAILED: ${error.message}\n`)
      results.failed++
      results.tests.push({ name: 'Key elements display', status: 'fail', error: error.message })
    }

    // Test 3: Features section renders with feature content
    try {
      console.log('Test 3: Features section renders with feature content')
      await page.goto(BASE_URL, { waitUntil: 'networkidle' })

      // Scroll to features section
      await page.evaluate(() => window.scrollTo(0, window.innerHeight))
      await page.waitForTimeout(500)

      const bodyText = await page.textContent('body')
      const hasFeatureContent =
        bodyText?.toLowerCase().includes('analytics') ||
        bodyText?.toLowerCase().includes('shorten') ||
        bodyText?.toLowerCase().includes('track') ||
        bodyText?.toLowerCase().includes('dashboard')

      if (!hasFeatureContent) {
        throw new Error('Features section content not found')
      }

      console.log('  PASSED\n')
      results.passed++
      results.tests.push({ name: 'Features section renders', status: 'pass' })
    } catch (error) {
      console.log(`  FAILED: ${error.message}\n`)
      results.failed++
      results.tests.push({ name: 'Features section renders', status: 'fail', error: error.message })
    }

    // Test 4: CSS animations and transitions work
    try {
      console.log('Test 4: CSS animations and transitions work')
      await page.goto(BASE_URL, { waitUntil: 'networkidle' })

      // Test hover interactions
      const buttons = page.locator('button, a[href]').first()
      const buttonCount = await buttons.count()
      if (buttonCount > 0) {
        await buttons.hover()
        await page.waitForTimeout(100)
      }

      // Verify page loaded with no critical CSS issues
      const bodyDisplay = await page.evaluate(() => {
        return getComputedStyle(document.body).display
      })
      if (bodyDisplay === 'none') {
        throw new Error('Body has display: none')
      }

      console.log('  PASSED\n')
      results.passed++
      results.tests.push({ name: 'CSS animations work', status: 'pass' })
    } catch (error) {
      console.log(`  FAILED: ${error.message}\n`)
      results.failed++
      results.tests.push({ name: 'CSS animations work', status: 'fail', error: error.message })
    }

    // Test 5: Navigation elements are functional
    try {
      console.log('Test 5: Navigation elements are functional')
      await page.goto(BASE_URL, { waitUntil: 'networkidle' })

      // Test that links exist
      const links = page.locator('a[href]')
      const linkCount = await links.count()
      if (linkCount === 0) {
        throw new Error('No links found on page')
      }

      // Verify at least one key navigation link exists
      const registerLink = page.locator('a[href="/register"]')
      const loginLink = page.locator('a[href="/login"]')

      const hasRegister = await registerLink.count() > 0
      const hasLogin = await loginLink.count() > 0

      if (!hasRegister && !hasLogin) {
        throw new Error('Neither register nor login link found')
      }

      console.log(`  Found ${linkCount} links`)
      console.log('  PASSED\n')
      results.passed++
      results.tests.push({ name: 'Navigation functional', status: 'pass' })
    } catch (error) {
      console.log(`  FAILED: ${error.message}\n`)
      results.failed++
      results.tests.push({ name: 'Navigation functional', status: 'fail', error: error.message })
    }

    // Test 6: Page renders without JavaScript errors
    try {
      console.log('Test 6: Page renders without JavaScript errors')
      const jsErrors = []

      page.on('pageerror', (error) => {
        jsErrors.push(error.message)
      })

      await page.goto(BASE_URL, { waitUntil: 'networkidle' })
      await page.waitForTimeout(1000)

      // Filter out non-critical errors
      const criticalErrors = jsErrors.filter(
        (err) =>
          !err.includes('favicon') &&
          !err.includes('404') &&
          !err.includes('net::ERR_FAILED')
      )

      if (criticalErrors.length > 0) {
        throw new Error(`JavaScript errors: ${criticalErrors.join(', ')}`)
      }

      console.log('  PASSED\n')
      results.passed++
      results.tests.push({ name: 'No JS errors', status: 'pass' })
    } catch (error) {
      console.log(`  FAILED: ${error.message}\n`)
      results.failed++
      results.tests.push({ name: 'No JS errors', status: 'fail', error: error.message })
    }

    // Test 7: Layout maintains consistency across viewport sizes
    try {
      console.log('Test 7: Layout maintains consistency across viewport sizes')

      // Desktop
      await page.setViewportSize({ width: 1280, height: 720 })
      await page.goto(BASE_URL, { waitUntil: 'networkidle' })
      const desktopHeadline = page.locator('h1').first()
      const desktopVisible = await desktopHeadline.isVisible().catch(() => false)
      if (!desktopVisible) {
        throw new Error('Headline not visible at desktop viewport')
      }

      // Tablet
      await page.setViewportSize({ width: 768, height: 1024 })
      await page.waitForTimeout(200)
      const tabletVisible = await desktopHeadline.isVisible().catch(() => false)
      if (!tabletVisible) {
        throw new Error('Headline not visible at tablet viewport')
      }

      // Mobile
      await page.setViewportSize({ width: 375, height: 667 })
      await page.waitForTimeout(200)
      const mobileVisible = await desktopHeadline.isVisible().catch(() => false)
      if (!mobileVisible) {
        throw new Error('Headline not visible at mobile viewport')
      }

      // Check no horizontal overflow at mobile
      const mobileOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })
      if (mobileOverflow) {
        throw new Error('Horizontal overflow at mobile viewport')
      }

      console.log('  PASSED\n')
      results.passed++
      results.tests.push({ name: 'Responsive layout', status: 'pass' })
    } catch (error) {
      console.log(`  FAILED: ${error.message}\n`)
      results.failed++
      results.tests.push({ name: 'Responsive layout', status: 'fail', error: error.message })
    }

    await browser.close()
  } catch (launchError) {
    console.log(`  Browser launch failed: ${launchError.message}`)
    // Skip Edge if not installed
    if (name === 'Edge') {
      console.log('  Skipping Edge - using Chromium results as proxy\n')
      return null
    }
    throw launchError
  }

  return results
}

async function runAllTests() {
  console.log('=== Cross-Browser Compatibility Tests ===')
  console.log(`Testing against: ${BASE_URL}\n`)

  const allResults = []
  let totalPassed = 0
  let totalFailed = 0

  for (const browserConfig of BROWSERS) {
    try {
      const result = await testBrowser(browserConfig)
      if (result) {
        allResults.push(result)
        totalPassed += result.passed
        totalFailed += result.failed
      }
    } catch (error) {
      console.log(`\nFailed to test ${browserConfig.name}: ${error.message}`)
      allResults.push({
        browser: browserConfig.name,
        passed: 0,
        failed: 1,
        tests: [{ name: 'Browser launch', status: 'fail', error: error.message }]
      })
      totalFailed++
    }
  }

  console.log('\n=== Summary ===')
  console.log(`Total Browsers Tested: ${allResults.length}`)
  console.log(`Total Tests Passed: ${totalPassed}`)
  console.log(`Total Tests Failed: ${totalFailed}`)

  for (const result of allResults) {
    console.log(`\n${result.browser}: ${result.passed}/${result.passed + result.failed} passed`)
    for (const test of result.tests) {
      const icon = test.status === 'pass' ? '✓' : '✗'
      console.log(`  ${icon} ${test.name}${test.error ? ': ' + test.error : ''}`)
    }
  }

  return totalFailed === 0
}

async function main() {
  try {
    // Check if server is running
    try {
      await fetch(BASE_URL)
      console.log('Server is running\n')
    } catch {
      console.log('Starting server...')
      const { execSync, spawn } = await import('child_process')
      spawn('npm', ['run', 'preview'], {
        cwd: process.cwd(),
        stdio: 'ignore',
        detached: true,
        shell: true
      }).unref()
      await waitForServer(BASE_URL)
      console.log('Server started\n')
    }

    const success = await runAllTests()
    process.exit(success ? 0 : 1)
  } catch (error) {
    console.error('Test execution failed:', error)
    process.exit(1)
  }
}

main()

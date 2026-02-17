/**
 * E2E Console Error Verification Tests
 *
 * Scenario 15: No Console Errors
 * - Test Case 1: Load homepage and capture console output - No JavaScript errors on page load
 * - Test Case 2: Interact with all features and capture console - No errors during user interactions
 * - Test Case 3: Navigate away and back - No memory leak warnings or unmount errors
 *
 * These tests verify that the homepage renders without JavaScript console errors,
 * ensuring code quality and proper error handling.
 */

import { test, expect, type Page, type ConsoleMessage } from '@playwright/test'

/**
 * Helper to categorize console messages
 */
function categorizeConsoleMessage(msg: ConsoleMessage): {
  isError: boolean
  isWarning: boolean
  isAcceptableWarning: boolean
} {
  const type = msg.type()
  const text = msg.text()

  // Known acceptable warnings (deprecation notices, dev-mode warnings)
  const acceptableWarnings = [
    'Warning: ReactDOM.render is deprecated',
    'Warning: componentWillReceiveProps',
    'Warning: componentWillMount',
    '[HMR]', // Hot Module Replacement
    '[vite]', // Vite dev server
    'Download the React DevTools',
    'Warning: Each child in a list should have a unique',
    'Warning: findDOMNode is deprecated',
    'DevTools failed to load',
    'Autofocus processing was blocked',
    'Third-party cookie',
    'net::ERR_', // Network errors during testing
    'Failed to load resource: net::',
    'The resource ',
  ]

  const isAcceptableWarning = acceptableWarnings.some((warning) =>
    text.includes(warning)
  )

  return {
    isError: type === 'error',
    isWarning: type === 'warning',
    isAcceptableWarning,
  }
}

/**
 * Collector class for console messages during test execution
 */
class ConsoleCollector {
  private messages: ConsoleMessage[] = []
  private errors: ConsoleMessage[] = []
  private warnings: ConsoleMessage[] = []

  constructor(private page: Page) {
    this.page.on('console', (msg) => {
      this.messages.push(msg)
      const { isError, isWarning, isAcceptableWarning } =
        categorizeConsoleMessage(msg)

      if (isError && !isAcceptableWarning) {
        this.errors.push(msg)
      }
      if (isWarning && !isAcceptableWarning) {
        this.warnings.push(msg)
      }
    })
  }

  getErrors(): ConsoleMessage[] {
    return this.errors
  }

  getWarnings(): ConsoleMessage[] {
    return this.warnings
  }

  getAll(): ConsoleMessage[] {
    return this.messages
  }

  clear(): void {
    this.messages = []
    this.errors = []
    this.warnings = []
  }

  getErrorTexts(): string[] {
    return this.errors.map((msg) => msg.text())
  }

  getWarningTexts(): string[] {
    return this.warnings.map((msg) => msg.text())
  }
}

/**
 * Test Case 1: Load homepage and capture console output
 * Input: Load homepage and capture console output
 * Expected: No JavaScript errors in console on page load
 */
test.describe('No Console Errors - Page Load', () => {
  test('homepage loads without JavaScript errors', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for the page to be fully rendered
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Wait a moment for any async operations to complete
    await page.waitForTimeout(500)

    // Check for console errors
    const errors = collector.getErrorTexts()

    // Log any errors for debugging
    if (errors.length > 0) {
      console.log('Console errors found:', errors)
    }

    // Assert no JavaScript errors occurred
    expect(errors.length).toBe(0)
  })

  test('homepage loads without critical warnings', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Wait for the page to be fully rendered
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Wait for React to fully hydrate
    await page.waitForTimeout(500)

    // Check for warnings that indicate code issues
    const warnings = collector.getWarningTexts()

    // Log warnings for debugging
    if (warnings.length > 0) {
      console.log('Console warnings found:', warnings)
    }

    // Allow some warnings but none should indicate React/JS errors
    const criticalWarnings = warnings.filter(
      (w) =>
        w.includes('undefined') ||
        w.includes('Cannot read property') ||
        w.includes('is not a function') ||
        w.includes('TypeError') ||
        w.includes('ReferenceError')
    )

    expect(criticalWarnings.length).toBe(0)
  })

  test('no uncaught promise rejections on page load', async ({ page }) => {
    const unhandledRejections: string[] = []

    // Listen for page errors (uncaught exceptions)
    page.on('pageerror', (error) => {
      unhandledRejections.push(error.message)
    })

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for any async operations
    await page.waitForTimeout(1000)

    // Log any errors for debugging
    if (unhandledRejections.length > 0) {
      console.log('Unhandled rejections:', unhandledRejections)
    }

    // Assert no uncaught exceptions
    expect(unhandledRejections.length).toBe(0)
  })
})

/**
 * Test Case 2: Interact with all features and capture console
 * Input: Interact with all features and capture console
 * Expected: No errors during user interactions
 */
test.describe('No Console Errors - User Interactions', () => {
  test('no errors when clicking theme toggle', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Try to find and click theme toggle
    const themeToggle = page.getByTestId('theme-toggle')
    const themeToggleExists = (await themeToggle.count()) > 0

    if (themeToggleExists) {
      // Click theme toggle multiple times
      await themeToggle.click()
      await page.waitForTimeout(300)
      await themeToggle.click()
      await page.waitForTimeout(300)
    }

    // Check for errors during theme toggle
    const errors = collector.getErrorTexts()
    expect(errors.length).toBe(0)
  })

  test('no errors when interacting with hero section buttons', async ({
    page,
  }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Click the primary CTA button (Get Started)
    const primaryCta = page.getByRole('button', { name: /get started/i })
    if ((await primaryCta.count()) > 0) {
      await primaryCta.click()

      // Wait for navigation
      await page.waitForURL(/\/register/)

      // Check for errors during navigation
      const errors = collector.getErrorTexts()
      expect(errors.length).toBe(0)
    }
  })

  test('no errors when using URL demo section', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Try to interact with URL demo section if it exists
    const urlInput = page.getByTestId('url-demo-input')
    const urlInputExists = (await urlInput.count()) > 0

    if (urlInputExists) {
      // Type a URL into the input
      await urlInput.fill('https://example.com')
      await page.waitForTimeout(300)

      // Try to click the shorten button
      const shortenButton = page.getByTestId('url-demo-submit')
      if ((await shortenButton.count()) > 0) {
        await shortenButton.click()
        await page.waitForTimeout(500)
      }
    }

    // Check for errors during URL demo interaction
    const errors = collector.getErrorTexts()
    expect(errors.length).toBe(0)
  })

  test('no errors when scrolling through page', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Scroll through the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))
    await page.waitForTimeout(500)

    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(500)

    // Scroll to middle
    await page.evaluate(
      () => window.scrollTo(0, document.body.scrollHeight / 2)
    )
    await page.waitForTimeout(300)

    // Check for errors during scrolling
    const errors = collector.getErrorTexts()
    expect(errors.length).toBe(0)
  })

  test('no errors when hovering over interactive elements', async ({
    page,
  }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Hover over buttons
    const buttons = page.getByRole('button')
    const buttonCount = await buttons.count()

    for (let i = 0; i < Math.min(buttonCount, 5); i++) {
      await buttons.nth(i).hover()
      await page.waitForTimeout(100)
    }

    // Hover over links
    const links = page.getByRole('link')
    const linkCount = await links.count()

    for (let i = 0; i < Math.min(linkCount, 5); i++) {
      await links.nth(i).hover()
      await page.waitForTimeout(100)
    }

    // Check for errors during hover interactions
    const errors = collector.getErrorTexts()
    expect(errors.length).toBe(0)
  })

  test('no errors when interacting with footer links', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Scroll to footer
    const footer = page.getByRole('contentinfo')
    if ((await footer.count()) > 0) {
      await footer.scrollIntoViewIfNeeded()
      await page.waitForTimeout(300)

      // Check for any links in footer and hover
      const footerLinks = footer.getByRole('link')
      const footerLinkCount = await footerLinks.count()

      for (let i = 0; i < Math.min(footerLinkCount, 3); i++) {
        await footerLinks.nth(i).hover()
        await page.waitForTimeout(100)
      }
    }

    // Check for errors during footer interactions
    const errors = collector.getErrorTexts()
    expect(errors.length).toBe(0)
  })
})

/**
 * Test Case 3: Navigate away and back
 * Input: Navigate away and back
 * Expected: No memory leak warnings or unmount errors
 */
test.describe('No Console Errors - Navigation', () => {
  test('no errors when navigating to login and back', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Navigate to login
    await page.goto('/login', { waitUntil: 'networkidle' })
    await page.waitForTimeout(500)

    // Navigate back to homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })
    await page.waitForTimeout(500)

    // Check for unmount/cleanup errors
    const errors = collector.getErrorTexts()

    // Log any errors for debugging
    if (errors.length > 0) {
      console.log('Navigation errors:', errors)
    }

    expect(errors.length).toBe(0)
  })

  test('no errors when navigating to register and back', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Navigate to register
    await page.goto('/register', { waitUntil: 'networkidle' })
    await page.waitForTimeout(500)

    // Navigate back to homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })
    await page.waitForTimeout(500)

    // Check for unmount/cleanup errors
    const errors = collector.getErrorTexts()
    expect(errors.length).toBe(0)
  })

  test('no memory leak warnings on component unmount', async ({ page }) => {
    const memoryWarnings: string[] = []

    // Listen for console messages that indicate memory leaks
    page.on('console', (msg) => {
      const text = msg.text()
      if (
        text.includes("Can't perform a React state update on an unmounted") ||
        text.includes('memory leak') ||
        text.includes('Warning: Can\'t call') ||
        text.includes('abort') ||
        text.includes('cleanup')
      ) {
        memoryWarnings.push(text)
      }
    })

    // Navigate multiple times to trigger mount/unmount cycles
    for (let i = 0; i < 3; i++) {
      await page.goto('/', { waitUntil: 'domcontentloaded' })
      await page.waitForTimeout(300)
      await page.goto('/login', { waitUntil: 'domcontentloaded' })
      await page.waitForTimeout(300)
    }

    // Return to homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForTimeout(500)

    // Log any warnings for debugging
    if (memoryWarnings.length > 0) {
      console.log('Memory warnings:', memoryWarnings)
    }

    // Assert no memory leak warnings
    expect(memoryWarnings.length).toBe(0)
  })

  test('no errors using browser back/forward navigation', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Clear any initial errors
    collector.clear()

    // Navigate to login page using link
    const loginCta = page.getByTestId('hero-login-cta')
    if ((await loginCta.count()) > 0) {
      await loginCta.click()
      await page.waitForURL(/\/login/)
      await page.waitForTimeout(300)

      // Use browser back button
      await page.goBack()
      await page.waitForSelector('[data-testid="hero-section"]', {
        state: 'visible',
      })
      await page.waitForTimeout(300)

      // Use browser forward button
      await page.goForward()
      await page.waitForURL(/\/login/)
      await page.waitForTimeout(300)

      // Check for errors during browser navigation
      const errors = collector.getErrorTexts()
      expect(errors.length).toBe(0)
    }
  })

  test('no errors during rapid navigation', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Clear any initial errors
    collector.clear()

    // Perform rapid navigation (simulates user clicking quickly)
    const routes = ['/', '/login', '/', '/register', '/', '/login', '/']

    for (const route of routes) {
      await page.goto(route, { waitUntil: 'domcontentloaded' })
      // Very short wait to simulate rapid clicks
      await page.waitForTimeout(100)
    }

    // Wait for any cleanup operations
    await page.waitForTimeout(1000)

    // Check for errors during rapid navigation
    const errors = collector.getErrorTexts()

    // Log any errors for debugging
    if (errors.length > 0) {
      console.log('Rapid navigation errors:', errors)
    }

    expect(errors.length).toBe(0)
  })
})

/**
 * Additional comprehensive tests
 */
test.describe('No Console Errors - Comprehensive', () => {
  test('full user journey without console errors', async ({ page }) => {
    const collector = new ConsoleCollector(page)

    // Step 1: Load homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="hero-section"]', {
      state: 'visible',
    })

    // Step 2: Scroll through the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))
    await page.waitForTimeout(300)
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(300)

    // Step 3: Try theme toggle if available
    const themeToggle = page.getByTestId('theme-toggle')
    if ((await themeToggle.count()) > 0) {
      await themeToggle.click()
      await page.waitForTimeout(200)
    }

    // Step 4: Try URL demo if available
    const urlInput = page.getByTestId('url-demo-input')
    if ((await urlInput.count()) > 0) {
      await urlInput.fill('https://example.com')
      await page.waitForTimeout(200)
    }

    // Step 5: Navigate to login
    const loginCta = page.getByTestId('hero-login-cta')
    if ((await loginCta.count()) > 0) {
      await loginCta.click()
      await page.waitForURL(/\/login/)
      await page.waitForTimeout(300)
    }

    // Step 6: Navigate back to homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForTimeout(500)

    // Check for any errors throughout the journey
    const errors = collector.getErrorTexts()

    // Log any errors for debugging
    if (errors.length > 0) {
      console.log('Full journey errors:', errors)
    }

    expect(errors.length).toBe(0)
  })

  test('no runtime errors in React components', async ({ page }) => {
    const runtimeErrors: string[] = []

    // Listen specifically for React runtime errors
    page.on('console', (msg) => {
      const text = msg.text()
      if (
        msg.type() === 'error' &&
        (text.includes('Uncaught') ||
          text.includes('TypeError') ||
          text.includes('ReferenceError') ||
          text.includes('is not defined') ||
          text.includes('Cannot read') ||
          text.includes('is not a function'))
      ) {
        runtimeErrors.push(text)
      }
    })

    page.on('pageerror', (error) => {
      runtimeErrors.push(error.message)
    })

    // Load homepage and interact
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForTimeout(1000)

    // Perform various interactions
    const buttons = page.getByRole('button')
    const buttonCount = await buttons.count()

    for (let i = 0; i < Math.min(buttonCount, 3); i++) {
      try {
        await buttons.nth(i).click({ timeout: 1000 })
        await page.waitForTimeout(200)
      } catch {
        // Ignore click errors (e.g., navigation that changes button availability)
      }
    }

    // Log any errors for debugging
    if (runtimeErrors.length > 0) {
      console.log('Runtime errors:', runtimeErrors)
    }

    expect(runtimeErrors.length).toBe(0)
  })
})

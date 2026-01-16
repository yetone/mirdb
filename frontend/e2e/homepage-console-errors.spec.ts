import { test, expect } from '@playwright/test'

test.describe('Homepage Console Errors', () => {
  // Test Case 4: Check for console errors on render
  test.describe('Test Case 4: No console errors or warnings on initial render', () => {
    test('homepage renders without console errors', async ({ page }) => {
      const consoleErrors: string[] = []
      const consoleWarnings: string[] = []

      // Capture console errors and warnings
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text())
        }
        if (msg.type() === 'warning') {
          consoleWarnings.push(msg.text())
        }
      })

      // Navigate to homepage
      await page.goto('/')

      // Wait for the page to be fully loaded
      await page.waitForLoadState('networkidle')

      // Verify hero section is rendered (page is interactive)
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Filter out known/expected warnings (e.g., React StrictMode duplicates)
      const filteredErrors = consoleErrors.filter(
        (error) =>
          // Filter out known false positives
          !error.includes('Download the React DevTools') &&
          !error.includes('React does not recognize')
      )

      const filteredWarnings = consoleWarnings.filter(
        (warning) =>
          // Filter out known acceptable warnings
          !warning.includes('ReactDOM.render is no longer supported') &&
          !warning.includes('Download the React DevTools')
      )

      // Assert no unexpected console errors
      expect(filteredErrors).toHaveLength(0)

      // Assert no critical warnings that indicate component issues
      const criticalWarnings = filteredWarnings.filter(
        (warning) =>
          warning.includes('Invalid prop') ||
          warning.includes('Failed prop type') ||
          warning.includes('Warning: Each child in a list should have a unique') ||
          warning.includes('Warning: Cannot update a component')
      )
      expect(criticalWarnings).toHaveLength(0)
    })

    test('homepage has no JavaScript exceptions on load', async ({ page }) => {
      const pageErrors: Error[] = []

      // Capture page errors (uncaught exceptions)
      page.on('pageerror', (error) => {
        pageErrors.push(error)
      })

      // Navigate to homepage
      await page.goto('/')

      // Wait for the page to be fully loaded
      await page.waitForLoadState('networkidle')

      // Verify the page loaded successfully
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Assert no uncaught JavaScript exceptions
      expect(pageErrors).toHaveLength(0)
    })

    test('all homepage sections render without errors', async ({ page }) => {
      const consoleErrors: string[] = []

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text())
        }
      })

      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Verify all main sections are visible
      await expect(page.getByTestId('navbar')).toBeVisible()
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('features-section')).toBeVisible()
      await expect(page.getByTestId('social-proof-section')).toBeVisible()
      await expect(page.getByTestId('how-it-works-section')).toBeVisible()
      await expect(page.getByTestId('footer-cta')).toBeVisible()
      await expect(page.getByTestId('footer')).toBeVisible()

      // Filter and check for unexpected errors
      const unexpectedErrors = consoleErrors.filter(
        (error) => !error.includes('Download the React DevTools')
      )
      expect(unexpectedErrors).toHaveLength(0)
    })

    test('homepage theme toggle does not cause console errors', async ({ page }) => {
      const consoleErrors: string[] = []

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text())
        }
      })

      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Open theme toggle dropdown (use the navbar one which is the first)
      const themeToggleButton = page.getByTestId('navbar-theme-toggle').getByTestId('theme-toggle-button')
      await themeToggleButton.click()

      // Select dark theme (use first visible dropdown)
      const darkOption = page.getByTestId('theme-option-dark').first()
      await darkOption.click()

      // Wait for theme to apply
      await page.waitForTimeout(500)

      // Verify theme changed
      const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'))
      expect(theme).toBe('dark')

      // Filter and check for errors
      const unexpectedErrors = consoleErrors.filter(
        (error) => !error.includes('Download the React DevTools')
      )
      expect(unexpectedErrors).toHaveLength(0)
    })

    test('homepage navigation interactions do not cause console errors', async ({ page }) => {
      const consoleErrors: string[] = []

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text())
        }
      })

      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Scroll to different sections
      await page.getByTestId('features-section').scrollIntoViewIfNeeded()
      await page.waitForTimeout(200)

      await page.getByTestId('how-it-works-section').scrollIntoViewIfNeeded()
      await page.waitForTimeout(200)

      await page.getByTestId('footer').scrollIntoViewIfNeeded()
      await page.waitForTimeout(200)

      // Filter and check for errors
      const unexpectedErrors = consoleErrors.filter(
        (error) => !error.includes('Download the React DevTools')
      )
      expect(unexpectedErrors).toHaveLength(0)
    })
  })
})

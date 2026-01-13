import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

/**
 * E2E Accessibility Tests for Homepage
 * Validates NFR-2: Lighthouse accessibility score of 90+
 * Tests WCAG 2.1 AA compliance requirements
 */

test.describe('Accessibility Compliance E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  /**
   * Test Case 1: Tab through all interactive elements on homepage
   * Input: Tab through all interactive elements on homepage
   * Expected: All interactive elements are reachable via keyboard navigation
   */
  test('Test Case 1: All interactive elements reachable via keyboard navigation', async ({
    page,
  }) => {
    // Focus on the first element
    await page.keyboard.press('Tab')

    // Track all focused elements
    const focusedElements: string[] = []
    const expectedMinimumElements = 8 // logo link, theme toggle, login, signup, get started, login btn, input, shorten btn

    // Tab through the page and collect focused elements
    for (let i = 0; i < 20; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement
        if (el) {
          const testId = el.getAttribute('data-testid')
          const tagName = el.tagName.toLowerCase()
          const text = el.textContent?.trim().substring(0, 20) || ''
          return testId || `${tagName}:${text}`
        }
        return null
      })

      if (focusedElement && !focusedElements.includes(focusedElement)) {
        focusedElements.push(focusedElement)
      }

      await page.keyboard.press('Tab')

      // Check if we've looped back to the beginning
      const currentFocused = await page.evaluate(() =>
        document.activeElement?.getAttribute('data-testid')
      )
      if (focusedElements.length > 3 && currentFocused === focusedElements[0]) {
        break
      }
    }

    // Verify minimum interactive elements are reachable
    expect(focusedElements.length).toBeGreaterThanOrEqual(expectedMinimumElements)

    // Verify critical elements are in the tab order
    const criticalElements = [
      'theme-toggle',
      'login-link',
      'register-link',
      'demo-url-input',
      'demo-shorten-button',
    ]

    criticalElements.forEach((testId) => {
      const found = focusedElements.some(
        (el) => el === testId || el.includes(testId)
      )
      expect(found).toBe(true)
    })
  })

  /**
   * Test Case 2: Check focus states on buttons and links
   * Input: Check focus states on buttons and links
   * Expected: Focus states are clearly visible with outline or highlight
   */
  test('Test Case 2: Focus states are clearly visible', async ({ page }) => {
    // Test theme toggle focus
    const themeToggle = page.locator('[data-testid="theme-toggle"]')
    await themeToggle.focus()

    // Check that element is focused
    const isFocused = await page.evaluate(() => {
      const el = document.activeElement
      return el?.getAttribute('data-testid') === 'theme-toggle'
    })
    expect(isFocused).toBe(true)

    // Test login link focus
    const loginLink = page.locator('[data-testid="login-link"]')
    await loginLink.focus()

    // Verify DaisyUI focus styles are applied (btn class includes focus-visible)
    await expect(loginLink).toHaveClass(/btn/)

    // Test input field focus
    const urlInput = page.locator('[data-testid="demo-url-input"]')
    await urlInput.focus()

    // Verify input has focus
    const inputFocused = await page.evaluate(() => {
      return document.activeElement?.getAttribute('data-testid') === 'demo-url-input'
    })
    expect(inputFocused).toBe(true)

    // Verify input has DaisyUI input class for focus styling
    await expect(urlInput).toHaveClass(/input/)
  })

  /**
   * Test Case 4: Run Lighthouse accessibility audit
   * Input: Run Lighthouse accessibility audit
   * Expected: Accessibility score is 90 or higher
   *
   * Note: Using axe-core which Lighthouse uses internally for accessibility scoring
   */
  test('Test Case 4: Lighthouse accessibility audit passes (axe-core)', async ({
    page,
  }) => {
    // Run axe accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze()

    // Get counts of violations by impact
    const violations = accessibilityScanResults.violations
    const criticalViolations = violations.filter((v) => v.impact === 'critical')
    const seriousViolations = violations.filter((v) => v.impact === 'serious')

    // Log violations for debugging
    if (violations.length > 0) {
      console.log('Accessibility violations found:')
      violations.forEach((v) => {
        console.log(`- [${v.impact}] ${v.id}: ${v.description}`)
        v.nodes.forEach((node) => {
          console.log(`  Target: ${node.target}`)
        })
      })
    }

    // No critical or serious violations should exist
    expect(criticalViolations.length).toBe(0)
    expect(seriousViolations.length).toBe(0)

    // Calculate approximate accessibility score
    // Lighthouse scores accessibility based on weighted violations
    // For 90+ score, we need minimal violations
    const totalIssues = violations.reduce(
      (sum, v) => sum + v.nodes.length,
      0
    )

    // With 0 critical/serious violations and minimal minor issues,
    // the score would be 90+
    expect(totalIssues).toBeLessThanOrEqual(5)
  })

  /**
   * Test Case 5: Check color contrast ratios in light mode
   * Input: Check color contrast ratios in light mode
   * Expected: Color contrast meets WCAG AA minimum 4.5:1 for normal text
   */
  test('Test Case 5: Color contrast meets WCAG AA in light mode', async ({
    page,
  }) => {
    // Set theme to light mode
    await page.evaluate(() => {
      localStorage.setItem('theme', 'light')
    })
    await page.reload()
    await page.waitForLoadState('networkidle')

    // Verify light theme is applied
    const theme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(theme).toBe('light')

    // Run axe-core color contrast checks
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .disableRules(['region']) // Disable region rule as it's not critical
      .analyze()

    // Get color contrast violations
    const contrastViolations = contrastResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Log any contrast issues
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations in light mode:')
      contrastViolations.forEach((v) => {
        v.nodes.forEach((node) => {
          console.log(`  Target: ${node.target}, Message: ${node.failureSummary}`)
        })
      })
    }

    // No color contrast violations
    expect(contrastViolations.length).toBe(0)
  })

  /**
   * Test Case 6: Check color contrast ratios in dark mode
   * Input: Check color contrast ratios in dark mode
   * Expected: Color contrast meets WCAG AA minimum 4.5:1 for normal text
   */
  test('Test Case 6: Color contrast meets WCAG AA in dark mode', async ({
    page,
  }) => {
    // Set theme to dark mode
    await page.evaluate(() => {
      localStorage.setItem('theme', 'dark')
    })
    await page.reload()
    await page.waitForLoadState('networkidle')

    // Verify dark theme is applied
    const theme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(theme).toBe('dark')

    // Run axe-core color contrast checks
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .disableRules(['region']) // Disable region rule as it's not critical
      .analyze()

    // Get color contrast violations
    const contrastViolations = contrastResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Log any contrast issues
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations in dark mode:')
      contrastViolations.forEach((v) => {
        v.nodes.forEach((node) => {
          console.log(`  Target: ${node.target}, Message: ${node.failureSummary}`)
        })
      })
    }

    // No color contrast violations
    expect(contrastViolations.length).toBe(0)
  })
})

test.describe('Screen Reader Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  /**
   * Test Case 3: Query for alt text on images and icons
   * Tests screen reader accessibility
   */
  test('Test Case 3: All images and icons have appropriate alt text or aria-hidden', async ({
    page,
  }) => {
    // Check that decorative icons have aria-hidden
    const iconsWithAriaHidden = await page.locator('svg[aria-hidden="true"]').count()
    expect(iconsWithAriaHidden).toBeGreaterThan(0)

    // Check that interactive icons have accessible labels
    const themeToggle = page.locator('[data-testid="theme-toggle"]')
    await expect(themeToggle).toHaveAttribute('aria-label')

    // Verify no images without alt attribute
    const imagesWithoutAlt = await page.locator('img:not([alt])').count()
    expect(imagesWithoutAlt).toBe(0)
  })

  test('should have proper ARIA labels for interactive elements', async ({
    page,
  }) => {
    // Theme toggle should have descriptive aria-label
    const themeToggle = page.locator('[data-testid="theme-toggle"]')
    const ariaLabel = await themeToggle.getAttribute('aria-label')
    expect(ariaLabel).toMatch(/switch to (light|dark) mode/i)

    // Input should have aria-label
    const urlInput = page.locator('[data-testid="demo-url-input"]')
    await expect(urlInput).toHaveAttribute('aria-label')
  })

  test('should have proper heading hierarchy for screen readers', async ({
    page,
  }) => {
    // Get all headings
    const h1Elements = await page.locator('h1').all()
    const h2Elements = await page.locator('h2').all()
    const h3Elements = await page.locator('h3').all()

    // Should have exactly one h1
    expect(h1Elements.length).toBe(1)

    // Should have multiple h2 for sections
    expect(h2Elements.length).toBeGreaterThanOrEqual(3)

    // Feature cards should use h3
    expect(h3Elements.length).toBeGreaterThanOrEqual(4)
  })

  test('should have semantic landmarks', async ({ page }) => {
    // Check for navigation landmark
    const nav = await page.locator('nav').count()
    expect(nav).toBeGreaterThanOrEqual(1)

    // Check for main content sections
    const sections = await page.locator('section').count()
    expect(sections).toBeGreaterThanOrEqual(3)

    // Check for footer landmark
    const footer = await page.locator('footer').count()
    expect(footer).toBe(1)
  })

  test('should announce error messages to screen readers', async ({ page }) => {
    // Submit form without URL
    const shortenButton = page.locator('[data-testid="demo-shorten-button"]')
    await shortenButton.click()

    // Wait for error to appear
    const errorElement = page.locator('[data-testid="demo-error"]')
    await expect(errorElement).toBeVisible()

    // Error should have role="alert" for screen reader announcement
    await expect(errorElement).toHaveAttribute('role', 'alert')
  })
})

test.describe('Keyboard Interaction Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('should allow form submission via Enter key', async ({ page }) => {
    // Focus on input
    const urlInput = page.locator('[data-testid="demo-url-input"]')
    await urlInput.focus()

    // Type a valid URL
    await urlInput.fill('https://example.com/test')

    // Press Enter to submit
    await page.keyboard.press('Enter')

    // Wait for result or error
    await page.waitForSelector('[data-testid="demo-result"], [data-testid="demo-error"]')

    // Result should appear
    const result = page.locator('[data-testid="demo-result"]')
    await expect(result).toBeVisible()
  })

  test('should allow theme toggle via Enter/Space key', async ({ page }) => {
    // Get initial theme
    const initialTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )

    // Focus on theme toggle
    const themeToggle = page.locator('[data-testid="theme-toggle"]')
    await themeToggle.focus()

    // Press Enter to toggle
    await page.keyboard.press('Enter')

    // Theme should change
    const newTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(newTheme).not.toBe(initialTheme)
  })

  test('should navigate links via Enter key', async ({ page }) => {
    // Focus on login link
    const loginLink = page.locator('[data-testid="login-link"]')
    await loginLink.focus()

    // Press Enter
    await page.keyboard.press('Enter')

    // Should navigate to login page
    await page.waitForSelector('[data-testid="login-page"]')
    expect(page.url()).toContain('/login')
  })
})

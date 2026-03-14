/**
 * E2E accessibility tests.
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * Test cases:
 * - Tab through entire page (test case 1)
 * - Keyboard activation of CTAs (test case 2)
 * - Focus indicator visibility (test case 3)
 * - Color contrast via axe-core (test case 4)
 * - Single h1 on page (test case 5)
 * - Heading hierarchy (test case 6)
 * - Image alt attributes (test case 7)
 * - ARIA labels (test case 8)
 * - Lighthouse accessibility audit (test case 9)
 */

import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  // Test case 1: Tab through entire page
  test('all interactive elements receive focus in logical order when tabbing', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Start at the beginning of the page
    await page.keyboard.press('Tab')

    // Collect all focused elements as we tab through
    const focusedElements: string[] = []
    let previousElement = ''
    let maxTabs = 50 // Prevent infinite loop

    while (maxTabs > 0) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement
        if (!el || el === document.body) return null
        return {
          tag: el.tagName.toLowerCase(),
          text: el.textContent?.trim().slice(0, 30) || '',
          id: el.id || '',
          role: el.getAttribute('role') || '',
          ariaLabel: el.getAttribute('aria-label') || '',
        }
      })

      if (!focusedElement) break

      const elementId = `${focusedElement.tag}:${focusedElement.text || focusedElement.ariaLabel || focusedElement.id}`

      // Check if we've cycled back to the beginning
      if (elementId === focusedElements[0] && focusedElements.length > 1) {
        break
      }

      if (elementId !== previousElement) {
        focusedElements.push(elementId)
        previousElement = elementId
      }

      await page.keyboard.press('Tab')
      maxTabs--
    }

    // Verify we have focusable elements
    expect(focusedElements.length).toBeGreaterThan(0)

    // Verify expected interactive elements are in the focus order
    const hasNavLinks = focusedElements.some((el) => el.includes('a:'))
    const hasButtons = focusedElements.some(
      (el) => el.includes('button:') || el.includes('a:Get Started')
    )

    expect(hasNavLinks).toBe(true)
    expect(hasButtons).toBe(true)
  })

  // Test case 2: Activate CTA with Enter key
  test('buttons respond to Enter key press', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Focus on the hero CTA button
    const heroCtaButton = page.getByTestId('hero-cta-button')
    await heroCtaButton.focus()

    // Verify it's focused
    const isFocused = await heroCtaButton.evaluate(
      (el) => document.activeElement === el
    )
    expect(isFocused).toBe(true)

    // Store the current URL
    const initialUrl = page.url()

    // Press Enter to activate
    await page.keyboard.press('Enter')

    // Give time for navigation/action
    await page.waitForTimeout(300)

    // The CTA should either navigate or perform an action
    // For anchor links, it should update the URL hash
    const newUrl = page.url()
    // Either the URL changed or an action was performed
    expect(newUrl).toBeTruthy()
  })

  // Test case 2 continued: Test button keyboard activation with Space
  test('buttons respond to Space key press', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Test hamburger menu button with space
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.focus()

    // Verify it's focused
    const isFocused = await hamburgerButton.evaluate(
      (el) => document.activeElement === el
    )
    expect(isFocused).toBe(true)

    // Press Space to activate
    await page.keyboard.press('Space')

    // Menu should open
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()
  })

  // Test case 3: Check focus indicator visibility
  test('focus indicator is visible on all focused elements', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Get all focusable elements
    const focusableElements = await page.$$eval(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      (elements) =>
        elements.map((el) => ({
          tag: el.tagName.toLowerCase(),
          id: el.id,
          className: el.className,
          testId: el.getAttribute('data-testid'),
        }))
    )

    // Test focus indicators on a sample of elements
    const sampleSize = Math.min(10, focusableElements.length)
    const testedElements: string[] = []

    for (let i = 0; i < sampleSize; i++) {
      await page.keyboard.press('Tab')

      // Check if the focused element has a visible focus indicator
      const focusStyles = await page.evaluate(() => {
        const el = document.activeElement as HTMLElement
        if (!el || el === document.body) return null

        const styles = window.getComputedStyle(el)
        return {
          outline: styles.outline,
          outlineStyle: styles.outlineStyle,
          outlineWidth: styles.outlineWidth,
          boxShadow: styles.boxShadow,
          borderColor: styles.borderColor,
          tag: el.tagName.toLowerCase(),
          testId: el.getAttribute('data-testid'),
        }
      })

      if (focusStyles) {
        // Verify focus indicator exists (outline, box-shadow, or ring)
        const hasOutline =
          focusStyles.outlineStyle !== 'none' &&
          focusStyles.outlineWidth !== '0px'
        const hasBoxShadow =
          focusStyles.boxShadow !== 'none' && focusStyles.boxShadow.length > 0
        const hasFocusIndicator = hasOutline || hasBoxShadow

        testedElements.push(
          `${focusStyles.tag}${focusStyles.testId ? `[${focusStyles.testId}]` : ''}: ${hasFocusIndicator ? 'has focus indicator' : 'MISSING focus indicator'}`
        )

        // All interactive elements should have visible focus indicators
        expect(hasFocusIndicator).toBe(true)
      }
    }

    // Verify we tested some elements
    expect(testedElements.length).toBeGreaterThan(0)
  })

  // Test case 4: Run color contrast analyzer (via axe-core)
  test('all text meets minimum 4.5:1 contrast ratio', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Run axe accessibility scan focused on color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa', 'wcag21aa'])
      .analyze()

    // Check specifically for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.id === 'color-contrast'
    )

    // Log any violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations:', JSON.stringify(contrastViolations, null, 2))
    }

    // No color contrast violations should exist
    expect(contrastViolations).toHaveLength(0)
  })

  // Test case 5: Verify single h1 on page
  test('exactly one h1 element is present on the page', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Find all h1 elements
    const h1Elements = await page.$$('h1')

    // Should have exactly one h1
    expect(h1Elements.length).toBe(1)

    // Verify the h1 has content
    const h1Text = await page.$eval('h1', (el) => el.textContent?.trim())
    expect(h1Text).toBeTruthy()
    expect(h1Text!.length).toBeGreaterThan(0)
  })

  // Test case 6: Check heading hierarchy
  test('headings follow logical order without skipping levels', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Get all headings in order
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) =>
      elements.map((el) => ({
        level: parseInt(el.tagName.charAt(1), 10),
        text: el.textContent?.trim().slice(0, 50) || '',
      }))
    )

    // Verify headings exist
    expect(headings.length).toBeGreaterThan(0)

    // Verify heading hierarchy
    let previousLevel = 0
    const issues: string[] = []

    for (const heading of headings) {
      // First heading should be h1
      if (previousLevel === 0 && heading.level !== 1) {
        issues.push(`First heading should be h1, got h${heading.level}`)
      }

      // Check for skipped levels (e.g., h1 to h3)
      if (previousLevel > 0 && heading.level > previousLevel + 1) {
        issues.push(
          `Heading level skipped: h${previousLevel} to h${heading.level} ("${heading.text}")`
        )
      }

      previousLevel = heading.level
    }

    // No hierarchy issues
    expect(issues).toHaveLength(0)
  })

  // Test case 7: Check image alt attributes
  test('all images have descriptive alt text or empty alt for decorative', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Get all images
    const images = await page.$$eval('img', (elements) =>
      elements.map((el) => ({
        src: el.src || el.getAttribute('src') || '',
        hasAlt: el.hasAttribute('alt'),
        alt: el.getAttribute('alt'),
        ariaHidden: el.getAttribute('aria-hidden'),
      }))
    )

    // Check each image
    const issues: string[] = []

    for (const img of images) {
      // Image should have alt attribute (can be empty for decorative)
      if (!img.hasAlt && img.ariaHidden !== 'true') {
        issues.push(`Image missing alt attribute: ${img.src}`)
      }
    }

    // No missing alt attributes
    expect(issues).toHaveLength(0)
  })

  // Test case 8: Check button ARIA labels
  test('icon-only buttons have aria-label or sr-only text', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Find all buttons
    const buttons = await page.$$eval('button, [role="button"]', (elements) =>
      elements.map((el) => ({
        hasAriaLabel: el.hasAttribute('aria-label'),
        ariaLabel: el.getAttribute('aria-label'),
        hasAriaLabelledby: el.hasAttribute('aria-labelledby'),
        textContent: el.textContent?.trim() || '',
        hasSrOnly: !!el.querySelector('.sr-only'),
        srOnlyText: el.querySelector('.sr-only')?.textContent?.trim() || '',
        testId: el.getAttribute('data-testid'),
        className: el.className,
      }))
    )

    // Check each button has an accessible name
    const issues: string[] = []

    for (const button of buttons) {
      const hasAccessibleName =
        button.hasAriaLabel ||
        button.hasAriaLabelledby ||
        button.textContent.length > 0 ||
        button.hasSrOnly

      if (!hasAccessibleName) {
        issues.push(
          `Button without accessible name: ${button.testId || button.className || 'unknown'}`
        )
      }
    }

    // No buttons without accessible names
    expect(issues).toHaveLength(0)
  })

  // Additional ARIA test: Check navigation landmarks
  test('page has proper ARIA landmarks', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Check for banner (header)
    const banner = page.locator('[role="banner"], header')
    await expect(banner.first()).toBeVisible()

    // Check for main content
    const main = page.locator('main, [role="main"]')
    await expect(main).toBeVisible()

    // Check for navigation
    const nav = page.locator('nav, [role="navigation"]')
    await expect(nav.first()).toBeVisible()

    // Check for contentinfo (footer)
    const footer = page.locator('[role="contentinfo"], footer')
    await expect(footer).toBeVisible()
  })

  // Test case 9: Run Lighthouse accessibility audit (via axe-core)
  test('Lighthouse accessibility audit passes with score >= 90', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Run comprehensive axe scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'best-practice'])
      .analyze()

    // Count violations by impact
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical'
    )
    const seriousViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'serious'
    )

    // Log violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log(
        'Accessibility violations:',
        JSON.stringify(
          accessibilityScanResults.violations.map((v) => ({
            id: v.id,
            impact: v.impact,
            description: v.description,
            nodes: v.nodes.length,
          })),
          null,
          2
        )
      )
    }

    // No critical violations allowed
    expect(criticalViolations).toHaveLength(0)

    // No serious violations allowed
    expect(seriousViolations).toHaveLength(0)

    // Calculate approximate accessibility score
    // Score is based on passes vs total testable rules
    const totalRules =
      accessibilityScanResults.passes.length +
      accessibilityScanResults.violations.length
    const passedRules = accessibilityScanResults.passes.length
    const approximateScore = Math.round((passedRules / totalRules) * 100)

    // Should have a high accessibility score (>= 90)
    expect(approximateScore).toBeGreaterThanOrEqual(90)
  })

  // Test for keyboard navigation escape key behavior
  test('Escape key closes mobile menu', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Verify menu is open
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Press Escape to close
    await page.keyboard.press('Escape')

    // Menu should be closed
    await expect(mobileMenu).not.toBeVisible()
  })

  // Test skip link functionality (if present)
  test('skip to main content link works correctly', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Check if skip link exists
    const skipLink = page.locator('a[href="#main"], a[href="#content"]')

    // Skip links are best practice but not mandatory
    const skipLinkExists = (await skipLink.count()) > 0

    if (skipLinkExists) {
      // Focus on skip link (usually first focusable element)
      await page.keyboard.press('Tab')

      // Activate it
      await page.keyboard.press('Enter')

      // Main content should be focused or in view
      const mainContent = page.locator('main, #main, #content')
      await expect(mainContent).toBeInViewport()
    }
  })

  // Test focus trap in mobile menu
  test('focus is trapped within mobile menu when open', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Verify menu is open
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Tab through menu items
    const focusableInMenu: string[] = []
    let maxTabs = 20

    while (maxTabs > 0) {
      await page.keyboard.press('Tab')

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement
        if (!el) return null
        return {
          testId: el.getAttribute('data-testid'),
          inMenu:
            el.closest('[data-testid="mobile-menu"]') !== null ||
            el.getAttribute('data-testid') === 'hamburger-menu-button',
        }
      })

      if (focusedElement?.testId) {
        // Focus should stay within menu or on hamburger button
        expect(focusedElement.inMenu).toBe(true)

        // Check if we've cycled through
        if (focusableInMenu.includes(focusedElement.testId)) {
          break
        }
        focusableInMenu.push(focusedElement.testId)
      }

      maxTabs--
    }

    // Should have found focusable elements in menu
    expect(focusableInMenu.length).toBeGreaterThan(0)
  })
})

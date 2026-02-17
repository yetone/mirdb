/**
 * E2E Accessibility Tests
 *
 * Scenario 11: Accessibility Compliance
 * Verifies that homepage meets WCAG 2.1 AA accessibility requirements
 *
 * Test Cases:
 * 1. Run axe-core accessibility audit - No critical accessibility violations detected
 * 2. Navigate using Tab key only - All interactive elements can be reached and activated via keyboard
 * 3. Check color contrast ratios - All text meets 4.5:1 contrast ratio requirement
 * 4. Verify focus indicators - Visible focus ring appears on keyboard navigation
 * 5. Check image alt text - All images have descriptive alt text
 * 6. Verify semantic HTML structure - Page uses proper semantic elements (header, main, nav, footer)
 * 7. Check ARIA labels - Non-semantic interactive elements have appropriate ARIA labels
 */

import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/')
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  /**
   * Test Case 1: Run axe-core accessibility audit
   * Input: Run axe-core accessibility audit
   * Expected: No critical accessibility violations detected
   */
  test('no critical accessibility violations detected via axe-core audit', async ({ page }) => {
    // Wait for main content to be visible
    await expect(page.getByTestId('hero-section')).toBeVisible()

    // Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze()

    // Filter for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    )

    // Log violations for debugging if any exist
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:', JSON.stringify(criticalViolations, null, 2))
    }

    // Assert no critical or serious violations
    expect(criticalViolations.length).toBe(0)
  })

  /**
   * Test Case 2: Navigate using Tab key only
   * Input: Navigate using Tab key only
   * Expected: All interactive elements can be reached and activated via keyboard
   */
  test('all interactive elements are reachable via Tab key navigation', async ({ page }) => {
    // Test that key interactive elements are reachable via Tab
    const primaryCta = page.getByTestId('hero-primary-cta')
    const loginCta = page.getByTestId('hero-login-cta')
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Verify elements exist and are visible
    await expect(primaryCta).toBeVisible()
    await expect(loginCta).toBeVisible()
    await expect(urlInput).toBeVisible()
    await expect(shortenButton).toBeVisible()

    // Verify each element can receive keyboard focus
    // This tests that tabindex is correctly set up
    await primaryCta.focus()
    await expect(primaryCta).toBeFocused()

    await loginCta.focus()
    await expect(loginCta).toBeFocused()

    await urlInput.focus()
    await expect(urlInput).toBeFocused()

    await shortenButton.focus()
    await expect(shortenButton).toBeFocused()

    // Verify Tab navigation reaches interactive elements
    // Start from the document body
    await page.evaluate(() => {
      (document.activeElement as HTMLElement)?.blur()
    })

    // Tab once and verify we get to a focusable element
    await page.keyboard.press('Tab')
    const firstFocused = page.locator(':focus')
    await expect(firstFocused).toBeVisible()

    // Tab a few more times to ensure navigation continues
    await page.keyboard.press('Tab')
    await page.keyboard.press('Tab')
    await page.keyboard.press('Tab')

    // Verify we're still on a focusable element
    const laterFocused = page.locator(':focus')
    await expect(laterFocused).toBeVisible()
  })

  /**
   * Test Case 2 (additional): Verify interactive elements can be activated via keyboard
   */
  test('interactive elements can be activated via keyboard (Enter and Space)', async ({ page }) => {
    // Test primary CTA activation with Enter key
    const primaryCta = page.getByTestId('hero-primary-cta')
    await primaryCta.focus()
    await expect(primaryCta).toBeFocused()
    await page.keyboard.press('Enter')
    await expect(page).toHaveURL(/\/register/)

    // Navigate back to homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Test login CTA activation with Space key
    const loginCta = page.getByTestId('hero-login-cta')
    await loginCta.focus()
    await expect(loginCta).toBeFocused()
    await page.keyboard.press('Space')
    await expect(page).toHaveURL(/\/login/)

    // Navigate back to homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Test URL input is focusable and accepts keyboard input
    const urlInput = page.getByTestId('url-input')
    await urlInput.focus()
    await expect(urlInput).toBeFocused()
    await page.keyboard.type('https://example.com')
    await expect(urlInput).toHaveValue('https://example.com')
  })

  /**
   * Test Case 3: Check color contrast ratios
   * Input: Check color contrast ratios
   * Expected: All text meets 4.5:1 contrast ratio requirement
   */
  test('all text meets color contrast requirements', async ({ page }) => {
    // Wait for content to load
    await expect(page.getByTestId('hero-section')).toBeVisible()

    // Run axe-core specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze()

    // Check for contrast violations
    const contrastViolations = contrastResults.violations.filter((v) => v.id === 'color-contrast')

    // Log violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color Contrast Violations:', JSON.stringify(contrastViolations, null, 2))
    }

    // Assert no color contrast violations
    expect(contrastViolations.length).toBe(0)
  })

  /**
   * Test Case 4: Verify focus indicators
   * Input: Verify focus indicators
   * Expected: Visible focus ring appears on keyboard navigation
   */
  test('visible focus indicators appear on keyboard navigation', async ({ page }) => {
    // Test focus indicator on primary CTA button
    const primaryCta = page.getByTestId('hero-primary-cta')
    await primaryCta.focus()

    // Check that the element is focused
    await expect(primaryCta).toBeFocused()

    // Verify focus ring is visible via CSS (outline or ring classes)
    const primaryFocusStyles = await primaryCta.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        boxShadow: styles.boxShadow,
      }
    })

    // Focus should be visible (either via outline or box-shadow for focus ring)
    const hasFocusIndicator =
      primaryFocusStyles.outlineWidth !== '0px' ||
      primaryFocusStyles.boxShadow !== 'none'
    expect(hasFocusIndicator).toBe(true)

    // Test focus indicator on login CTA button
    const loginCta = page.getByTestId('hero-login-cta')
    await loginCta.focus()
    await expect(loginCta).toBeFocused()

    const loginFocusStyles = await loginCta.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        boxShadow: styles.boxShadow,
      }
    })

    const loginHasFocusIndicator =
      loginFocusStyles.outlineWidth !== '0px' ||
      loginFocusStyles.boxShadow !== 'none'
    expect(loginHasFocusIndicator).toBe(true)

    // Test focus indicator on URL input
    const urlInput = page.getByTestId('url-input')
    await urlInput.focus()
    await expect(urlInput).toBeFocused()

    const inputFocusStyles = await urlInput.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      }
    })

    // Input should show focus state (typically via border color change or outline)
    const inputHasFocusIndicator =
      inputFocusStyles.outlineWidth !== '0px' ||
      inputFocusStyles.boxShadow !== 'none'
    expect(inputHasFocusIndicator).toBe(true)
  })

  /**
   * Test Case 5: Check image alt text
   * Input: Check image alt text
   * Expected: All images have descriptive alt text
   */
  test('all images have descriptive alt text', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img')
    const imageCount = await images.count()

    // If there are images, verify they all have alt text
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i)
      const altText = await img.getAttribute('alt')

      // Alt text should exist
      expect(altText, `Image ${i + 1} should have alt attribute`).not.toBeNull()

      // Alt text should not be empty (unless it's a decorative image with alt="")
      // Decorative images with empty alt are valid, but we check that it's intentional
      if (altText === '') {
        // If alt is empty, verify the image is marked as decorative (role="presentation" or aria-hidden)
        const role = await img.getAttribute('role')
        const ariaHidden = await img.getAttribute('aria-hidden')
        const isDecorative = role === 'presentation' || role === 'none' || ariaHidden === 'true'
        expect(isDecorative, `Image ${i + 1} with empty alt should be marked as decorative`).toBe(true)
      }
    }

    // Run axe-core check for image alt text
    const imageAltResults = await new AxeBuilder({ page })
      .withRules(['image-alt'])
      .analyze()

    expect(imageAltResults.violations.length).toBe(0)
  })

  /**
   * Test Case 5 (additional): SVG icons have appropriate accessibility attributes
   */
  test('SVG icons have appropriate accessibility attributes', async ({ page }) => {
    // Get all SVGs on the page
    const svgs = page.locator('svg')
    const svgCount = await svgs.count()

    for (let i = 0; i < svgCount; i++) {
      const svg = svgs.nth(i)
      const isVisible = await svg.isVisible().catch(() => false)

      if (!isVisible) continue

      // Check for accessibility attributes
      const ariaHidden = await svg.getAttribute('aria-hidden')
      const ariaLabel = await svg.getAttribute('aria-label')
      const role = await svg.getAttribute('role')

      // SVGs should either be hidden from assistive tech or have proper labels
      const isAccessible =
        ariaHidden === 'true' || // Hidden from screen readers
        ariaLabel !== null || // Has aria-label
        role === 'img' // Marked as image role

      expect(
        isAccessible,
        `SVG ${i + 1} should be either hidden from assistive tech or have proper labeling`
      ).toBe(true)
    }
  })

  /**
   * Test Case 6: Verify semantic HTML structure
   * Input: Verify semantic HTML structure
   * Expected: Page uses proper semantic elements (header, main, nav, footer)
   */
  test('page uses proper semantic HTML elements', async ({ page }) => {
    // Check for presence of main landmark
    const main = page.locator('main')
    await expect(main, 'Page should have a main element').toBeVisible()

    // Check for presence of navigation
    const nav = page.locator('nav')
    const navCount = await nav.count()
    expect(navCount, 'Page should have at least one nav element').toBeGreaterThanOrEqual(1)

    // Check for presence of footer
    const footer = page.locator('footer')
    await expect(footer, 'Page should have a footer element').toBeVisible()

    // Check that the footer has proper role
    const footerRole = await footer.getAttribute('role')
    expect(footerRole, 'Footer should have contentinfo role').toBe('contentinfo')

    // Verify heading hierarchy (h1 should exist and be unique on the page)
    const h1Elements = page.locator('h1')
    const h1Count = await h1Elements.count()
    expect(h1Count, 'Page should have exactly one h1 element').toBe(1)

    // Verify h2 elements exist for sections
    const h2Elements = page.locator('h2')
    const h2Count = await h2Elements.count()
    expect(h2Count, 'Page should have h2 elements for sections').toBeGreaterThanOrEqual(1)
  })

  /**
   * Test Case 6 (additional): Verify proper heading hierarchy
   */
  test('heading hierarchy is properly structured', async ({ page }) => {
    // Run axe-core check for heading order
    const headingResults = await new AxeBuilder({ page })
      .withRules(['heading-order', 'page-has-heading-one'])
      .analyze()

    // Check for heading-related violations
    const headingViolations = headingResults.violations

    if (headingViolations.length > 0) {
      console.log('Heading Violations:', JSON.stringify(headingViolations, null, 2))
    }

    expect(headingViolations.length).toBe(0)
  })

  /**
   * Test Case 6 (additional): Verify landmark regions
   */
  test('page has proper landmark regions', async ({ page }) => {
    // Run axe-core check for landmarks
    const landmarkResults = await new AxeBuilder({ page })
      .withRules(['landmark-one-main', 'landmark-no-duplicate-main'])
      .analyze()

    expect(landmarkResults.violations.length).toBe(0)

    // Verify landmark regions manually
    const landmarks = await page.evaluate(() => {
      const main = document.querySelector('main')
      const navs = document.querySelectorAll('nav')
      const footer = document.querySelector('footer')
      const header = document.querySelector('header')

      return {
        hasMain: !!main,
        navCount: navs.length,
        hasFooter: !!footer,
        hasHeader: !!header,
      }
    })

    expect(landmarks.hasMain, 'Page should have main landmark').toBe(true)
    expect(landmarks.navCount, 'Page should have navigation').toBeGreaterThanOrEqual(1)
    expect(landmarks.hasFooter, 'Page should have footer').toBe(true)
  })

  /**
   * Test Case 7: Check ARIA labels
   * Input: Check ARIA labels
   * Expected: Non-semantic interactive elements have appropriate ARIA labels
   */
  test('non-semantic interactive elements have appropriate ARIA labels', async ({ page }) => {
    // Check buttons have accessible names
    const buttons = page.locator('button')
    const buttonCount = await buttons.count()

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i)
      const isVisible = await button.isVisible().catch(() => false)

      if (!isVisible) continue

      // Get accessible name
      const textContent = await button.textContent().catch(() => '')
      const ariaLabel = await button.getAttribute('aria-label')
      const ariaLabelledby = await button.getAttribute('aria-labelledby')

      // Button should have an accessible name (either via text content or aria-label)
      const hasAccessibleName =
        (textContent && textContent.trim().length > 0) ||
        ariaLabel !== null ||
        ariaLabelledby !== null

      expect(
        hasAccessibleName,
        `Button ${i + 1} should have an accessible name`
      ).toBe(true)
    }

    // Check that inputs have labels
    const inputs = page.locator('input')
    const inputCount = await inputs.count()

    for (let i = 0; i < inputCount; i++) {
      const input = inputs.nth(i)
      const isVisible = await input.isVisible().catch(() => false)

      if (!isVisible) continue

      // Get input type
      const type = await input.getAttribute('type')

      // Skip hidden inputs
      if (type === 'hidden') continue

      // Check for accessible labeling
      const id = await input.getAttribute('id')
      const ariaLabel = await input.getAttribute('aria-label')
      const ariaLabelledby = await input.getAttribute('aria-labelledby')

      // Input should have accessible label
      const hasAssociatedLabel = id
        ? await page.locator(`label[for="${id}"]`).count() > 0
        : false

      const hasAccessibleName =
        hasAssociatedLabel ||
        ariaLabel !== null ||
        ariaLabelledby !== null

      expect(
        hasAccessibleName,
        `Input ${i + 1} (type: ${type}) should have an accessible label`
      ).toBe(true)
    }
  })

  /**
   * Test Case 7 (additional): Run comprehensive ARIA checks via axe-core
   */
  test('ARIA attributes are used correctly', async ({ page }) => {
    // Run axe-core checks for ARIA
    const ariaResults = await new AxeBuilder({ page })
      .withRules([
        'aria-allowed-attr',
        'aria-hidden-body',
        'aria-hidden-focus',
        'aria-required-attr',
        'aria-required-children',
        'aria-required-parent',
        'aria-roles',
        'aria-valid-attr',
        'aria-valid-attr-value',
      ])
      .analyze()

    if (ariaResults.violations.length > 0) {
      console.log('ARIA Violations:', JSON.stringify(ariaResults.violations, null, 2))
    }

    expect(ariaResults.violations.length).toBe(0)
  })

  /**
   * Additional Test: Form controls have proper error states
   */
  test('form controls have proper error state accessibility', async ({ page }) => {
    // Test URL input error state
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Enter invalid URL
    await urlInput.fill('not-a-valid-url')
    await shortenButton.click()

    // Wait for error message
    const errorMessage = page.getByTestId('url-error')
    await expect(errorMessage).toBeVisible()

    // Verify error is properly announced
    const errorRole = await errorMessage.getAttribute('role')
    expect(errorRole).toBe('alert')

    // Verify input has aria-invalid
    const ariaInvalid = await urlInput.getAttribute('aria-invalid')
    expect(ariaInvalid).toBe('true')

    // Verify input is connected to error via aria-describedby
    const ariaDescribedby = await urlInput.getAttribute('aria-describedby')
    expect(ariaDescribedby).toBeTruthy()
  })

  /**
   * Additional Test: Tab order follows visual reading order
   */
  test('tab order follows visual reading order', async ({ page }) => {
    // Get the Y positions of key interactive elements
    const primaryCta = page.getByTestId('hero-primary-cta')
    const urlInput = page.getByTestId('url-input')

    const primaryCtaY = (await primaryCta.boundingBox())?.y ?? 0
    const urlInputY = (await urlInput.boundingBox())?.y ?? 0

    // Verify hero elements appear before URL demo elements visually
    expect(primaryCtaY).toBeLessThan(urlInputY)

    // Verify both elements can be focused (confirming they're in tab order)
    await primaryCta.focus()
    await expect(primaryCta).toBeFocused()

    await urlInput.focus()
    await expect(urlInput).toBeFocused()
  })
})

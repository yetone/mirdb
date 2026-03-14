/**
 * E2E tests for User Interaction Patterns.
 * Owner: Scenario 11 - User Interaction Patterns
 *
 * Test cases:
 * - TC1: Navigation links hover effect (underline, color change)
 * - TC2: Button hover effects (color, shadow, scale)
 * - TC3: Smooth scroll to anchor sections
 * - TC4: Button active/pressed states
 * - TC5: CSS scroll-behavior property (unit test via computed style)
 * - TC6: Feature card hover animation without layout shifts
 * - TC7: Reduced motion preference respected
 */

import { test, expect } from '@playwright/test'

test.describe('User Interaction Patterns E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  // Test Case 1: Navigation links display visible hover effect
  test('TC1: Navigation links display visible hover effect on hover', async ({
    page,
  }) => {
    // Get the main navigation on desktop
    await page.setViewportSize({ width: 1440, height: 900 })

    // Find navigation links (they are in the header nav)
    const navLinks = page.locator('header nav ul li a')
    const firstNavLink = navLinks.first()

    await expect(firstNavLink).toBeVisible()

    // Get initial styles before hover
    const initialStyles = await firstNavLink.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        color: styles.color,
        textDecoration: styles.textDecoration,
        textDecorationLine: styles.textDecorationLine,
      }
    })

    // Hover over the navigation link
    await firstNavLink.hover()
    await page.waitForTimeout(250) // Wait for transition

    // Get styles after hover
    const hoverStyles = await firstNavLink.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        color: styles.color,
        textDecoration: styles.textDecoration,
        textDecorationLine: styles.textDecorationLine,
      }
    })

    // Verify hover effect is visible (color should change)
    const hoverEffectVisible =
      initialStyles.color !== hoverStyles.color ||
      initialStyles.textDecoration !== hoverStyles.textDecoration ||
      initialStyles.textDecorationLine !== hoverStyles.textDecorationLine

    expect(hoverEffectVisible).toBeTruthy()
  })

  // Test Case 2: Buttons display visible hover effect
  test('TC2: Buttons display visible hover effect on hover', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Find a CTA button
    const ctaButton = page.getByTestId('cta-primary-button')
    await expect(ctaButton).toBeVisible()

    // Get initial styles before hover
    const initialStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      }
    })

    // Hover over the button
    await ctaButton.hover()
    await page.waitForTimeout(250) // Wait for transition

    // Get styles after hover
    const hoverStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      }
    })

    // Verify hover effect is visible (at least one property should change)
    const hoverEffectVisible =
      initialStyles.backgroundColor !== hoverStyles.backgroundColor ||
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.boxShadow !== hoverStyles.boxShadow

    expect(hoverEffectVisible).toBeTruthy()
  })

  // Test Case 3: Anchor link navigation uses smooth scroll
  test('TC3: Clicking anchor link smoothly scrolls to target section', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Record initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)
    expect(initialScrollY).toBe(0) // Should start at top

    // Find and click the Learn More button (links to #features)
    // Use the specific CTA secondary button with data-testid
    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await expect(secondaryCTA).toBeVisible()
    await secondaryCTA.click()

    // Wait a bit for scroll animation to complete
    await page.waitForTimeout(500)

    // Check that the page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)

    // Verify URL includes the hash
    await expect(page).toHaveURL(/#features/)
  })

  // Test Case 4: Button displays active/pressed state when clicked
  test('TC4: Button displays active/pressed state when clicked', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Find a button to test
    const ctaButton = page.getByTestId('cta-primary-button')
    await expect(ctaButton).toBeVisible()

    // Get initial styles
    const initialStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      }
    })

    // Use mouse.down() to simulate press and hold
    await ctaButton.hover()
    const box = await ctaButton.boundingBox()
    if (box) {
      await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2)
      await page.mouse.down()

      // Wait briefly for the active state to apply
      await page.waitForTimeout(100)

      // Get styles while button is pressed
      const activeStyles = await ctaButton.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return {
          backgroundColor: styles.backgroundColor,
          transform: styles.transform,
          boxShadow: styles.boxShadow,
        }
      })

      await page.mouse.up()

      // The button should have some visual change when pressed
      // (typically darker background or scale change)
      const activeStateVisible =
        initialStyles.backgroundColor !== activeStyles.backgroundColor ||
        initialStyles.transform !== activeStyles.transform ||
        initialStyles.boxShadow !== activeStyles.boxShadow

      expect(activeStateVisible).toBeTruthy()
    }
  })

  // Test Case 5: CSS scroll-behavior: smooth applied to html element
  test('TC5: scroll-behavior: smooth is applied to html element', async ({
    page,
  }) => {
    // Get the computed style of the html element
    const scrollBehavior = await page.evaluate(() => {
      const htmlElement = document.documentElement
      return window.getComputedStyle(htmlElement).scrollBehavior
    })

    expect(scrollBehavior).toBe('smooth')
  })

  // Test Case 6: Feature card hover animation is smooth without layout shifts
  test('TC6: Feature card hover animation is smooth without layout shifts', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(300)

    // Find a feature card
    const featureCard = page.getByTestId('feature-card').first()
    await expect(featureCard).toBeVisible()

    // Get initial bounding box and styles
    const initialBox = await featureCard.boundingBox()
    const initialStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        width: styles.width,
        height: styles.height,
      }
    })

    // Hover over the feature card
    await featureCard.hover()
    await page.waitForTimeout(350) // Wait for transition (300ms + buffer)

    // Get final bounding box and styles
    const hoverBox = await featureCard.boundingBox()
    const hoverStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        width: styles.width,
        height: styles.height,
      }
    })

    // Animation should change transform or shadow (visual effect)
    const animationApplied =
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.boxShadow !== hoverStyles.boxShadow

    expect(animationApplied).toBeTruthy()

    // No layout shift: width and height should remain the same
    // (only transform-based animations should be used)
    expect(initialStyles.width).toBe(hoverStyles.width)
    expect(initialStyles.height).toBe(hoverStyles.height)

    // The position shift should be minimal (transform-based, not layout-based)
    // Allow small difference due to transform: scale()
    if (initialBox && hoverBox) {
      const widthDiff = Math.abs(hoverBox.width - initialBox.width)
      const heightDiff = Math.abs(hoverBox.height - initialBox.height)

      // Width/height changes should be small (from scale effect only)
      expect(widthDiff).toBeLessThan(20)
      expect(heightDiff).toBeLessThan(20)
    }
  })

  // Test Case 7: Animations respect prefers-reduced-motion media query
  test('TC7: Animations respect prefers-reduced-motion media query', async ({
    page,
  }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' })
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Check that scroll-behavior is set to auto (not smooth)
    const scrollBehavior = await page.evaluate(() => {
      const htmlElement = document.documentElement
      return window.getComputedStyle(htmlElement).scrollBehavior
    })

    expect(scrollBehavior).toBe('auto')

    // Check that transitions are reduced/disabled
    const transitionDuration = await page.evaluate(() => {
      const button = document.querySelector('button')
      if (!button) return '0s'
      return window.getComputedStyle(button).transitionDuration
    })

    // Transition duration should be very short (0.01ms = 0.00001s or 0s)
    const durationMs = parseFloat(transitionDuration) * 1000
    expect(durationMs).toBeLessThanOrEqual(1) // 1ms or less
  })

  // Additional test: Verify multiple nav links have hover states
  test('Multiple navigation links have consistent hover effects', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    const navLinks = page.locator('header nav ul li a')
    const count = await navLinks.count()

    expect(count).toBeGreaterThan(0)

    // Test first and last nav links for consistency
    for (const link of [navLinks.first(), navLinks.last()]) {
      const initialColor = await link.evaluate(
        (el) => window.getComputedStyle(el).color
      )

      await link.hover()
      await page.waitForTimeout(250)

      const hoverColor = await link.evaluate(
        (el) => window.getComputedStyle(el).color
      )

      // Color should change on hover
      expect(initialColor).not.toBe(hoverColor)
    }
  })

  // Additional test: Focus states are visible
  test('Interactive elements have visible focus states', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Tab to the first interactive element
    await page.keyboard.press('Tab')
    await page.waitForTimeout(100)

    // Get the focused element
    const focusedElement = page.locator(':focus')
    const focusStyles = await focusedElement.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        outlineWidth: styles.outlineWidth,
      }
    })

    // Focus indicator should be visible (ring or outline)
    const focusVisible =
      focusStyles.boxShadow !== 'none' ||
      focusStyles.outline !== 'none' ||
      parseInt(focusStyles.outlineWidth) > 0

    expect(focusVisible).toBeTruthy()
  })

  // Test smooth scroll works with keyboard navigation
  test('Anchor links work with Enter key', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Find the secondary CTA that links to features
    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await secondaryCTA.focus()

    // Press Enter to activate the link
    await page.keyboard.press('Enter')
    await page.waitForTimeout(500)

    // Page should have scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)
  })
})

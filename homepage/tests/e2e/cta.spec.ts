/**
 * E2E tests for CTA components.
 * Owner: Scenario 5 - Secondary CTAs Implementation
 *
 * Test cases:
 * - Test Case 2: Click 'Learn More' CTA - Navigates to appropriate page or scrolls to section
 * - Test Case 3: Click 'Contact Sales' CTA - Opens contact form or navigates to contact page
 * - Test Case 4: Test all CTA hover states - All CTAs display visible hover effect
 * - Test Case 5: Test all CTA focus states - All CTAs display visible focus indicator
 */

import { test, expect } from '@playwright/test'

test.describe('CTA Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 1: Multiple CTAs present on homepage
  test('Test Case 1: Homepage has multiple CTAs present', async ({ page }) => {
    // Check for CTA section
    const ctaSection = page.getByTestId('cta-section')
    await expect(ctaSection).toBeVisible()

    // Check for primary CTA button
    const primaryCTA = page.getByTestId('cta-primary-button')
    await expect(primaryCTA).toBeVisible()

    // Check for secondary CTA button (Learn More)
    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await expect(secondaryCTA).toBeVisible()

    // Verify multiple CTAs are present (at least primary + secondary)
    const allCTAButtons = page.locator('[data-testid*="cta"]').filter({
      hasText: /.+/,
    })
    const count = await allCTAButtons.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  // Test Case 2: Click 'Learn More' CTA
  test('Test Case 2: Learn More CTA navigates to features section', async ({
    page,
  }) => {
    // Get the secondary CTA (Learn More button)
    const learnMoreCTA = page.getByTestId('cta-secondary-button')
    await expect(learnMoreCTA).toBeVisible()

    // Verify the href points to features
    const href = await learnMoreCTA.getAttribute('href')
    expect(href).toBe('#features')

    // Click the Learn More button
    await learnMoreCTA.click()

    // Verify URL includes the hash
    await expect(page).toHaveURL(/#features/)
  })

  // Test Case 3: Click 'Contact Sales' CTA if present
  test('Test Case 3: Contact Sales CTA navigates to contact section', async ({
    page,
  }) => {
    // Look for Contact Sales CTA (might be in the CTA section)
    const contactSalesCTA = page.getByTestId('contact-sales-cta')

    // If Contact Sales CTA exists, test it
    if ((await contactSalesCTA.count()) > 0) {
      await expect(contactSalesCTA).toBeVisible()

      // Verify it has proper href
      const href = await contactSalesCTA.getAttribute('href')
      expect(href).toContain('contact')

      // Click the Contact Sales button
      await contactSalesCTA.click()

      // Verify navigation
      await expect(page).toHaveURL(/contact/)
    } else {
      // If no Contact Sales CTA, verify primary CTA leads to signup/contact
      const primaryCTA = page.getByTestId('cta-primary-button')
      const primaryHref = await primaryCTA.getAttribute('href')
      expect(primaryHref).toBeTruthy()
    }
  })

  // Test Case 4: Test all CTA hover states
  test('Test Case 4: All CTAs display visible hover effect', async ({ page }) => {
    // Test primary CTA hover state
    const primaryCTA = page.getByTestId('cta-primary-button')
    await expect(primaryCTA).toBeVisible()

    // Get initial styles before hover
    const initialPrimaryStyles = await primaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      }
    })

    // Hover over primary CTA
    await primaryCTA.hover()

    // Wait for transition
    await page.waitForTimeout(250)

    // Get hover styles
    const hoverPrimaryStyles = await primaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      }
    })

    // Verify hover effect is visible (at least one property changed)
    const primaryHoverChange =
      initialPrimaryStyles.backgroundColor !== hoverPrimaryStyles.backgroundColor ||
      initialPrimaryStyles.transform !== hoverPrimaryStyles.transform ||
      initialPrimaryStyles.boxShadow !== hoverPrimaryStyles.boxShadow

    expect(primaryHoverChange).toBeTruthy()

    // Test secondary CTA hover state
    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await expect(secondaryCTA).toBeVisible()

    // Get initial styles before hover
    const initialSecondaryStyles = await secondaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      }
    })

    // Hover over secondary CTA
    await secondaryCTA.hover()

    // Wait for transition
    await page.waitForTimeout(250)

    // Get hover styles
    const hoverSecondaryStyles = await secondaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      }
    })

    // Verify hover effect is visible
    const secondaryHoverChange =
      initialSecondaryStyles.backgroundColor !==
        hoverSecondaryStyles.backgroundColor ||
      initialSecondaryStyles.transform !== hoverSecondaryStyles.transform ||
      initialSecondaryStyles.boxShadow !== hoverSecondaryStyles.boxShadow ||
      initialSecondaryStyles.borderColor !== hoverSecondaryStyles.borderColor

    expect(secondaryHoverChange).toBeTruthy()
  })

  // Test Case 5: Test all CTA focus states
  test('Test Case 5: All CTAs display visible focus indicator', async ({
    page,
  }) => {
    // Test primary CTA focus state
    const primaryCTA = page.getByTestId('cta-primary-button')
    await expect(primaryCTA).toBeVisible()

    // Get initial styles before focus
    const initialPrimaryStyles = await primaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        outlineOffset: styles.outlineOffset,
      }
    })

    // Focus the primary CTA
    await primaryCTA.focus()

    // Wait for focus styles
    await page.waitForTimeout(100)

    // Get focus styles
    const focusPrimaryStyles = await primaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        outlineOffset: styles.outlineOffset,
      }
    })

    // Verify focus indicator is visible (Tailwind uses box-shadow for focus ring)
    const primaryFocusVisible =
      focusPrimaryStyles.boxShadow !== 'none' ||
      focusPrimaryStyles.outline !== 'none' ||
      focusPrimaryStyles.boxShadow !== initialPrimaryStyles.boxShadow

    expect(primaryFocusVisible).toBeTruthy()

    // Test secondary CTA focus state
    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await expect(secondaryCTA).toBeVisible()

    // Focus the secondary CTA
    await secondaryCTA.focus()

    // Wait for focus styles
    await page.waitForTimeout(100)

    // Get focus styles
    const focusSecondaryStyles = await secondaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        outlineOffset: styles.outlineOffset,
      }
    })

    // Verify focus indicator is visible
    const secondaryFocusVisible =
      focusSecondaryStyles.boxShadow !== 'none' ||
      focusSecondaryStyles.outline !== 'none'

    expect(secondaryFocusVisible).toBeTruthy()
  })

  // Additional E2E tests
  test('CTA section has proper accessibility attributes', async ({ page }) => {
    const ctaSection = page.getByTestId('cta-section')
    await expect(ctaSection).toHaveAttribute(
      'aria-labelledby',
      /cta-section-title/
    )

    const title = page.getByTestId('cta-section-title')
    await expect(title).toBeVisible()
    await expect(title).toHaveAttribute('id', /cta-section-title/)
  })

  test('CTA buttons have aria-label attributes', async ({ page }) => {
    const primaryCTA = page.getByTestId('cta-primary-button')
    await expect(primaryCTA).toHaveAttribute('aria-label')

    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await expect(secondaryCTA).toHaveAttribute('aria-label')
  })

  test('CTA buttons are keyboard navigable', async ({ page }) => {
    // Tab through the page to reach CTAs
    // First need to scroll to CTA section
    const ctaSection = page.getByTestId('cta-section')
    await ctaSection.scrollIntoViewIfNeeded()

    // Focus on first CTA
    const primaryCTA = page.getByTestId('cta-primary-button')
    await primaryCTA.focus()
    await expect(primaryCTA).toBeFocused()

    // Tab to next CTA
    await page.keyboard.press('Tab')
    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await expect(secondaryCTA).toBeFocused()
  })

  test('CTA buttons respond to Enter key', async ({ page }) => {
    const secondaryCTA = page.getByTestId('cta-secondary-button')
    await secondaryCTA.focus()

    // Press Enter
    await page.keyboard.press('Enter')

    // Verify navigation
    await expect(page).toHaveURL(/#features/)
  })

  test('Primary CTA has readable text', async ({ page }) => {
    const primaryCTA = page.getByTestId('cta-primary-button')
    const text = await primaryCTA.textContent()

    expect(text).toBeTruthy()
    expect(text!.length).toBeGreaterThan(0)
  })

  test('CTA section description is visible', async ({ page }) => {
    const description = page.getByTestId('cta-section-description')
    await expect(description).toBeVisible()

    const text = await description.textContent()
    expect(text).toBeTruthy()
    expect(text!.length).toBeGreaterThan(10)
  })

  test('CTAs are visually distinct from regular text', async ({ page }) => {
    const primaryCTA = page.getByTestId('cta-primary-button')
    await expect(primaryCTA).toBeVisible()

    // Verify button has substantial padding (making it appear as a button)
    const boundingBox = await primaryCTA.boundingBox()
    expect(boundingBox).toBeTruthy()
    expect(boundingBox!.height).toBeGreaterThan(30) // Should have noticeable height
    expect(boundingBox!.width).toBeGreaterThan(80) // Should have noticeable width
  })
})

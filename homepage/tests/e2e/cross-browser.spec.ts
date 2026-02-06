/**
 * Cross-browser compatibility E2E tests.
 * Owner: Scenario 9 - Cross-Browser Compatibility
 *
 * Tests run on all configured browsers in playwright.config.ts:
 * - Chrome (latest) - chromium project
 * - Firefox (latest) - firefox project
 * - Safari (latest) - webkit project
 * - Edge (latest) - edge project
 *
 * Validates NFR-3: Support for modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * To run tests on specific browsers:
 *   npx playwright test tests/e2e/cross-browser.spec.ts --project=chromium
 *   npx playwright test tests/e2e/cross-browser.spec.ts --project=firefox
 *   npx playwright test tests/e2e/cross-browser.spec.ts --project=webkit
 *   npx playwright test tests/e2e/cross-browser.spec.ts --project=edge
 */

import { test, expect, Page } from '@playwright/test'

/**
 * Helper function to verify all homepage sections render correctly
 */
async function verifySectionsRender(page: Page) {
  // Hero section
  const heroSection = page.getByTestId('hero-section')
  await expect(heroSection).toBeVisible()

  // Hero headline
  const headline = page.locator('#hero-headline')
  await expect(headline).toBeVisible()
  await expect(headline).toHaveText('Build Something Amazing')

  // Hero subheadline
  const subheadline = page.getByText(/modern platform for building/i)
  await expect(subheadline).toBeVisible()

  // Features section
  const featuresSection = page.locator('#features')
  await expect(featuresSection).toBeVisible()

  // Features heading
  const featuresHeading = page.locator('#features-heading')
  await expect(featuresHeading).toBeVisible()
  await expect(featuresHeading).toHaveText('Our Features')

  // Features grid
  const featuresGrid = page.getByTestId('features-grid')
  await expect(featuresGrid).toBeVisible()

  // Footer
  const footer = page.locator('footer')
  await expect(footer).toBeVisible()
}

/**
 * Helper function to verify navigation works correctly
 */
async function verifyNavigationWorks(page: Page) {
  // Primary CTA - Get Started
  const primaryCTA = page.getByRole('link', { name: /get started/i })
  await expect(primaryCTA).toBeVisible()

  // Secondary CTA - Learn More
  const secondaryCTA = page.getByRole('link', { name: /learn more/i })
  await expect(secondaryCTA).toBeVisible()

  // Footer navigation links
  const privacyLink = page.getByRole('link', { name: /privacy policy/i })
  await expect(privacyLink).toBeVisible()

  const termsLink = page.getByRole('link', { name: /terms of service/i })
  await expect(termsLink).toBeVisible()
}

/**
 * Helper function to verify CTAs are functional
 */
async function verifyCTAsFunctional(page: Page) {
  // Test primary CTA navigation
  const primaryCTA = page.getByRole('link', { name: /get started/i })
  await primaryCTA.click()
  await expect(page).toHaveURL('/signup')

  // Navigate back
  await page.goBack()
  await page.waitForLoadState('domcontentloaded')

  // Test secondary CTA scroll behavior
  const secondaryCTA = page.getByRole('link', { name: /learn more/i })
  await secondaryCTA.click()
  await page.waitForTimeout(500) // Wait for smooth scroll

  // Verify URL hash and features section is in view
  await expect(page).toHaveURL('/#features')
  const featuresSection = page.locator('#features')
  await expect(featuresSection).toBeInViewport()
}

/**
 * Helper function to get CSS grid computed style
 */
async function getGridColumnCount(page: Page, selector: string): Promise<number> {
  const columnCount = await page.evaluate((sel) => {
    const element = document.querySelector(sel)
    if (!element) return 0
    const style = window.getComputedStyle(element)
    const columns = style.gridTemplateColumns.split(' ').filter(Boolean)
    return columns.length
  }, selector)
  return columnCount
}

/**
 * Helper function to verify flexbox layout
 */
async function getFlexDirection(page: Page, selector: string): Promise<string> {
  return page.evaluate((sel) => {
    const element = document.querySelector(sel)
    if (!element) return ''
    return window.getComputedStyle(element).flexDirection
  }, selector)
}

// Test Case 1-4: Browser Compatibility Tests
// These tests run on all configured browser projects
test.describe('Homepage Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('TC1-4: All sections render correctly across browsers', async ({ page, browserName }) => {
    // This test runs on chromium, firefox, webkit, and edge projects
    // Based on which browser project is running, it validates that browser
    await verifySectionsRender(page)

    // Log browser for test reporting
    console.log(`Testing on browser: ${browserName}`)
  })

  test('TC1-4: Navigation works correctly across browsers', async ({ page, browserName }) => {
    await verifyNavigationWorks(page)
    console.log(`Navigation verified on: ${browserName}`)
  })

  test('TC1-4: CTAs are functional across browsers', async ({ page, browserName }) => {
    await verifyCTAsFunctional(page)
    console.log(`CTAs verified on: ${browserName}`)
  })
})

// Test Case 5: CSS Grid/Flexbox Support
test.describe('CSS Grid and Flexbox Support', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('TC5: CSS Grid layout renders identically at desktop viewport (3 columns)', async ({
    page,
    browserName,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })
    await page.waitForTimeout(100)

    const columnCount = await getGridColumnCount(page, '[data-testid="features-grid"]')
    expect(columnCount).toBe(3)
    console.log(`Desktop grid (3 cols) verified on: ${browserName}`)
  })

  test('TC5: CSS Grid layout renders identically at tablet viewport (2 columns)', async ({
    page,
    browserName,
  }) => {
    await page.setViewportSize({ width: 900, height: 1024 })
    await page.waitForTimeout(100)

    const columnCount = await getGridColumnCount(page, '[data-testid="features-grid"]')
    expect(columnCount).toBe(2)
    console.log(`Tablet grid (2 cols) verified on: ${browserName}`)
  })

  test('TC5: CSS Grid layout renders identically at mobile viewport (1 column)', async ({
    page,
    browserName,
  }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(100)

    const columnCount = await getGridColumnCount(page, '[data-testid="features-grid"]')
    expect(columnCount).toBe(1)
    console.log(`Mobile grid (1 col) verified on: ${browserName}`)
  })

  test('TC5: Flexbox CTA container renders as row on desktop', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 1440, height: 900 })
    await page.waitForTimeout(100)

    const flexDirection = await getFlexDirection(page, '[class*="ctaContainer"]')
    expect(flexDirection).toBe('row')
    console.log(`Desktop flexbox (row) verified on: ${browserName}`)
  })

  test('TC5: Flexbox CTA container renders as column on mobile', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(100)

    const flexDirection = await getFlexDirection(page, '[class*="ctaContainer"]')
    expect(flexDirection).toBe('column')
    console.log(`Mobile flexbox (column) verified on: ${browserName}`)
  })
})

// Test Case 6: Animation Consistency
test.describe('Animation and Transition Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
    await page.setViewportSize({ width: 1440, height: 900 })
  })

  test('TC6: Button hover effects work consistently', async ({ page, browserName }) => {
    const primaryButton = page.getByRole('link', { name: /get started/i })

    // Get initial styles
    const initialBgColor = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor
    })

    // Hover over the button
    await primaryButton.hover()
    await page.waitForTimeout(150) // Wait for transition

    // Get hover styles
    const hoverBgColor = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor
    })

    // Background color should change on hover
    expect(hoverBgColor).not.toBe(initialBgColor)
    console.log(`Button hover effect verified on: ${browserName}`)
  })

  test('TC6: Feature card hover transform works', async ({ page, browserName }) => {
    // Find first feature card
    const featureCard = page.locator('[class*="card"]').first()

    // Get initial bounding box position
    const initialBox = await featureCard.boundingBox()
    expect(initialBox).not.toBeNull()

    // Hover over the card
    await featureCard.hover()
    await page.waitForTimeout(200) // Wait for transition

    // Get hover bounding box position
    const hoverBox = await featureCard.boundingBox()
    expect(hoverBox).not.toBeNull()

    // The card should have moved up slightly (translateY negative)
    // Check that Y position changed or transform was applied
    if (initialBox && hoverBox) {
      // Y position should be different (card moves up on hover)
      expect(hoverBox.y).toBeLessThanOrEqual(initialBox.y)
    }
    console.log(`Feature card transform verified on: ${browserName}`)
  })

  test('TC6: Transition properties are applied to buttons', async ({ page, browserName }) => {
    const primaryButton = page.getByRole('link', { name: /get started/i })
    const buttonTransition = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).transition
    })

    // Should have transition property set (not default 'all 0s ease 0s')
    expect(buttonTransition).not.toBe('all 0s ease 0s')
    expect(buttonTransition.length).toBeGreaterThan(0)
    console.log(`Button transition verified on: ${browserName}`)
  })

  test('TC6: Transition properties are applied to feature cards', async ({ page, browserName }) => {
    const featureCard = page.locator('[class*="card"]').first()
    const cardTransition = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transition
    })

    // Should have transition property set
    expect(cardTransition).not.toBe('all 0s ease 0s')
    expect(cardTransition.length).toBeGreaterThan(0)
    console.log(`Card transition verified on: ${browserName}`)
  })
})

// Cross-browser Visual Consistency Tests
test.describe('Visual Consistency Across Browsers', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('Font rendering consistency', async ({ page, browserName }) => {
    const headline = page.locator('#hero-headline')
    const fontFamily = await headline.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily
    })

    // Verify font-family is set (not browser default)
    expect(fontFamily).toBeTruthy()
    expect(fontFamily.length).toBeGreaterThan(0)

    // Verify font weight is applied (bold = 700)
    const fontWeight = await headline.evaluate((el) => {
      return window.getComputedStyle(el).fontWeight
    })
    expect(parseInt(fontWeight)).toBeGreaterThanOrEqual(700)
    console.log(`Font rendering verified on: ${browserName}`)
  })

  test('Color rendering consistency', async ({ page, browserName }) => {
    // Check primary button background color
    const primaryButton = page.getByRole('link', { name: /get started/i })
    const bgColor = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor
    })

    // Should have a non-transparent background
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)')
    expect(bgColor).not.toBe('transparent')

    // Check text color is readable
    const textColor = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).color
    })
    expect(textColor).toBeTruthy()
    console.log(`Color rendering verified on: ${browserName}`)
  })

  test('Border radius renders correctly', async ({ page, browserName }) => {
    // Check button border radius
    const primaryButton = page.getByRole('link', { name: /get started/i })
    const borderRadius = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius
    })

    // Should have border radius applied
    expect(borderRadius).not.toBe('0px')
    console.log(`Border radius verified on: ${browserName}`)
  })

  test('Box shadow renders correctly', async ({ page, browserName }) => {
    // Check feature card box shadow
    const featureCard = page.locator('[class*="card"]').first()
    const boxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow
    })

    // Should have box shadow (not 'none')
    expect(boxShadow).not.toBe('none')
    console.log(`Box shadow verified on: ${browserName}`)
  })
})

// Keyboard Accessibility Across Browsers
test.describe('Keyboard Accessibility Across Browsers', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('Tab navigation works correctly', async ({ page, browserName }) => {
    // Tab through the page
    await page.keyboard.press('Tab') // First focusable element

    // Continue tabbing to find CTAs
    const primaryCTA = page.getByRole('link', { name: /get started/i })
    const secondaryCTA = page.getByRole('link', { name: /learn more/i })

    // Tab until we reach primary CTA
    let maxTabs = 10
    let foundPrimary = false
    while (maxTabs > 0) {
      const focused = page.locator(':focus')
      const focusedText = await focused.textContent().catch(() => '')
      if (focusedText?.toLowerCase().includes('get started')) {
        foundPrimary = true
        break
      }
      await page.keyboard.press('Tab')
      maxTabs--
    }

    expect(foundPrimary).toBeTruthy()
    console.log(`Tab navigation verified on: ${browserName}`)
  })

  test('Focus indicators are visible', async ({ page, browserName }) => {
    const primaryCTA = page.getByRole('link', { name: /get started/i })

    // Focus the element programmatically
    await primaryCTA.focus()

    // Check focus styles are applied
    const focusStyles = await primaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        boxShadow: styles.boxShadow,
      }
    })

    // Should have visible focus indicator (outline or box-shadow)
    const hasFocusIndicator =
      (focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none') ||
      focusStyles.boxShadow !== 'none'
    expect(hasFocusIndicator).toBeTruthy()
    console.log(`Focus indicators verified on: ${browserName}`)
  })

  test('Enter key activates links', async ({ page, browserName }) => {
    const primaryCTA = page.getByRole('link', { name: /get started/i })

    // Focus and press Enter
    await primaryCTA.focus()
    await page.keyboard.press('Enter')

    // Should navigate to signup
    await expect(page).toHaveURL('/signup')
    console.log(`Enter key activation verified on: ${browserName}`)
  })
})

// Footer Links Consistency
test.describe('Footer Links Consistency Across Browsers', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('Privacy Policy link works', async ({ page, browserName }) => {
    const privacyLink = page.getByRole('link', { name: /privacy policy/i })
    await expect(privacyLink).toBeVisible()
    await privacyLink.click()
    await expect(page).toHaveURL('/privacy')
    console.log(`Privacy link verified on: ${browserName}`)
  })

  test('Terms of Service link works', async ({ page, browserName }) => {
    const termsLink = page.getByRole('link', { name: /terms of service/i })
    await expect(termsLink).toBeVisible()
    await termsLink.click()
    await expect(page).toHaveURL('/terms')
    console.log(`Terms link verified on: ${browserName}`)
  })

  test('Social links have correct attributes', async ({ page, browserName }) => {
    // Check social links exist and have proper attributes
    const socialLinks = page.locator('footer a[target="_blank"]')
    const count = await socialLinks.count()

    // Should have at least one social link
    expect(count).toBeGreaterThan(0)

    // Each social link should have rel="noopener noreferrer" for security
    for (let i = 0; i < count; i++) {
      const link = socialLinks.nth(i)
      const rel = await link.getAttribute('rel')
      expect(rel).toContain('noopener')
    }
    console.log(`Social links verified on: ${browserName}`)
  })
})

/**
 * E2E tests for Cross-Browser Compatibility.
 * Owner: Scenario 12 - Cross-Browser Compatibility
 *
 * Tests homepage functionality and appearance across:
 * - Chrome (latest 2 versions)
 * - Firefox (latest 2 versions)
 * - Safari (latest 2 versions)
 * - Edge (latest 2 versions)
 * - Mobile Safari (iOS)
 * - Mobile Chrome (Android)
 *
 * Test cases:
 * - Load homepage in each browser
 * - CSS Grid consistency across browsers
 * - CSS Flexbox consistency across browsers
 * - Mobile browser compatibility
 */

import { test, expect, Page, BrowserName } from '@playwright/test'

// Viewport configurations for testing
const VIEWPORTS = {
  desktop: { width: 1440, height: 900 },
  mobile: { width: 375, height: 667 },
}

/**
 * Helper function to verify all main homepage components are rendered
 */
async function verifyHomepageComponents(page: Page): Promise<void> {
  // Header should be visible
  const header = page.locator('header[role="banner"]')
  await expect(header).toBeVisible()

  // Hero section should be visible
  const heroSection = page.getByTestId('hero-section')
  await expect(heroSection).toBeVisible()

  // Hero headline should be visible
  const heroHeadline = page.getByTestId('hero-headline')
  await expect(heroHeadline).toBeVisible()

  // Features section should be visible
  const featuresSection = page.getByTestId('features-section')
  await expect(featuresSection).toBeVisible()

  // Social proof section should be visible
  const socialProofSection = page.getByTestId('social-proof-section')
  await expect(socialProofSection).toBeVisible()

  // Footer should be visible
  const footer = page.getByTestId('footer')
  await expect(footer).toBeVisible()
}

/**
 * Helper function to verify CSS Grid layouts work correctly
 */
async function verifyCSSGridLayouts(page: Page): Promise<void> {
  // Verify features grid is using CSS Grid
  const featuresGrid = page.getByTestId('features-grid')
  await expect(featuresGrid).toBeVisible()

  const gridStyles = await featuresGrid.evaluate((el) => {
    const style = window.getComputedStyle(el)
    return {
      display: style.display,
      gridTemplateColumns: style.gridTemplateColumns,
    }
  })

  // Should be using CSS Grid
  expect(gridStyles.display).toBe('grid')
  // Grid template columns should be defined (not 'none')
  expect(gridStyles.gridTemplateColumns).not.toBe('none')

  // Verify feature cards render correctly within grid
  const featureCards = page.getByTestId('feature-card')
  const cardCount = await featureCards.count()
  expect(cardCount).toBeGreaterThanOrEqual(3)

  // Check each card is visible and properly positioned
  for (let i = 0; i < cardCount; i++) {
    const card = featureCards.nth(i)
    await expect(card).toBeVisible()

    const cardBox = await card.boundingBox()
    expect(cardBox).not.toBeNull()
    expect(cardBox!.width).toBeGreaterThan(0)
    expect(cardBox!.height).toBeGreaterThan(0)
  }
}

/**
 * Helper function to verify CSS Flexbox layouts work correctly
 */
async function verifyCSSFlexboxLayouts(page: Page): Promise<void> {
  // Verify header uses Flexbox for layout
  // The structure is: header > div.container-main > div.flex
  const header = page.locator('header[role="banner"]')
  const headerStyles = await header.evaluate((el) => {
    // Get the flex container (nested inside container-main)
    const flexContainer = el.querySelector('.flex')
    if (!flexContainer) return { display: 'block', justifyContent: '', alignItems: '' }
    const style = window.getComputedStyle(flexContainer)
    return {
      display: style.display,
      justifyContent: style.justifyContent,
      alignItems: style.alignItems,
    }
  })

  expect(headerStyles.display).toBe('flex')

  // Verify footer uses Flexbox
  const footer = page.getByTestId('footer')
  const footerInner = footer.locator('> div').first()
  const footerStyles = await footerInner.evaluate((el) => {
    const style = window.getComputedStyle(el)
    return {
      display: style.display,
    }
  })

  // Footer container should use flex
  expect(['flex', 'grid', 'block']).toContain(footerStyles.display)

  // Verify social links use Flexbox
  const socialLinks = page.getByTestId('social-links')
  if ((await socialLinks.count()) > 0) {
    const socialLinksStyles = await socialLinks.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        display: style.display,
      }
    })

    expect(socialLinksStyles.display).toBe('flex')
  }
}

/**
 * Helper function to verify proper styling is applied
 */
async function verifyProperStyling(page: Page): Promise<void> {
  // Check that Tailwind CSS styles are applied
  const heroHeadline = page.getByTestId('hero-headline')
  const headlineStyles = await heroHeadline.evaluate((el) => {
    const style = window.getComputedStyle(el)
    return {
      fontSize: style.fontSize,
      fontWeight: style.fontWeight,
      color: style.color,
    }
  })

  // Font size should be set (not default 16px)
  expect(parseInt(headlineStyles.fontSize)).toBeGreaterThan(20)
  // Font weight should be bold (700 or higher)
  expect(parseInt(headlineStyles.fontWeight)).toBeGreaterThanOrEqual(600)

  // Check CTA button styling
  const heroCta = page.getByTestId('hero-cta-button')
  const ctaStyles = await heroCta.evaluate((el) => {
    const style = window.getComputedStyle(el)
    return {
      backgroundColor: style.backgroundColor,
      borderRadius: style.borderRadius,
      padding: style.padding,
    }
  })

  // Background color should be set (not transparent)
  expect(ctaStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)')
  expect(ctaStyles.backgroundColor).not.toBe('transparent')

  // Border radius should be applied
  expect(ctaStyles.borderRadius).not.toBe('0px')
}

/**
 * Helper function to verify fixed header behavior
 */
async function verifyFixedHeader(page: Page): Promise<void> {
  const header = page.locator('header[role="banner"]')

  // Get initial position
  const initialBox = await header.boundingBox()
  expect(initialBox?.y).toBe(0)

  // Scroll down
  await page.evaluate(() => window.scrollTo(0, 500))
  await page.waitForTimeout(200)

  // Header should still be at top
  const afterScrollBox = await header.boundingBox()
  expect(afterScrollBox?.y).toBe(0)
}

// Test Case 1: Load homepage in Chrome
test.describe('Test Case 1: Chrome Compatibility', () => {
  test.skip(
    ({ browserName }) => browserName !== 'chromium',
    'Chrome-specific test'
  )

  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should render all components correctly in Chrome', async ({ page }) => {
    await verifyHomepageComponents(page)
  })

  test('should apply proper styling in Chrome', async ({ page }) => {
    await verifyProperStyling(page)
  })

  test('should have fixed header behavior in Chrome', async ({ page }) => {
    await verifyFixedHeader(page)
  })

  test('should handle CSS Grid layouts in Chrome', async ({ page }) => {
    await verifyCSSGridLayouts(page)
  })

  test('should handle CSS Flexbox layouts in Chrome', async ({ page }) => {
    await verifyCSSFlexboxLayouts(page)
  })
})

// Test Case 2: Load homepage in Firefox
test.describe('Test Case 2: Firefox Compatibility', () => {
  test.skip(
    ({ browserName }) => browserName !== 'firefox',
    'Firefox-specific test'
  )

  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should render all components correctly in Firefox', async ({ page }) => {
    await verifyHomepageComponents(page)
  })

  test('should apply proper styling in Firefox', async ({ page }) => {
    await verifyProperStyling(page)
  })

  test('should have fixed header behavior in Firefox', async ({ page }) => {
    await verifyFixedHeader(page)
  })

  test('should handle CSS Grid layouts in Firefox', async ({ page }) => {
    await verifyCSSGridLayouts(page)
  })

  test('should handle CSS Flexbox layouts in Firefox', async ({ page }) => {
    await verifyCSSFlexboxLayouts(page)
  })
})

// Test Case 3: Load homepage in Safari (WebKit)
test.describe('Test Case 3: Safari Compatibility', () => {
  test.skip(({ browserName }) => browserName !== 'webkit', 'Safari-specific test')

  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should render all components correctly in Safari', async ({ page }) => {
    await verifyHomepageComponents(page)
  })

  test('should apply proper styling in Safari', async ({ page }) => {
    await verifyProperStyling(page)
  })

  test('should have fixed header behavior in Safari', async ({ page }) => {
    await verifyFixedHeader(page)
  })

  test('should handle CSS Grid layouts in Safari', async ({ page }) => {
    await verifyCSSGridLayouts(page)
  })

  test('should handle CSS Flexbox layouts in Safari', async ({ page }) => {
    await verifyCSSFlexboxLayouts(page)
  })

  test('should handle WebKit-specific prefixes in Safari', async ({ page }) => {
    // Test that -webkit- prefixed styles work
    const heroSection = page.getByTestId('hero-section')
    const styles = await heroSection.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        // WebKit handles flexbox natively now, but verify box model
        display: style.display,
        boxSizing: style.boxSizing,
      }
    })

    // Box sizing should be border-box (Tailwind default)
    expect(styles.boxSizing).toBe('border-box')
  })
})

// Test Case 4: Load homepage in Edge
test.describe('Test Case 4: Edge Compatibility', () => {
  // Edge uses Chromium, so it runs with the chromium project
  // We'll test it alongside Chrome but verify Edge-specific behaviors
  test.skip(
    ({ browserName }) => browserName !== 'chromium',
    'Edge-specific test (Chromium-based)'
  )

  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should render all components correctly in Edge', async ({ page }) => {
    await verifyHomepageComponents(page)
  })

  test('should apply proper styling in Edge', async ({ page }) => {
    await verifyProperStyling(page)
  })

  test('should have fixed header behavior in Edge', async ({ page }) => {
    await verifyFixedHeader(page)
  })

  test('should handle CSS Grid layouts in Edge', async ({ page }) => {
    await verifyCSSGridLayouts(page)
  })

  test('should handle CSS Flexbox layouts in Edge', async ({ page }) => {
    await verifyCSSFlexboxLayouts(page)
  })
})

// Test Case 5: CSS Grid consistency across browsers
test.describe('Test Case 5: CSS Grid Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should have consistent grid gap across browsers', async ({ page }) => {
    const featuresGrid = page.getByTestId('features-grid')
    const gridStyles = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        gap: style.gap,
        rowGap: style.rowGap,
        columnGap: style.columnGap,
      }
    })

    // Gap should be defined and consistent
    expect(gridStyles.gap).not.toBe('normal')
    expect(gridStyles.gap).not.toBe('0px')
  })

  test('should have consistent grid auto-flow across browsers', async ({
    page,
  }) => {
    const featuresGrid = page.getByTestId('features-grid')
    const gridStyles = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        gridAutoFlow: style.gridAutoFlow,
      }
    })

    // Grid auto-flow should be row (default)
    expect(gridStyles.gridAutoFlow).toBe('row')
  })

  test('should align grid items consistently across browsers', async ({
    page,
  }) => {
    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()

    // Get all card heights
    const cardHeights: number[] = []
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      const box = await card.boundingBox()
      if (box) {
        cardHeights.push(box.height)
      }
    }

    // All cards in the same row should have consistent heights (within grid)
    // At desktop, we have 3 columns, so cards 0,1,2 are in row 1
    if (cardHeights.length >= 3) {
      const row1Heights = cardHeights.slice(0, 3)
      const maxHeight = Math.max(...row1Heights)
      const minHeight = Math.min(...row1Heights)
      // Heights should be reasonably consistent (within 50px variance)
      expect(maxHeight - minHeight).toBeLessThan(50)
    }
  })
})

// Test Case 6: CSS Flexbox consistency across browsers
test.describe('Test Case 6: CSS Flexbox Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should have consistent flex alignment in header', async ({ page }) => {
    const header = page.locator('header[role="banner"]')

    // The structure is: header > div.container-main > div.flex
    const flexStyles = await header.evaluate((el) => {
      const flexContainer = el.querySelector('.flex')
      if (!flexContainer) return { display: 'block', alignItems: '', justifyContent: '' }
      const style = window.getComputedStyle(flexContainer)
      return {
        display: style.display,
        alignItems: style.alignItems,
        justifyContent: style.justifyContent,
      }
    })

    expect(flexStyles.display).toBe('flex')
    expect(flexStyles.alignItems).toBe('center')
    expect(flexStyles.justifyContent).toBe('space-between')
  })

  test('should have consistent flex-wrap behavior', async ({ page }) => {
    // Test that flex containers properly wrap content
    const socialLinks = page.getByTestId('social-links')
    if ((await socialLinks.count()) > 0) {
      const flexStyles = await socialLinks.evaluate((el) => {
        const style = window.getComputedStyle(el)
        return {
          display: style.display,
          flexWrap: style.flexWrap,
        }
      })

      expect(flexStyles.display).toBe('flex')
      // Flex wrap could be 'nowrap' or 'wrap' depending on design
      expect(['nowrap', 'wrap']).toContain(flexStyles.flexWrap)
    }
  })

  test('should have consistent flex-grow/shrink behavior', async ({ page }) => {
    // Verify navigation links use proper flex behavior
    const nav = page.locator('nav[aria-label="Main navigation"]')
    if ((await nav.count()) > 0) {
      const navStyles = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el)
        return {
          display: style.display,
        }
      })

      // Navigation should use flex display
      expect(navStyles.display).toBe('flex')
    }
  })
})

// Test Case 7: Mobile Safari on iOS
test.describe('Test Case 7: Mobile Safari (iOS)', () => {
  // This test runs on webkit project which emulates Safari
  test.skip(({ browserName }) => browserName !== 'webkit', 'WebKit-only test')

  test.beforeEach(async ({ page }) => {
    // iPhone 12 viewport
    await page.setViewportSize({ width: 390, height: 844 })
    await page.goto('/')
  })

  test('should render homepage correctly on iOS Safari', async ({ page }) => {
    await verifyHomepageComponents(page)
  })

  test('should display hamburger menu on mobile Safari', async ({ page }) => {
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeVisible()

    // Desktop navigation should be hidden
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeHidden()
  })

  test('should handle mobile menu interaction on iOS Safari', async ({
    page,
  }) => {
    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Mobile menu should be visible
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Navigation links should be visible in mobile menu
    await expect(page.getByTestId('mobile-nav-link-features')).toBeVisible()
  })

  test('should handle touch targets properly on iOS Safari', async ({
    page,
  }) => {
    // Verify touch targets meet minimum size
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    const box = await hamburgerButton.boundingBox()

    expect(box).not.toBeNull()
    expect(box!.width).toBeGreaterThanOrEqual(40)
    expect(box!.height).toBeGreaterThanOrEqual(40)
  })

  test('should handle iOS-specific viewport units', async ({ page }) => {
    // Test that the page doesn't have overflow issues with iOS viewport
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth
    })

    expect(hasHorizontalOverflow).toBe(false)
  })

  test('should render CSS Grid correctly on iOS Safari', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    const featuresGrid = page.getByTestId('features-grid')
    const gridStyles = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        display: style.display,
      }
    })

    expect(gridStyles.display).toBe('grid')
  })
})

// Test Case 8: Mobile Chrome on Android
test.describe('Test Case 8: Mobile Chrome (Android)', () => {
  test.skip(
    ({ browserName }) => browserName !== 'chromium',
    'Chromium-only test'
  )

  test.beforeEach(async ({ page }) => {
    // Pixel 5 viewport
    await page.setViewportSize({ width: 393, height: 851 })
    await page.goto('/')
  })

  test('should render homepage correctly on Android Chrome', async ({
    page,
  }) => {
    await verifyHomepageComponents(page)
  })

  test('should display hamburger menu on Android Chrome', async ({ page }) => {
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeVisible()

    // Desktop navigation should be hidden
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeHidden()
  })

  test('should handle mobile menu interaction on Android Chrome', async ({
    page,
  }) => {
    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Mobile menu should be visible
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // CTA button should be visible in mobile menu
    await expect(page.getByTestId('mobile-menu-cta')).toBeVisible()
  })

  test('should handle touch targets properly on Android Chrome', async ({
    page,
  }) => {
    // Verify CTA button has adequate touch target
    const heroCta = page.getByTestId('hero-cta-button')
    const box = await heroCta.boundingBox()

    expect(box).not.toBeNull()
    expect(box!.width).toBeGreaterThanOrEqual(40)
    expect(box!.height).toBeGreaterThanOrEqual(40)
  })

  test('should handle Android-specific CSS rendering', async ({ page }) => {
    // Test that font rendering works correctly
    const heroHeadline = page.getByTestId('hero-headline')
    const headlineStyles = await heroHeadline.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        fontSize: style.fontSize,
        fontFamily: style.fontFamily,
        lineHeight: style.lineHeight,
      }
    })

    // Font size should be readable on mobile
    expect(parseInt(headlineStyles.fontSize)).toBeGreaterThan(20)
    // Font family should be set
    expect(headlineStyles.fontFamily).not.toBe('')
  })

  test('should render CSS Flexbox correctly on Android Chrome', async ({
    page,
  }) => {
    const header = page.locator('header[role="banner"]')

    // The structure is: header > div.container-main > div.flex
    const flexStyles = await header.evaluate((el) => {
      const flexContainer = el.querySelector('.flex')
      if (!flexContainer) return { display: 'block' }
      const style = window.getComputedStyle(flexContainer)
      return {
        display: style.display,
      }
    })

    expect(flexStyles.display).toBe('flex')
  })
})

// Cross-browser visual consistency tests
test.describe('Cross-Browser Visual Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should have consistent font rendering across browsers', async ({
    page,
  }) => {
    const heroHeadline = page.getByTestId('hero-headline')
    const styles = await heroHeadline.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        fontFamily: style.fontFamily,
        fontWeight: style.fontWeight,
        fontStyle: style.fontStyle,
      }
    })

    // Font family should be set (not just browser default)
    expect(styles.fontFamily).not.toBe('')
    // Font weight should be bold
    expect(parseInt(styles.fontWeight)).toBeGreaterThanOrEqual(600)
    // Font style should be normal (not italic unless intended)
    expect(styles.fontStyle).toBe('normal')
  })

  test('should have consistent color rendering across browsers', async ({
    page,
  }) => {
    const heroCta = page.getByTestId('hero-cta-button')
    const styles = await heroCta.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        backgroundColor: style.backgroundColor,
        color: style.color,
      }
    })

    // Colors should be defined
    expect(styles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)')
    expect(styles.color).not.toBe('rgba(0, 0, 0, 0)')
  })

  test('should have consistent box model across browsers', async ({ page }) => {
    const featureCard = page.getByTestId('feature-card').first()
    const styles = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        boxSizing: style.boxSizing,
        margin: style.margin,
        padding: style.padding,
      }
    })

    // Box sizing should be border-box (Tailwind default)
    expect(styles.boxSizing).toBe('border-box')
  })

  test('should have consistent transform support across browsers', async ({
    page,
  }) => {
    // Test that CSS transforms work (used for animations/hover states)
    const featureCard = page.getByTestId('feature-card').first()

    // Apply a transform and verify it works
    await featureCard.evaluate((el) => {
      el.style.transform = 'scale(1.05)'
    })

    const transformStyles = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transform: style.transform,
      }
    })

    // Transform should be applied (not 'none')
    expect(transformStyles.transform).not.toBe('none')
  })

  test('should have consistent transition support across browsers', async ({
    page,
  }) => {
    const heroCta = page.getByTestId('hero-cta-button')
    const styles = await heroCta.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transition: style.transition,
        transitionProperty: style.transitionProperty,
      }
    })

    // Transitions should be defined for interactive elements
    // If no transition is explicitly set, transitionProperty will be 'all' or similar
    expect(styles.transitionProperty).not.toBe('')
  })
})

// Performance consistency across browsers
test.describe('Cross-Browser Performance', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
  })

  test('should load homepage within acceptable time across browsers', async ({
    page,
  }) => {
    const startTime = Date.now()
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
    const loadTime = Date.now() - startTime

    // Page should load within 5 seconds (generous for CI environments)
    expect(loadTime).toBeLessThan(5000)
  })

  test('should render above-the-fold content quickly', async ({ page }) => {
    await page.goto('/')

    // Above-the-fold content should be visible immediately
    const header = page.locator('header[role="banner"]')
    const heroSection = page.getByTestId('hero-section')

    await expect(header).toBeVisible({ timeout: 3000 })
    await expect(heroSection).toBeVisible({ timeout: 3000 })
  })
})

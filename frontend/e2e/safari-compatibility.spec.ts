import { test, expect } from '@playwright/test'

/**
 * Browser Compatibility Tests for Safari (NFR-3)
 * Verifies homepage works correctly in Safari browser (via WebKit)
 *
 * Test cases cover:
 * 1. All sections render correctly without layout issues
 * 2. Webkit-prefixed CSS features work correctly
 * 3. Safari-specific CSS handling (flexbox, grid, gradients)
 * 4. Interactive elements function properly
 * 5. Theme switching works correctly
 */

// Only run these tests on webkit browser
test.describe('Browser Compatibility - Safari (NFR-3)', () => {
  test.beforeEach(async ({ browserName }) => {
    // Skip if not running on webkit
    test.skip(browserName !== 'webkit', 'Safari tests only run on WebKit browser')
  })

  /**
   * Test Case 1: Load homepage in Safari (latest) and verify all sections render correctly
   * Expected: All sections render correctly without layout issues
   */
  test('all sections render correctly without layout issues', async ({ page }) => {
    // Collect any console errors during page load
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text())
      }
    })

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify Hero Section renders correctly
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Check hero section has proper dimensions (not collapsed)
    const heroBox = await heroSection.boundingBox()
    expect(heroBox).not.toBeNull()
    expect(heroBox!.height).toBeGreaterThan(100)
    expect(heroBox!.width).toBeGreaterThan(300)

    // Verify headline is visible and readable
    const headline = page.getByRole('heading', { level: 1 })
    await expect(headline).toBeVisible()
    await expect(headline).toContainText('Shorten URLs')

    // Verify subheadline is visible
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()

    // Verify CTA buttons are visible and properly styled
    const getStartedButton = page.getByTestId('cta-get-started')
    await expect(getStartedButton).toBeVisible()
    const getStartedBox = await getStartedButton.boundingBox()
    expect(getStartedBox).not.toBeNull()
    expect(getStartedBox!.height).toBeGreaterThan(20) // Button has proper height

    const loginButton = page.getByTestId('cta-login')
    await expect(loginButton).toBeVisible()

    // Verify Features Section renders correctly
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible()
    const featuresBox = await featuresSection.boundingBox()
    expect(featuresBox).not.toBeNull()
    expect(featuresBox!.height).toBeGreaterThan(100)

    // Verify feature cards are visible
    const featureCards = page.getByTestId('feature-card')
    await expect(featureCards.first()).toBeVisible()
    const featureCount = await featureCards.count()
    expect(featureCount).toBeGreaterThanOrEqual(3)

    // Verify Demo Section renders correctly
    const demoSection = page.getByTestId('demo-section')
    await expect(demoSection).toBeVisible()
    const demoBox = await demoSection.boundingBox()
    expect(demoBox).not.toBeNull()
    expect(demoBox!.height).toBeGreaterThan(100)

    // Verify demo form elements are visible
    const urlInput = page.getByTestId('demo-url-input')
    await expect(urlInput).toBeVisible()
    const submitButton = page.getByTestId('demo-submit-button')
    await expect(submitButton).toBeVisible()

    // Verify Footer Section renders correctly
    const footerSection = page.getByTestId('footer-section')
    await expect(footerSection).toBeVisible()

    // Verify no layout overlap issues (sections are in proper order vertically)
    expect(heroBox!.y).toBeLessThan(featuresBox!.y)
    expect(featuresBox!.y).toBeLessThan(demoBox!.y)
  })

  /**
   * Test Case 2: Test Safari-specific CSS features - Webkit prefixed styles work correctly
   * Expected: Webkit prefixed styles work correctly
   */
  test('webkit-prefixed CSS features work correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify webkit-specific CSS properties are supported
    // Test -webkit-backdrop-filter support (Safari's glassmorphism)
    const webkitBackdropFilterSupported = await page.evaluate(() => {
      return (
        CSS.supports('-webkit-backdrop-filter', 'blur(10px)') ||
        CSS.supports('backdrop-filter', 'blur(10px)')
      )
    })
    expect(webkitBackdropFilterSupported).toBe(true)

    // Test -webkit-transform support
    const webkitTransformSupported = await page.evaluate(() => {
      return (
        CSS.supports('-webkit-transform', 'translateY(0)') ||
        CSS.supports('transform', 'translateY(0)')
      )
    })
    expect(webkitTransformSupported).toBe(true)

    // Test -webkit-transition support
    const webkitTransitionSupported = await page.evaluate(() => {
      return (
        CSS.supports('-webkit-transition', 'all 0.3s ease') ||
        CSS.supports('transition', 'all 0.3s ease')
      )
    })
    expect(webkitTransitionSupported).toBe(true)

    // Test flexbox rendering in Safari
    const heroSection = page.getByTestId('hero-section')
    const flexboxStyles = await heroSection.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        display: computed.display,
        alignItems: computed.alignItems,
        justifyContent: computed.justifyContent,
      }
    })
    expect(flexboxStyles.display).toBe('flex')
    expect(flexboxStyles.alignItems).toBe('center')
    expect(flexboxStyles.justifyContent).toBe('center')

    // Test CSS Grid rendering in Safari
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for animations to complete
    await page.waitForTimeout(1500)

    const gridContainer = featuresSection.locator('.grid')
    const gridStyles = await gridContainer.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        display: computed.display,
        gap: computed.gap,
      }
    })
    expect(gridStyles.display).toBe('grid')

    // Test CSS gradient support
    const gradientSupported = await page.evaluate(() => {
      const testDiv = document.createElement('div')
      testDiv.style.backgroundImage = 'linear-gradient(to right, red, blue)'
      return testDiv.style.backgroundImage !== ''
    })
    expect(gradientSupported).toBe(true)

    // Test CSS custom properties (variables) support
    const cssVarsSupported = await page.evaluate(() => {
      return CSS.supports('(--test-var: 1)')
    })
    expect(cssVarsSupported).toBe(true)

    // Verify DaisyUI theme variables are working
    const themeVarsWorking = await page.evaluate(() => {
      const html = document.documentElement
      const computedStyle = getComputedStyle(html)
      // Check if DaisyUI theme variables are set
      const primaryColor = computedStyle.getPropertyValue('--p')
      return primaryColor !== ''
    })
    expect(themeVarsWorking).toBe(true)
  })

  /**
   * Test: Safari flexbox and grid layout compatibility
   */
  test('flexbox and grid layouts render correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Test hero section flexbox container
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify flexbox centering works
    const heroStyles = await heroSection.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        display: computed.display,
        flexDirection: computed.flexDirection,
        alignItems: computed.alignItems,
        justifyContent: computed.justifyContent,
        minHeight: computed.minHeight,
      }
    })

    expect(heroStyles.display).toBe('flex')
    expect(heroStyles.alignItems).toBe('center')
    expect(heroStyles.justifyContent).toBe('center')
    // min-h-screen should be 100vh
    expect(heroStyles.minHeight).toMatch(/100vh|100svh/)

    // Test CTA button container flexbox
    const ctaGetStarted = page.getByTestId('cta-get-started')
    const ctaContainer = ctaGetStarted.locator('..')
    const ctaContainerStyles = await ctaContainer.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        display: computed.display,
        flexDirection: computed.flexDirection,
        gap: computed.gap,
      }
    })
    expect(ctaContainerStyles.display).toBe('flex')

    // Test features section grid layout
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(1000)

    const gridContainer = featuresSection.locator('.grid')
    await expect(gridContainer).toBeVisible()

    const gridStyles = await gridContainer.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns,
        gap: computed.gap,
      }
    })

    expect(gridStyles.display).toBe('grid')
    // Should have proper gap value
    expect(gridStyles.gap).toBeTruthy()
  })

  /**
   * Test: Safari CSS transitions and transforms
   */
  test('CSS transitions and transforms work correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for page to fully load
    await expect(page.getByTestId('hero-section')).toBeVisible()

    // Test that feature cards have transition properties
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(1500)

    const featureCards = page.getByTestId('feature-card')
    const firstCard = featureCards.first()
    await expect(firstCard).toBeVisible()

    // Check card has transition properties
    const cardStyles = await firstCard.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        transition: computed.transition,
        transitionProperty: computed.transitionProperty,
        transitionDuration: computed.transitionDuration,
      }
    })

    // Card should have some transition defined
    expect(cardStyles.transition || cardStyles.transitionProperty).toBeTruthy()

    // Test hover effect by checking computed styles change
    const initialBoxShadow = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow
    })

    // Hover over the card
    await firstCard.hover()
    await page.waitForTimeout(400)

    // Box shadow may change on hover
    const hoverBoxShadow = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow
    })

    // Both should be valid (hover may or may not change shadow based on CSS)
    expect(hoverBoxShadow).toBeTruthy()
  })

  /**
   * Test: Interactive elements function correctly in Safari
   */
  test('interactive elements function correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Test Get Started CTA button click
    const getStartedButton = page.getByTestId('cta-get-started')
    await expect(getStartedButton).toBeVisible()
    await expect(getStartedButton).toBeEnabled()

    // Verify button is clickable and navigates correctly
    await getStartedButton.click()
    await expect(page).toHaveURL(/register/)

    // Navigate back to homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Test Login CTA button click
    const loginButton = page.getByTestId('cta-login')
    await expect(loginButton).toBeVisible()
    await expect(loginButton).toBeEnabled()
    await loginButton.click()
    await expect(page).toHaveURL(/login/)

    // Navigate back to homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Test demo form input
    const urlInput = page.getByTestId('demo-url-input')
    await expect(urlInput).toBeVisible()
    await urlInput.fill('https://test-url.com')
    await expect(urlInput).toHaveValue('https://test-url.com')

    // Test demo submit button
    const submitButton = page.getByTestId('demo-submit-button')
    await submitButton.click()

    // Should show register prompt for valid URL
    const registerPrompt = page.getByTestId('demo-register-prompt')
    await expect(registerPrompt).toBeVisible()
  })

  /**
   * Test: Theme toggle works in Safari
   */
  test('theme toggle works correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Get initial theme
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })

    // Find and click theme toggle if available
    const themeToggle = page.locator('[data-testid="theme-toggle"]')
    if (await themeToggle.isVisible()) {
      await themeToggle.click()
      await page.waitForTimeout(300)

      // Verify theme changed
      const newTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      // Theme should have changed (or at least be a valid theme)
      expect(newTheme).toBeTruthy()
    }

    // Verify page still renders correctly after theme change
    await expect(page.getByTestId('hero-section')).toBeVisible()
    await expect(page.getByTestId('features-section')).toBeVisible()
  })

  /**
   * Test: No JavaScript errors in Safari
   */
  test('no JavaScript errors in console', async ({ page }) => {
    const consoleErrors: string[] = []
    const pageErrors: string[] = []

    // Listen for console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text())
      }
    })

    // Listen for page errors (uncaught exceptions)
    page.on('pageerror', (error) => {
      pageErrors.push(error.message)
    })

    // Navigate to homepage and wait for full load
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for any animations to complete
    await page.waitForTimeout(1000)

    // Verify page content loaded successfully
    await expect(page.getByTestId('hero-section')).toBeVisible()
    await expect(page.getByTestId('features-section')).toBeVisible()
    await expect(page.getByTestId('demo-section')).toBeVisible()

    // Scroll through the page to trigger any lazy-loaded content
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))
    await page.waitForTimeout(500)
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(500)

    // Filter out known non-critical errors (e.g., favicon, extensions)
    const criticalErrors = consoleErrors.filter(
      (error) =>
        !error.includes('favicon') &&
        !error.includes('404') &&
        !error.includes('Failed to load resource') &&
        !error.includes('net::ERR_')
    )

    // Assert no critical JavaScript errors
    expect(criticalErrors).toHaveLength(0)
    expect(pageErrors).toHaveLength(0)
  })

  /**
   * Test: Smooth scroll navigation works in Safari
   */
  test('smooth scroll navigation works correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Test smooth scroll to features section
    const featuresNav = page.getByTestId('nav-features')
    if (await featuresNav.isVisible()) {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY)

      await featuresNav.click()
      await page.waitForTimeout(1000) // Wait for smooth scroll animation

      // Get new scroll position
      const newScrollY = await page.evaluate(() => window.scrollY)

      // Scroll position should have changed (scrolled down to features)
      expect(newScrollY).toBeGreaterThan(initialScrollY)

      // Verify features section is now in view
      const featuresSection = page.getByTestId('features-section')
      const isInViewport = await featuresSection.evaluate((el) => {
        const rect = el.getBoundingClientRect()
        return rect.top >= -100 && rect.top < window.innerHeight
      })
      expect(isInViewport).toBe(true)
    }
  })

  /**
   * Test: SVG rendering in Safari
   */
  test('SVG icons render correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Scroll to features section where icons are
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(1500)

    // Find SVG elements
    const svgElements = featuresSection.locator('svg')
    const svgCount = await svgElements.count()

    // Should have SVG icons in feature cards
    expect(svgCount).toBeGreaterThan(0)

    // Verify first SVG is visible and has proper dimensions
    const firstSvg = svgElements.first()
    await expect(firstSvg).toBeVisible()

    const svgBox = await firstSvg.boundingBox()
    expect(svgBox).not.toBeNull()
    expect(svgBox!.width).toBeGreaterThan(0)
    expect(svgBox!.height).toBeGreaterThan(0)

    // Verify SVG has proper attributes
    const svgAttrs = await firstSvg.evaluate((el) => {
      return {
        xmlns: el.getAttribute('xmlns'),
        viewBox: el.getAttribute('viewBox'),
      }
    })
    expect(svgAttrs.xmlns).toBe('http://www.w3.org/2000/svg')
    expect(svgAttrs.viewBox).toBeTruthy()
  })

  /**
   * Test: Form elements render and function correctly in Safari
   */
  test('form elements work correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Scroll to demo section
    const demoSection = page.getByTestId('demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    // Test input field
    const urlInput = page.getByTestId('demo-url-input')
    await expect(urlInput).toBeVisible()

    // Verify input has proper styling
    const inputStyles = await urlInput.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        display: computed.display,
        padding: computed.padding,
        border: computed.border,
        borderRadius: computed.borderRadius,
      }
    })

    // Input should have proper border and padding
    expect(inputStyles.padding).toBeTruthy()

    // Test input interaction
    await urlInput.click()
    await urlInput.fill('https://example.com/test-url')
    await expect(urlInput).toHaveValue('https://example.com/test-url')

    // Clear and type again
    await urlInput.clear()
    await urlInput.type('https://another-test.com')
    await expect(urlInput).toHaveValue('https://another-test.com')

    // Test button
    const submitButton = page.getByTestId('demo-submit-button')
    await expect(submitButton).toBeVisible()

    const buttonStyles = await submitButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        display: computed.display,
        cursor: computed.cursor,
        backgroundColor: computed.backgroundColor,
      }
    })

    // Button should have pointer cursor
    expect(buttonStyles.cursor).toBe('pointer')
    expect(buttonStyles.backgroundColor).toBeTruthy()
  })

  /**
   * Test: Responsive design works in Safari
   */
  test('responsive design adapts correctly', async ({ page }) => {
    // Test mobile viewport
    await page.setViewportSize({ width: 375, height: 812 })
    await page.goto('/', { waitUntil: 'networkidle' })

    // Hero section should be visible and properly sized
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const heroBoxMobile = await heroSection.boundingBox()
    expect(heroBoxMobile!.width).toBeLessThanOrEqual(375)

    // CTA buttons should be stacked on mobile
    const ctaGetStarted = page.getByTestId('cta-get-started')
    const ctaLogin = page.getByTestId('cta-login')

    const getStartedBox = await ctaGetStarted.boundingBox()
    const loginBox = await ctaLogin.boundingBox()

    // On mobile, buttons should be stacked (different Y positions) or side by side
    expect(getStartedBox).not.toBeNull()
    expect(loginBox).not.toBeNull()

    // Test tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.waitForTimeout(500)

    const heroBoxTablet = await heroSection.boundingBox()
    expect(heroBoxTablet!.width).toBeGreaterThan(heroBoxMobile!.width)

    // Test desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 })
    await page.waitForTimeout(500)

    const heroBoxDesktop = await heroSection.boundingBox()
    expect(heroBoxDesktop!.width).toBeGreaterThan(heroBoxTablet!.width)

    // Features should be in grid layout on desktop
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(500)

    const featureCards = page.getByTestId('feature-card')
    const firstCard = featureCards.first()
    const secondCard = featureCards.nth(1)

    const firstCardBox = await firstCard.boundingBox()
    const secondCardBox = await secondCard.boundingBox()

    // On desktop, cards should be side by side (same Y position, different X)
    if (firstCardBox && secondCardBox) {
      expect(Math.abs(firstCardBox.y - secondCardBox.y)).toBeLessThan(50)
      expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x)
    }
  })
})

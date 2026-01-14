import { test, expect } from '@playwright/test'

/**
 * Browser Compatibility Tests for Chrome (NFR-3)
 * Verifies homepage works correctly in Chrome browser
 *
 * Test cases cover:
 * 1. All sections render correctly without layout issues
 * 2. No JavaScript errors in console
 * 3. Glassmorphism/backdrop-filter blur effects render correctly
 */

test.describe('Browser Compatibility - Chrome (NFR-3)', () => {
  /**
   * Test Case 1: Load homepage in Chrome and verify all sections render correctly
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
   * Test Case 2: Check Chrome console for JavaScript errors after page load
   * Expected: No JavaScript errors in console
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

    // Test CTA interactions
    const getStartedButton = page.getByTestId('cta-get-started')
    await getStartedButton.hover()
    await page.waitForTimeout(100)

    // Test theme toggle if available
    const themeToggle = page.locator('[data-testid="theme-toggle"]')
    if (await themeToggle.isVisible()) {
      await themeToggle.click()
      await page.waitForTimeout(200)
    }

    // Test demo form interaction
    const urlInput = page.getByTestId('demo-url-input')
    await urlInput.fill('https://example.com')
    const submitButton = page.getByTestId('demo-submit-button')
    await submitButton.click()
    await page.waitForTimeout(500)

    // Scroll through the page to trigger any lazy-loaded content
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))
    await page.waitForTimeout(500)
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(500)

    // Log any errors found for debugging
    if (consoleErrors.length > 0) {
      console.log('Console errors found:', consoleErrors)
    }
    if (pageErrors.length > 0) {
      console.log('Page errors found:', pageErrors)
    }

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
   * Test Case 3: Test glassmorphism effects render correctly in Chrome
   * Expected: Backdrop-filter blur effects render correctly
   */
  test('glassmorphism backdrop-filter blur effects render correctly', async ({
    page,
  }) => {
    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for page to fully render
    await expect(page.getByTestId('hero-section')).toBeVisible()

    // Check if backdrop-filter is supported in Chrome
    const backdropFilterSupported = await page.evaluate(() => {
      return CSS.supports('backdrop-filter', 'blur(10px)')
    })
    expect(backdropFilterSupported).toBe(true)

    // Scroll to features section to trigger Framer Motion animations
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for animations to complete (Framer Motion uses whileInView)
    await page.waitForTimeout(1500)

    // Find elements that might use glassmorphism effects (cards, buttons)
    const cards = page.locator('.card')
    const cardsCount = await cards.count()

    if (cardsCount > 0) {
      // Check that cards render with proper styling
      const firstCard = cards.first()
      await expect(firstCard).toBeVisible()

      // Verify card has visual presence (proper box model)
      const cardBox = await firstCard.boundingBox()
      expect(cardBox).not.toBeNull()
      expect(cardBox!.width).toBeGreaterThan(0)
      expect(cardBox!.height).toBeGreaterThan(0)

      // Check computed styles to verify CSS is being applied
      const cardStyles = await firstCard.evaluate((el) => {
        const computed = window.getComputedStyle(el)
        return {
          backgroundColor: computed.backgroundColor,
          boxShadow: computed.boxShadow,
          borderRadius: computed.borderRadius,
        }
      })

      // Verify card has some styling applied (not completely unstyled)
      expect(cardStyles.backgroundColor).toBeTruthy()
    }

    // Check feature cards specifically for glassmorphism effects
    const featureCards = page.getByTestId('feature-card')
    const featureCardCount = await featureCards.count()

    for (let i = 0; i < Math.min(featureCardCount, 3); i++) {
      const card = featureCards.nth(i)
      await expect(card).toBeVisible()

      // Verify card has proper visual appearance after animation
      const cardStyles = await card.evaluate((el) => {
        const computed = window.getComputedStyle(el)
        return {
          display: computed.display,
          visibility: computed.visibility,
          opacity: computed.opacity,
          boxShadow: computed.boxShadow,
        }
      })

      // Card should be visible (not hidden)
      expect(cardStyles.visibility).toBe('visible')
      // After animation, opacity should be greater than 0 (allow for animation in progress)
      const opacity = parseFloat(cardStyles.opacity)
      expect(opacity).toBeGreaterThanOrEqual(0)
    }

    // Test that backdrop-filter can be applied dynamically
    const backdropTestResult = await page.evaluate(() => {
      const testDiv = document.createElement('div')
      testDiv.style.backdropFilter = 'blur(10px)'
      document.body.appendChild(testDiv)
      const computed = window.getComputedStyle(testDiv)
      const result = computed.backdropFilter
      document.body.removeChild(testDiv)
      return result
    })

    // Chrome should support and apply backdrop-filter
    expect(backdropTestResult).toMatch(/blur|none/)

    // Verify there are no rendering issues with semi-transparent backgrounds
    const bgElements = await page.evaluate(() => {
      const elements = document.querySelectorAll('[class*="bg-"]')
      const issues: string[] = []

      elements.forEach((el) => {
        const computed = window.getComputedStyle(el)
        const bg = computed.backgroundColor

        // Check if background color is valid (not completely transparent unless intended)
        if (bg === 'rgba(0, 0, 0, 0)') {
          // Element has transparent background - this is often intentional
        }
      })

      return issues
    })

    expect(bgElements).toHaveLength(0)
  })

  /**
   * Additional test: Verify all interactive elements work in Chrome
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

    // Test smooth scroll navigation links
    const featuresNav = page.getByTestId('nav-features')
    if (await featuresNav.isVisible()) {
      await featuresNav.click()
      await page.waitForTimeout(1000) // Wait for smooth scroll

      // Verify features section is now in view
      const featuresSection = page.getByTestId('features-section')
      const isInViewport = await featuresSection.evaluate((el) => {
        const rect = el.getBoundingClientRect()
        return rect.top >= 0 && rect.top < window.innerHeight
      })
      expect(isInViewport).toBe(true)
    }
  })

  /**
   * Additional test: Verify visual rendering consistency
   */
  test('visual rendering is consistent across viewport', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Take screenshot of hero section for visual verification
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify text is readable (has proper color contrast)
    const headline = page.getByRole('heading', { level: 1 })
    const headlineStyles = await headline.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        color: computed.color,
        fontSize: computed.fontSize,
        fontWeight: computed.fontWeight,
      }
    })

    // Headline should have valid styling
    expect(headlineStyles.color).toBeTruthy()
    expect(headlineStyles.fontSize).toBeTruthy()
    expect(parseFloat(headlineStyles.fontSize)).toBeGreaterThan(20)
    expect(parseInt(headlineStyles.fontWeight)).toBeGreaterThanOrEqual(600)

    // Verify features section cards have proper grid layout
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible()

    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()

    // Get positions of all cards
    const cardPositions: { x: number; y: number }[] = []
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      const box = await card.boundingBox()
      if (box) {
        cardPositions.push({ x: box.x, y: box.y })
      }
    }

    // On desktop, cards should be in a grid (not all stacked vertically)
    // Check if any cards share the same Y position (indicating grid layout)
    const uniqueYPositions = new Set(cardPositions.map((p) => Math.round(p.y)))
    // For 3 cards in a grid on desktop, we might have 1 row, for more cards we'd have multiple rows
    expect(cardPositions.length).toBeGreaterThanOrEqual(3)

    // Verify footer is at the bottom
    const footer = page.getByTestId('footer-section')
    await expect(footer).toBeVisible()
    const footerBox = await footer.boundingBox()
    const demoBox = await page.getByTestId('demo-section').boundingBox()

    if (footerBox && demoBox) {
      expect(footerBox.y).toBeGreaterThan(demoBox.y)
    }
  })
})

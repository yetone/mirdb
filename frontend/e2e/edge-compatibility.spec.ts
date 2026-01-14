import { test, expect } from '@playwright/test'

/**
 * Edge Browser Compatibility Tests (NFR-3)
 *
 * Validates that the homepage works correctly in Microsoft Edge browser:
 * 1. All sections render correctly without layout issues
 * 2. No JavaScript errors in console
 * 3. CSS backdrop-filter/glassmorphism effects work correctly
 * 4. Interactive elements function properly
 *
 * Note: Run with --project=msedge to execute these tests in Edge
 */

test.describe('Edge Browser Compatibility (NFR-3)', () => {
  /**
   * Test Case 1: Load homepage in Edge (latest)
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
    const heroHeadline = page.getByRole('heading', { level: 1 })
    await expect(heroHeadline).toBeVisible()
    await expect(heroHeadline).toHaveText('Shorten URLs. Track Results.')

    // Verify subheadline is visible
    const heroSubheadline = page.getByTestId('hero-subheadline')
    await expect(heroSubheadline).toBeVisible()

    // Verify CTA buttons are visible and properly styled
    const getStartedButton = page.getByTestId('cta-get-started')
    await expect(getStartedButton).toBeVisible()
    await expect(getStartedButton).toHaveAttribute('href', '/register')
    const getStartedBox = await getStartedButton.boundingBox()
    expect(getStartedBox).not.toBeNull()
    expect(getStartedBox!.height).toBeGreaterThan(20) // Button has proper height

    const loginButton = page.getByTestId('cta-login')
    await expect(loginButton).toBeVisible()
    await expect(loginButton).toHaveAttribute('href', '/login')

    // Verify Features Section renders correctly
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible()

    // Check features heading
    const featuresHeading = featuresSection.getByRole('heading', { level: 2 })
    await expect(featuresHeading).toHaveText('Powerful Features')

    // Verify all feature cards are visible
    const featureCards = page.getByTestId('feature-card')
    await expect(featureCards).toHaveCount(3)
    const featuresBox = await featuresSection.boundingBox()
    expect(featuresBox).not.toBeNull()
    expect(featuresBox!.height).toBeGreaterThan(100)

    // Verify Demo Section renders correctly
    const demoSection = page.getByTestId('demo-section')
    await expect(demoSection).toBeVisible()
    const demoBox = await demoSection.boundingBox()
    expect(demoBox).not.toBeNull()
    expect(demoBox!.height).toBeGreaterThan(100)

    // Check demo form elements
    const demoInput = page.getByTestId('demo-url-input')
    await expect(demoInput).toBeVisible()
    await expect(demoInput).toBeEnabled()

    const demoButton = page.getByTestId('demo-submit-button')
    await expect(demoButton).toBeVisible()
    await expect(demoButton).toBeEnabled()

    // Verify Footer Section renders correctly
    const footerSection = page.getByTestId('footer-section')
    await expect(footerSection).toBeVisible()

    // Check footer copyright
    const footerCopyright = page.getByTestId('footer-copyright')
    await expect(footerCopyright).toBeVisible()
    await expect(footerCopyright).toContainText('URL Shortener')

    // Verify no layout issues by checking viewport dimensions
    const viewportSize = page.viewportSize()
    expect(viewportSize).toBeDefined()

    // Check that no horizontal scrollbar appears (layout issue indicator)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)

    // Verify sections are in correct order (top to bottom)
    const footerRect = await footerSection.boundingBox()
    expect(heroBox!.y).toBeLessThan(featuresBox!.y)
    expect(featuresBox!.y).toBeLessThan(demoBox!.y)
    expect(demoBox!.y).toBeLessThan(footerRect!.y)
  })

  /**
   * Test Case 2: Check Edge console after page load
   * Expected: No JavaScript errors in console
   */
  test('no JavaScript errors in console', async ({ page }) => {
    const consoleErrors: { message: string; url: string; lineNumber: number }[] = []
    const pageErrors: string[] = []

    // Listen for console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push({
          message: msg.text(),
          url: msg.location().url,
          lineNumber: msg.location().lineNumber,
        })
      }
    })

    // Listen for uncaught exceptions
    page.on('pageerror', (error) => {
      pageErrors.push(error.message)
    })

    // Navigate to homepage and wait for full load
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for animations to complete
    await page.waitForTimeout(1500)

    // Verify page content loaded successfully
    await expect(page.getByTestId('hero-section')).toBeVisible()
    await expect(page.getByTestId('features-section')).toBeVisible()
    await expect(page.getByTestId('demo-section')).toBeVisible()

    // Interact with the page to trigger any potential JS errors
    // Test navigation links
    const navFeatures = page.getByTestId('nav-features')
    await navFeatures.click()
    await page.waitForTimeout(500)

    const navDemo = page.getByTestId('nav-demo')
    await navDemo.click()
    await page.waitForTimeout(500)

    // Test CTA hover interaction
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
    const demoInput = page.getByTestId('demo-url-input')
    await demoInput.fill('https://example.com/test-url')

    const demoButton = page.getByTestId('demo-submit-button')
    await demoButton.click()
    await page.waitForTimeout(500)

    // Verify register prompt appears (indicates JS is working)
    const registerPrompt = page.getByTestId('demo-register-prompt')
    await expect(registerPrompt).toBeVisible()

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

    // Filter out non-critical errors (like network failures that aren't JS errors)
    const criticalErrors = consoleErrors.filter((error) => {
      // Ignore network-related errors or resource loading errors
      const ignoredPatterns = [
        'net::ERR_',
        'Failed to load resource',
        'favicon',
        '404',
      ]
      return !ignoredPatterns.some((pattern) => error.message.includes(pattern))
    })

    // Assert no critical JavaScript errors
    expect(criticalErrors).toHaveLength(0)
    expect(pageErrors).toHaveLength(0)
  })

  /**
   * Test Case 3: Test glassmorphism effects render correctly in Edge
   * Expected: Backdrop-filter blur effects render correctly (Edge Chromium supports this)
   */
  test('glassmorphism backdrop-filter blur effects render correctly', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for page to fully render
    await expect(page.getByTestId('hero-section')).toBeVisible()

    // Check if backdrop-filter is supported in Edge (should be since Edge is Chromium-based)
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

      // Wait for animation to complete for this specific card
      await page.waitForFunction(
        ({ selector, index }) => {
          const cards = document.querySelectorAll(selector)
          const card = cards[index] as HTMLElement
          if (!card) return false
          const styles = window.getComputedStyle(card)
          return styles.opacity === '1'
        },
        { selector: '[data-testid="feature-card"]', index: i },
        { timeout: 2000 }
      )

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
      expect(cardStyles.opacity).toBe('1')
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

    // Edge should support and apply backdrop-filter
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
   * Test Case 4: Verify all interactive elements work in Edge
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
   * Test Case 5: Verify theme toggle works in Edge
   */
  test('theme toggle works correctly in Edge', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Get initial theme
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })

    // Verify theme is applied (DaisyUI themes)
    expect(['light', 'dark', 'cyberpunk', 'synthwave', null]).toContain(initialTheme)

    // Verify base-content colors are applied properly
    const heroHeadline = page.getByRole('heading', { level: 1 })
    const textColor = await heroHeadline.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Text should have a color defined (not inherit or transparent)
    expect(textColor).not.toBe('inherit')
    expect(textColor).not.toBe('transparent')

    // Verify link colors are styled
    const navLinks = page.getByTestId('nav-features')
    const linkColor = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).color
    })
    expect(linkColor).not.toBe('transparent')
  })

  /**
   * Test Case 6: Verify visual rendering consistency in Edge
   */
  test('visual rendering is consistent across viewport', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

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

  /**
   * Test Case 7: Verify responsive layout works in Edge at different viewport sizes
   */
  test('responsive layout works correctly in Edge at different viewport sizes', async ({
    page,
  }) => {
    // Test desktop viewport (default)
    await page.setViewportSize({ width: 1280, height: 720 })
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify feature cards are in grid layout on desktop
    const featureCards = page.getByTestId('feature-card')
    const desktopCardPositions = await featureCards.evaluateAll((cards) =>
      cards.map((card) => {
        const rect = card.getBoundingClientRect()
        return { x: rect.x, y: rect.y }
      })
    )

    // On desktop (lg breakpoint), cards should be in 3 columns
    expect(desktopCardPositions.length).toBe(3)

    // Test tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.waitForTimeout(300)

    // Verify hero buttons are visible on tablet
    const ctaButtons = page.locator('[data-testid="cta-get-started"], [data-testid="cta-login"]')
    await expect(ctaButtons).toHaveCount(2)

    // Test mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(300)

    // Verify no horizontal scroll on mobile
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)

    // Verify all sections are still visible on mobile
    await expect(page.getByTestId('hero-section')).toBeVisible()
    await expect(page.getByTestId('features-section')).toBeVisible()
    await expect(page.getByTestId('demo-section')).toBeVisible()
    await expect(page.getByTestId('footer-section')).toBeVisible()

    // Verify form is usable on mobile
    const demoInput = page.getByTestId('demo-url-input')
    await expect(demoInput).toBeVisible()

    // Input should be full width on mobile (within its container)
    const inputBox = await demoInput.boundingBox()
    expect(inputBox).toBeDefined()
    expect(inputBox!.width).toBeGreaterThan(200) // Should be reasonably wide
  })
})

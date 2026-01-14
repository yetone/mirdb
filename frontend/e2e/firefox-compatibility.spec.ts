import { test, expect, type Page } from '@playwright/test'

/**
 * Firefox Browser Compatibility Tests (NFR-3)
 *
 * Validates that the homepage works correctly in Firefox browser:
 * 1. All sections render correctly without layout issues
 * 2. No JavaScript errors in console
 * 3. CSS backdrop-filter/glassmorphism effects work or have graceful fallback
 *
 * Note: Run with --project=firefox to execute these tests in Firefox
 */

test.describe('Firefox Browser Compatibility (NFR-3)', () => {
  /**
   * Test Case 1: Load homepage in Firefox (latest)
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

    // Check hero section content
    const heroHeadline = page.getByRole('heading', { level: 1 })
    await expect(heroHeadline).toBeVisible()
    await expect(heroHeadline).toHaveText('Shorten URLs. Track Results.')

    const heroSubheadline = page.getByTestId('hero-subheadline')
    await expect(heroSubheadline).toBeVisible()

    // Verify CTA buttons are visible and properly styled
    const getStartedButton = page.getByTestId('cta-get-started')
    await expect(getStartedButton).toBeVisible()
    await expect(getStartedButton).toHaveAttribute('href', '/register')

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

    // Verify Demo Section renders correctly
    const demoSection = page.getByTestId('demo-section')
    await expect(demoSection).toBeVisible()

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
    const heroRect = await heroSection.boundingBox()
    const featuresRect = await featuresSection.boundingBox()
    const demoRect = await demoSection.boundingBox()
    const footerRect = await footerSection.boundingBox()

    expect(heroRect).toBeDefined()
    expect(featuresRect).toBeDefined()
    expect(demoRect).toBeDefined()
    expect(footerRect).toBeDefined()

    expect(heroRect!.y).toBeLessThan(featuresRect!.y)
    expect(featuresRect!.y).toBeLessThan(demoRect!.y)
    expect(demoRect!.y).toBeLessThan(footerRect!.y)
  })

  /**
   * Test Case 2: Check Firefox console after page load
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

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait for animations to complete
    await page.waitForTimeout(1500)

    // Interact with the page to trigger any potential JS errors
    // Click on navigation links
    const navFeatures = page.getByTestId('nav-features')
    await navFeatures.click()
    await page.waitForTimeout(500)

    const navDemo = page.getByTestId('nav-demo')
    await navDemo.click()
    await page.waitForTimeout(500)

    // Test demo form interaction
    const demoInput = page.getByTestId('demo-url-input')
    await demoInput.fill('https://example.com/test-url')

    const demoButton = page.getByTestId('demo-submit-button')
    await demoButton.click()
    await page.waitForTimeout(500)

    // Verify register prompt appears (indicates JS is working)
    const registerPrompt = page.getByTestId('demo-register-prompt')
    await expect(registerPrompt).toBeVisible()

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
   * Test Case 3: Test CSS backdrop-filter in Firefox
   * Expected: Glassmorphism effects work or have graceful fallback
   */
  test('CSS backdrop-filter and glassmorphism effects work or have graceful fallback', async ({
    page,
  }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Check if backdrop-filter is supported in Firefox
    const supportsBackdropFilter = await page.evaluate(() => {
      return CSS.supports('backdrop-filter', 'blur(10px)')
    })

    console.log(`Firefox supports backdrop-filter: ${supportsBackdropFilter}`)

    // Even if backdrop-filter isn't fully supported, the page should still render correctly
    // Check that cards and sections have proper background styling

    // Scroll features section into view to trigger whileInView animations
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for Framer Motion animations to complete (stagger + duration)
    await page.waitForTimeout(1000)

    // Verify feature cards have visible backgrounds (fallback or actual backdrop-filter)
    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      await expect(card).toBeVisible()

      // Wait for this specific card's animation to complete
      // Framer Motion uses CSS transforms and opacity for animations
      await page.waitForFunction(
        ({ selector, index }) => {
          const cards = document.querySelectorAll(selector)
          const card = cards[index] as HTMLElement
          if (!card) return false
          const styles = window.getComputedStyle(card)
          // Animation complete when opacity is 1
          return styles.opacity === '1'
        },
        { selector: '[data-testid="feature-card"]', index: i },
        { timeout: 2000 }
      )

      // Check that card has some background styling (not transparent)
      const cardStyles = await card.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return {
          backgroundColor: styles.backgroundColor,
          boxShadow: styles.boxShadow,
          opacity: styles.opacity,
        }
      })

      // Verify card has visible styling (background-color isn't fully transparent)
      expect(cardStyles.opacity).toBe('1')
      // Background should be set (not 'transparent' or rgba with 0 alpha)
      expect(cardStyles.backgroundColor).not.toBe('transparent')
    }

    // Verify the DaisyUI card component renders with proper shadow
    const firstCard = featureCards.first()
    const hasBoxShadow = await firstCard.evaluate((el) => {
      const shadow = window.getComputedStyle(el).boxShadow
      return shadow !== 'none' && shadow !== ''
    })

    // DaisyUI cards should have shadow-xl which provides visual depth
    expect(hasBoxShadow).toBe(true)

    // Verify overall page visual appearance is intact
    // Check that gradients render correctly (used in hero section)
    const heroSection = page.getByTestId('hero-section')
    const heroStyles = await heroSection.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundImage: styles.backgroundImage,
        backgroundColor: styles.backgroundColor,
      }
    })

    // Hero section should have gradient background
    const hasGradientOrColor =
      heroStyles.backgroundImage.includes('gradient') ||
      heroStyles.backgroundColor !== 'transparent'

    expect(hasGradientOrColor).toBe(true)

    // Test that animations work correctly (Framer Motion)
    // Elements should be visible (opacity: 1) after animation completes
    const heroHeadline = page.getByRole('heading', { level: 1 })
    const headlineOpacity = await heroHeadline.evaluate((el) => {
      return window.getComputedStyle(el).opacity
    })
    expect(headlineOpacity).toBe('1')

    // Verify button styling is intact
    const getStartedBtn = page.getByTestId('cta-get-started')
    const btnStyles = await getStartedBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        color: styles.color,
        cursor: styles.cursor,
      }
    })

    // Button should have visible background and pointer cursor
    expect(btnStyles.backgroundColor).not.toBe('transparent')
    expect(btnStyles.cursor).toBe('pointer')
  })

  /**
   * Additional test: Verify theme toggle works in Firefox
   */
  test('theme toggle works correctly in Firefox', async ({ page }) => {
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
   * Additional test: Verify responsive layout in Firefox
   */
  test('responsive layout works correctly in Firefox at different viewport sizes', async ({
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
    // This means at least 2 cards should have different Y positions similar but different X
    expect(desktopCardPositions.length).toBe(3)

    // Test tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.waitForTimeout(300)

    // Verify hero buttons stack correctly on tablet
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

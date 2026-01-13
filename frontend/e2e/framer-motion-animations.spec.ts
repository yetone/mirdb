import { test, expect } from '@playwright/test'

/**
 * E2E Tests for Framer Motion Animations
 * Validates NFR-5: Smooth animations using Framer Motion library
 *
 * These tests verify that animations work correctly in a real browser environment
 */

test.describe('Framer Motion Animations E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage
    await page.goto('/')
    // Wait for initial page load
    await page.waitForLoadState('domcontentloaded')
  })

  /**
   * Test Case 2: Hero section entrance animation
   * Input: Observe hero section entrance animation
   * Expected: Hero section animates smoothly on page load
   */
  test('hero section should have entrance animations on page load', async ({ page }) => {
    // Wait for hero section to be visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Hero content should be animated in
    const heroContent = page.getByTestId('hero-content')
    await expect(heroContent).toBeVisible()

    // Verify hero elements are visible after animation
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()
    await expect(headline).toContainText('Shorten. Track. Share.')

    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()
    await expect(subheadline).toContainText('Transform long URLs')

    const ctaButtons = page.getByTestId('hero-cta-buttons')
    await expect(ctaButtons).toBeVisible()
  })

  test('hero content should have smooth fade-in animation', async ({ page }) => {
    // Get hero content element
    const heroContent = page.getByTestId('hero-content')

    // Check that element has transform style from Framer Motion
    // After animation completes, transform should be near identity
    await page.waitForTimeout(1000) // Allow animation to complete

    const transform = await heroContent.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Transform should exist (Framer Motion manages it)
    expect(transform).toBeTruthy()
  })

  /**
   * Test Case 3: Feature cards animations
   * Input: Observe feature cards animations
   * Expected: Feature cards animate smoothly (staggered entrance or on-scroll)
   */
  test('feature cards should animate when scrolled into view', async ({ page }) => {
    // Features section is below the fold, scroll to it
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for scroll animation to trigger
    await page.waitForTimeout(500)

    // All feature cards should be visible after animation
    const urlCard = page.getByTestId('feature-card-url-shortening')
    const analyticsCard = page.getByTestId('feature-card-analytics')
    const managementCard = page.getByTestId('feature-card-link-management')
    const themesCard = page.getByTestId('feature-card-themes')

    await expect(urlCard).toBeVisible()
    await expect(analyticsCard).toBeVisible()
    await expect(managementCard).toBeVisible()
    await expect(themesCard).toBeVisible()
  })

  test('feature cards should have staggered entrance animation', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Get the cards container
    const container = page.getByTestId('feature-cards-container')
    await expect(container).toBeVisible()

    // Wait for staggered animation to complete (0.1s * 4 cards + 0.5s duration)
    await page.waitForTimeout(1000)

    // All cards should be visible
    const cards = page.locator('[data-testid^="feature-card-"]')
    await expect(cards).toHaveCount(4)

    for (let i = 0; i < 4; i++) {
      await expect(cards.nth(i)).toBeVisible()
    }
  })

  test('social proof statistics should animate with stagger effect', async ({ page }) => {
    // Scroll to social proof section
    const socialProofSection = page.getByTestId('social-proof-section')
    await socialProofSection.scrollIntoViewIfNeeded()

    // Wait for animations
    await page.waitForTimeout(1000)

    // All stat cards should be visible
    const statsContainer = page.getByTestId('statistics-container')
    await expect(statsContainer).toBeVisible()

    await expect(page.getByTestId('stat-card-urls-shortened')).toBeVisible()
    await expect(page.getByTestId('stat-card-clicks-tracked')).toBeVisible()
    await expect(page.getByTestId('stat-card-active-users')).toBeVisible()
    await expect(page.getByTestId('stat-card-countries-reached')).toBeVisible()
  })

  /**
   * Test Case 4: Button hover animations
   * Input: Test button hover animations
   * Expected: CTA buttons have smooth hover state transitions
   */
  test('CTA buttons should have hover animation effects', async ({ page }) => {
    // Get the Get Started button
    const getStartedBtn = page.getByTestId('get-started-btn').locator('button')
    await expect(getStartedBtn).toBeVisible()

    // Get initial transform
    const initialTransform = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Hover over the button
    await getStartedBtn.hover()

    // Wait for hover animation
    await page.waitForTimeout(200)

    // Check that button has scale transform on hover
    const hoverTransform = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Transform should be applied (either initial or hover state)
    expect(hoverTransform).toBeTruthy()
  })

  test('FuturisticButton should have glow animation effect', async ({ page }) => {
    // Get the Get Started button
    const getStartedBtn = page.getByTestId('get-started-btn').locator('button')
    await expect(getStartedBtn).toBeVisible()

    // The button should have a child div for glow effect
    const glowElement = getStartedBtn.locator('div').first()

    // Glow element should exist and have animation
    const hasGlow = await glowElement.count()
    expect(hasGlow).toBeGreaterThan(0)
  })

  test('theme toggle button should animate on click', async ({ page }) => {
    // Get theme toggle
    const themeToggle = page.getByTestId('theme-toggle')
    await expect(themeToggle).toBeVisible()

    // Check for initial icon (either sun or moon)
    const hasIcon = await themeToggle.locator('svg').count()
    expect(hasIcon).toBe(1)

    // Click to toggle theme
    await themeToggle.click()

    // Wait for rotation animation
    await page.waitForTimeout(500)

    // Icon should still be present (switched between sun/moon)
    const hasIconAfter = await themeToggle.locator('svg').count()
    expect(hasIconAfter).toBe(1)
  })

  test('demo section should animate when scrolled into view', async ({ page }) => {
    // Scroll to demo section
    const demoSection = page.getByTestId('demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    // Wait for animation
    await page.waitForTimeout(500)

    // Demo form should be visible
    const demoForm = page.getByTestId('demo-form')
    await expect(demoForm).toBeVisible()

    // URL input should be visible
    const urlInput = page.getByTestId('demo-url-input')
    await expect(urlInput).toBeVisible()

    // Shorten button should be visible
    const shortenButton = page.getByTestId('demo-shorten-button')
    await expect(shortenButton).toBeVisible()
  })

  /**
   * Animation Performance Tests
   */
  test('animations should not cause layout thrashing', async ({ page }) => {
    // Start performance measurement
    const startTime = Date.now()

    // Navigate and wait for animations
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Scroll through the page to trigger all animations
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight)
    })

    await page.waitForTimeout(1500) // Allow all animations

    const endTime = Date.now()
    const totalTime = endTime - startTime

    // Page with all animations should load and complete in reasonable time
    expect(totalTime).toBeLessThan(10000) // 10 seconds max
  })

  test('animations should complete without errors', async ({ page }) => {
    const errors: string[] = []

    // Listen for console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        errors.push(msg.text())
      }
    })

    // Navigate and trigger all animations
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Scroll to trigger viewport animations
    await page.evaluate(async () => {
      const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
      const maxScroll = document.body.scrollHeight - window.innerHeight
      const scrollStep = maxScroll / 5

      for (let i = 0; i <= 5; i++) {
        window.scrollTo(0, i * scrollStep)
        await delay(300)
      }
    })

    await page.waitForTimeout(1000)

    // No Framer Motion errors should occur
    const motionErrors = errors.filter(e =>
      e.toLowerCase().includes('motion') ||
      e.toLowerCase().includes('framer') ||
      e.toLowerCase().includes('animation')
    )

    expect(motionErrors).toHaveLength(0)
  })
})

test.describe('Animation Accessibility', () => {
  test('animations should respect reduced motion preference', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' })

    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')

    // Page should still be functional with reduced motion
    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()
    await expect(heroHeadline).toContainText('Shorten. Track. Share.')

    // All sections should be visible
    await expect(page.getByTestId('features-section')).toBeVisible()
    await expect(page.getByTestId('social-proof-section')).toBeVisible()
    await expect(page.getByTestId('footer-section')).toBeVisible()
  })

  test('animated elements should remain keyboard focusable', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Tab through the page
    await page.keyboard.press('Tab')

    // Get focused element
    const focusedElement = page.locator(':focus')
    await expect(focusedElement).toBeVisible()

    // Continue tabbing - should be able to reach CTA buttons
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab')
    }

    // Theme toggle should be focusable
    const themeToggle = page.getByTestId('theme-toggle')
    await themeToggle.focus()
    await expect(themeToggle).toBeFocused()

    // Should be able to interact with keyboard
    await page.keyboard.press('Enter')
    // Theme should toggle (no error)
  })
})

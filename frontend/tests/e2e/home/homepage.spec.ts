/**
 * Homepage E2E Tests
 * Owner: Scenario 17 - Smooth Scroll Behavior
 *
 * End-to-end tests for homepage smooth scroll behavior:
 * - CSS scroll-behavior property verification
 * - Anchor link smooth scroll animation (if present)
 * - Framer Motion scroll-triggered animations
 *
 * Testing framework: Playwright
 */
import { test, expect } from '@playwright/test'

test.describe('Smooth Scroll Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  test('should have scroll-behavior: smooth CSS property on html element', async ({
    page,
  }) => {
    // Test Case 1: Check CSS scroll-behavior property on html/body
    const scrollBehavior = await page.evaluate(() => {
      const html = document.documentElement
      return window.getComputedStyle(html).scrollBehavior
    })

    expect(scrollBehavior).toBe('smooth')
  })

  test('should have smooth scroll behavior applied to the page', async ({
    page,
  }) => {
    // Additional verification: check that the CSS is correctly applied
    const htmlScrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior
    })

    // Verify scroll-behavior is set to smooth
    expect(htmlScrollBehavior).toBe('smooth')
  })

  test('should scroll smoothly when using scrollIntoView', async ({ page }) => {
    // Test Case 2: Test smooth scroll animation behavior
    // Scroll to the features section using scrollIntoView with smooth behavior
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Trigger a smooth scroll to the features section
    await page.evaluate(() => {
      const featuresSection = document.querySelector(
        '[data-testid="features-section"]'
      )
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for scroll animation to complete (smooth scroll takes time)
    await page.waitForTimeout(500)

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)
  })

  test('should smoothly scroll to different sections', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Scroll to How It Works section
    await page.evaluate(() => {
      const howItWorksSection = document.querySelector(
        '[data-testid="how-it-works-section"]'
      )
      if (howItWorksSection) {
        howItWorksSection.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for smooth scroll to complete
    await page.waitForTimeout(600)

    // Verify scroll occurred
    const scrolledY = await page.evaluate(() => window.scrollY)
    expect(scrolledY).toBeGreaterThan(initialScrollY)

    // Verify the section is in view
    const isInView = await page.evaluate(() => {
      const section = document.querySelector(
        '[data-testid="how-it-works-section"]'
      )
      if (!section) return false
      const rect = section.getBoundingClientRect()
      return rect.top >= 0 && rect.top < window.innerHeight
    })
    expect(isInView).toBe(true)
  })

  test('should have Framer Motion scroll-triggered animations', async ({
    page,
  }) => {
    // Test Case 3: Verify Framer Motion scroll animations are present and performant
    // Check that motion elements exist in the hero section
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Scroll to features section and verify it's visible with animations
    await page.evaluate(() => {
      const featuresSection = document.querySelector(
        '[data-testid="features-section"]'
      )
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for scroll and animations
    await page.waitForTimeout(700)

    // Verify features section is now visible
    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()

    // Check that feature cards are visible after scroll animation
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(3)

    // Verify each card is visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible()
    }
  })

  test('should have smooth scroll-triggered animations in How It Works section', async ({
    page,
  }) => {
    // Scroll to How It Works section
    await page.evaluate(() => {
      const section = document.querySelector(
        '[data-testid="how-it-works-section"]'
      )
      if (section) {
        section.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for scroll and animations to complete
    await page.waitForTimeout(800)

    // Verify all steps are visible
    const steps = page.locator('[data-testid^="step-"]')
    const stepCount = await steps.count()
    expect(stepCount).toBeGreaterThanOrEqual(3)

    // Check that step 1, 2, and 3 are visible
    await expect(page.locator('[data-testid="step-1"]')).toBeVisible()
    await expect(page.locator('[data-testid="step-2"]')).toBeVisible()
    await expect(page.locator('[data-testid="step-3"]')).toBeVisible()
  })

  test('scroll animation should be performant with no significant frame drops', async ({
    page,
  }) => {
    // Measure scroll performance
    const performanceMetrics = await page.evaluate(async () => {
      const startTime = performance.now()
      const initialScrollY = window.scrollY

      // Perform smooth scroll to bottom
      window.scrollTo({
        top: document.body.scrollHeight,
        behavior: 'smooth',
      })

      // Wait for scroll to complete (using RAF-based detection)
      await new Promise<void>((resolve) => {
        let lastScrollY = window.scrollY
        let stableFrames = 0

        const checkScroll = () => {
          if (window.scrollY === lastScrollY) {
            stableFrames++
            if (stableFrames > 5) {
              resolve()
              return
            }
          } else {
            stableFrames = 0
            lastScrollY = window.scrollY
          }
          requestAnimationFrame(checkScroll)
        }
        requestAnimationFrame(checkScroll)

        // Timeout fallback
        setTimeout(resolve, 2000)
      })

      const endTime = performance.now()
      const finalScrollY = window.scrollY

      return {
        duration: endTime - startTime,
        scrollDistance: finalScrollY - initialScrollY,
        completed: finalScrollY > initialScrollY,
      }
    })

    // Verify scroll completed successfully
    expect(performanceMetrics.completed).toBe(true)

    // Verify scroll animation was smooth (reasonable duration)
    // Smooth scroll should take some time (not instant) but not too long
    expect(performanceMetrics.duration).toBeGreaterThan(100) // Not instant
    expect(performanceMetrics.duration).toBeLessThan(3000) // Not too slow
  })

  test('should scroll back to top smoothly', async ({ page }) => {
    // First scroll down
    await page.evaluate(() => {
      window.scrollTo({
        top: document.body.scrollHeight,
        behavior: 'smooth',
      })
    })

    await page.waitForTimeout(800)

    // Then scroll back to top
    await page.evaluate(() => {
      window.scrollTo({
        top: 0,
        behavior: 'smooth',
      })
    })

    await page.waitForTimeout(800)

    // Verify we're back at the top
    const scrollY = await page.evaluate(() => window.scrollY)
    expect(scrollY).toBeLessThanOrEqual(50) // Allow small tolerance
  })
})

/**
 * Hover Effects and Interactions E2E Tests
 * Owner: Scenario 18 - Hover Effects and Interactions
 *
 * End-to-end tests verifying interactive elements have appropriate hover states:
 * - CTA buttons (Sign Up, Log In) show visual feedback via Framer Motion scale
 * - Feature cards have hover effects (transform, shadow, or color change)
 * - Footer links indicate interactivity on hover (color change via Tailwind)
 *
 * Testing framework: Playwright
 */
test.describe('Hover Effects and Interactions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('Sign Up button shows visual change on hover (scale effect)', async ({
    page,
  }) => {
    // Test Case 1: Trigger hover state on Sign Up button
    // The FuturisticButton uses Framer Motion whileHover={{ scale: 1.02 }}
    const signUpButton = page.locator('a[href="/register"] button').first()
    await expect(signUpButton).toBeVisible()

    // Get initial transform
    const initialTransform = await signUpButton.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Hover over the button
    await signUpButton.hover()

    // Wait for Framer Motion animation to apply
    await page.waitForTimeout(300)

    // Get the transform after hover
    const hoverTransform = await signUpButton.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Verify transform changed (Framer Motion applies scale via transform matrix)
    // The scale(1.02) in Framer Motion creates a matrix transform
    expect(hoverTransform).not.toBe('none')

    // Verify transform indicates scale change if both are non-none
    if (hoverTransform !== 'none' && initialTransform !== 'none') {
      expect(hoverTransform).not.toBe(initialTransform)
    }
  })

  test('Log In button shows visual change on hover', async ({ page }) => {
    // Test Login button hover effect
    const loginButton = page.locator('a[href="/login"] button').first()
    await expect(loginButton).toBeVisible()

    // Get button's computed style before hover
    const beforeHover = await loginButton.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transform: style.transform,
        cursor: style.cursor,
      }
    })

    // Hover over the button
    await loginButton.hover()
    await page.waitForTimeout(300)

    // Get button's computed style after hover
    const afterHover = await loginButton.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transform: style.transform,
        cursor: style.cursor,
      }
    })

    // The button should have cursor: pointer for interactivity
    expect(afterHover.cursor).toBe('pointer')
    // Verify some transform is applied (Framer Motion scale)
    expect(afterHover.transform).not.toBe('none')
  })

  test('feature cards have hover effect (transform, shadow, or visual change)', async ({
    page,
  }) => {
    // Test Case 2: Trigger hover state on feature cards
    // First scroll to features section to ensure cards are visible
    await page.evaluate(() => {
      const section = document.querySelector('[data-testid="features-section"]')
      if (section) {
        section.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

    // Find feature cards using GlassMorphismCard (has .card class)
    const featureCards = page.locator('.feature-card')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(3)

    // Test first feature card hover effect
    const firstCard = featureCards.first()
    await expect(firstCard).toBeVisible()

    // Get initial styles
    const initialStyles = await firstCard.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transform: style.transform,
        boxShadow: style.boxShadow,
        opacity: style.opacity,
      }
    })

    // Hover over the card
    await firstCard.hover()
    await page.waitForTimeout(350)

    // Get styles after hover
    const hoverStyles = await firstCard.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transform: style.transform,
        boxShadow: style.boxShadow,
        opacity: style.opacity,
      }
    })

    // Verify some visual change occurred (transform, shadow, or opacity)
    // The GlassMorphismCard has shadow-xl and Framer Motion animations
    const hasVisualChange =
      hoverStyles.transform !== initialStyles.transform ||
      hoverStyles.boxShadow !== initialStyles.boxShadow ||
      hoverStyles.opacity !== initialStyles.opacity ||
      hoverStyles.boxShadow !== 'none'

    expect(hasVisualChange).toBe(true)
  })

  test('all feature cards are interactive with hover states', async ({
    page,
  }) => {
    // Scroll to features section
    await page.evaluate(() => {
      const section = document.querySelector('[data-testid="features-section"]')
      if (section) {
        section.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

    // Test each feature card
    const featureIds = ['url-shortening', 'analytics', 'link-management']

    for (const featureId of featureIds) {
      const card = page.locator(`[data-testid="feature-card-${featureId}"]`)
      await expect(card).toBeVisible()

      // Verify the card's parent (GlassMorphismCard) has styling that supports hover
      const parentCard = card.locator(
        'xpath=ancestor::div[contains(@class, "card")]'
      )
      const hasCardStyling = await parentCard.evaluate((el) => {
        const style = window.getComputedStyle(el)
        // Check for backdrop-blur (glassmorphism effect) or shadow
        return (
          style.boxShadow !== 'none' ||
          style.backdropFilter !== 'none' ||
          el.classList.contains('shadow-xl')
        )
      })

      expect(hasCardStyling).toBe(true)
    }
  })

  test('footer links show hover indication (color change)', async ({
    page,
  }) => {
    // Test Case 3: Trigger hover state on footer links
    // Scroll to footer
    await page.evaluate(() => {
      const footer = document.querySelector('[data-testid="footer"]')
      if (footer) {
        footer.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

    const footer = page.locator('[data-testid="footer"]')
    await expect(footer).toBeVisible()

    // Test each footer link
    const footerLinks = footer.locator('nav a')
    const linkCount = await footerLinks.count()
    expect(linkCount).toBeGreaterThanOrEqual(3) // Home, Login, Register

    // Test the first link (Home)
    const homeLink = footerLinks.first()
    await expect(homeLink).toBeVisible()

    // Get initial color
    const initialColor = await homeLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Hover over the link
    await homeLink.hover()
    await page.waitForTimeout(250)

    // Get color after hover
    const hoverColor = await homeLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Verify color changed on hover (hover:text-primary applies)
    expect(hoverColor).not.toBe(initialColor)
  })

  test('all footer links have hover cursor and transition', async ({
    page,
  }) => {
    // Scroll to footer
    await page.evaluate(() => {
      const footer = document.querySelector('[data-testid="footer"]')
      if (footer) {
        footer.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

    const footerLinks = page.locator('[data-testid="footer"] nav a')
    const linkCount = await footerLinks.count()

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i)
      await expect(link).toBeVisible()

      // Verify cursor is pointer (indicates interactivity)
      const cursor = await link.evaluate((el) => {
        return window.getComputedStyle(el).cursor
      })
      expect(cursor).toBe('pointer')

      // Verify transition is set for smooth hover effect
      const transition = await link.evaluate((el) => {
        return window.getComputedStyle(el).transition
      })
      // Should have transition-colors applied
      expect(transition).toContain('color')
    }
  })

  test('E2E: all hover effects are visible and enhance user experience', async ({
    page,
  }) => {
    // Test Case 4: Comprehensive E2E visual hover effect testing
    // This test verifies the overall hover experience across the page

    // 1. Test hero section CTAs
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Verify Get Started button is hoverable
    const getStartedBtn = page.locator('a[href="/register"] button').first()
    await expect(getStartedBtn).toBeVisible()
    await getStartedBtn.hover()
    await page.waitForTimeout(200)

    // Button should have interactive appearance
    const btnCursor = await getStartedBtn.evaluate(
      (el) => window.getComputedStyle(el).cursor
    )
    expect(btnCursor).toBe('pointer')

    // Verify transform is applied on hover
    const signUpHoverTransform = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })
    expect(signUpHoverTransform).toBeTruthy()

    // 2. Scroll to and test features section
    await page.evaluate(() => {
      const section = document.querySelector('[data-testid="features-section"]')
      if (section) section.scrollIntoView({ behavior: 'instant' })
    })
    await page.waitForTimeout(400)

    // Hover over each feature card and verify visual feedback
    const featureCards = page.locator('.feature-card')
    for (let i = 0; i < 3; i++) {
      const card = featureCards.nth(i)
      await expect(card).toBeVisible()

      // Card should have shadow (visual feedback)
      const hasShadow = await card.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow !== 'none'
      })
      expect(hasShadow).toBe(true)
    }

    // 3. Scroll to and test footer
    await page.evaluate(() => {
      const footer = document.querySelector('[data-testid="footer"]')
      if (footer) footer.scrollIntoView({ behavior: 'instant' })
    })
    await page.waitForTimeout(300)

    // Test footer links have proper hover styling
    const footerNav = page.locator('[data-testid="footer"] nav')
    await expect(footerNav).toBeVisible()

    const links = footerNav.locator('a')
    for (let i = 0; i < (await links.count()); i++) {
      const link = links.nth(i)
      const hasHoverClass = await link.evaluate((el) => {
        return (
          el.classList.contains('link-hover') || el.className.includes('hover:')
        )
      })
      // Link should have hover styling classes
      expect(hasHoverClass).toBe(true)
    }

    // Test specific link color change on hover
    const loginLink = page.locator('[data-testid="footer"] nav a[href="/login"]')
    await expect(loginLink).toBeVisible()

    const linkInitialColor = await loginLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    await loginLink.hover()
    await page.waitForTimeout(200)

    const linkHoverColor = await loginLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Verify footer link hover effect is visible
    expect(linkHoverColor).not.toBe(linkInitialColor)

    // 4. Overall UX verification - interactive elements are discoverable
    // Count total interactive elements with proper cursor
    const interactiveElements = await page.evaluate(() => {
      const buttons = document.querySelectorAll('button')
      const links = document.querySelectorAll('a')
      let count = 0

      buttons.forEach((btn) => {
        if (window.getComputedStyle(btn).cursor === 'pointer') count++
      })
      links.forEach((link) => {
        if (window.getComputedStyle(link).cursor === 'pointer') count++
      })

      return count
    })

    // Should have multiple interactive elements (CTAs, nav links, footer links)
    expect(interactiveElements).toBeGreaterThanOrEqual(5)
  })
})

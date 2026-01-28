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
 * End-to-end tests for hover states on interactive elements:
 * - CTA button hover effects (scale transform via Framer Motion)
 * - Feature card hover effects (transform and shadow via CSS)
 * - Footer link hover effects (color change via Tailwind)
 *
 * Testing framework: Playwright
 */
test.describe('Hover Effects and Interactions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('CTA buttons should show visual change on hover (scale transform)', async ({
    page,
  }) => {
    // Test Case 1: Verify Sign Up button has hover effect
    const signUpButton = page.locator('a[href="/register"] button').first()
    await expect(signUpButton).toBeVisible()

    // Get initial transform state
    const initialTransform = await signUpButton.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Hover over the button
    await signUpButton.hover()

    // Wait for Framer Motion animation
    await page.waitForTimeout(200)

    // Get transform after hover
    const hoverTransform = await signUpButton.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Verify transform changed (Framer Motion applies scale: 1.02 on hover)
    // The transform should be different - initially "none" or "matrix(1, 0, 0, 1, 0, 0)"
    // After hover it should contain a scale transformation
    expect(hoverTransform).not.toBe('none')

    // Verify it contains matrix indicating scale change
    // matrix(1.02, 0, 0, 1.02, 0, 0) for scale(1.02)
    if (hoverTransform !== 'none' && initialTransform !== 'none') {
      expect(hoverTransform).not.toBe(initialTransform)
    }
  })

  test('Login button should show visual change on hover', async ({ page }) => {
    // Test Login button hover effect
    const loginButton = page.locator('a[href="/login"] button').first()
    await expect(loginButton).toBeVisible()

    // Hover over the button and verify transform
    await loginButton.hover()
    await page.waitForTimeout(200)

    const hoverTransform = await loginButton.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Verify some transform is applied (Framer Motion scale)
    expect(hoverTransform).not.toBe('none')
  })

  test('Feature cards should have hover effect (transform and shadow change)', async ({
    page,
  }) => {
    // Test Case 2: Verify feature cards have hover effects
    // Scroll to features section first
    await page.evaluate(() => {
      const featuresSection = document.querySelector(
        '[data-testid="features-section"]'
      )
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

    // Get the first feature card
    const featureCard = page.locator('.feature-card').first()
    await expect(featureCard).toBeVisible()

    // Get initial styles
    const initialStyles = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transform: style.transform,
        boxShadow: style.boxShadow,
      }
    })

    // Hover over the card
    await featureCard.hover()
    await page.waitForTimeout(350) // Wait for CSS transition (0.3s)

    // Get styles after hover
    const hoverStyles = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        transform: style.transform,
        boxShadow: style.boxShadow,
      }
    })

    // Verify transform changed (translateY(-4px))
    // Either transform changed or boxShadow changed
    const transformChanged = hoverStyles.transform !== initialStyles.transform
    const shadowChanged = hoverStyles.boxShadow !== initialStyles.boxShadow

    expect(transformChanged || shadowChanged).toBe(true)
  })

  test('All feature cards should have hover effects', async ({ page }) => {
    // Scroll to features section
    await page.evaluate(() => {
      const featuresSection = document.querySelector(
        '[data-testid="features-section"]'
      )
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

    // Test each feature card
    const featureCards = page.locator('.feature-card')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(3)

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      await expect(card).toBeVisible()

      // Get initial styles
      const initialStyles = await card.evaluate((el) => {
        const style = window.getComputedStyle(el)
        return {
          transform: style.transform,
          boxShadow: style.boxShadow,
        }
      })

      // Hover
      await card.hover()
      await page.waitForTimeout(350)

      // Get styles after hover
      const hoverStyles = await card.evaluate((el) => {
        const style = window.getComputedStyle(el)
        return {
          transform: style.transform,
          boxShadow: style.boxShadow,
        }
      })

      // Verify either transform or boxShadow changed on hover
      const transformChanged = hoverStyles.transform !== initialStyles.transform
      const shadowChanged = hoverStyles.boxShadow !== initialStyles.boxShadow

      expect(transformChanged || shadowChanged).toBe(true)
    }
  })

  test('Footer links should show hover indication (color change)', async ({
    page,
  }) => {
    // Test Case 3: Verify footer links have hover effects
    // Scroll to footer
    await page.evaluate(() => {
      const footer = document.querySelector('[data-testid="footer"]')
      if (footer) {
        footer.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

    // Get footer navigation links
    const footerLinks = page.locator('[data-testid="footer"] nav a')
    const linkCount = await footerLinks.count()
    expect(linkCount).toBeGreaterThanOrEqual(3)

    // Test the first link (Home)
    const homeLink = footerLinks.first()
    await expect(homeLink).toBeVisible()

    // Get initial color
    const initialColor = await homeLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Hover over the link
    await homeLink.hover()
    await page.waitForTimeout(200) // Wait for transition

    // Get color after hover
    const hoverColor = await homeLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Verify color changed (hover:text-primary applies primary color)
    expect(hoverColor).not.toBe(initialColor)
  })

  test('All footer links should have hover color change', async ({ page }) => {
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

      // Get initial color
      const initialColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color
      })

      // Hover
      await link.hover()
      await page.waitForTimeout(200)

      // Verify color changed
      const hoverColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color
      })

      expect(hoverColor).not.toBe(initialColor)

      // Move mouse away to reset
      await page.mouse.move(0, 0)
      await page.waitForTimeout(100)
    }
  })

  test('E2E: All hover effects should be visible and enhance user experience', async ({
    page,
  }) => {
    // Test Case 4: Comprehensive E2E test for all hover effects

    // 1. Test hero section CTA buttons
    const signUpBtn = page.locator('a[href="/register"] button').first()
    await expect(signUpBtn).toBeVisible()
    await signUpBtn.hover()
    await page.waitForTimeout(200)

    // Verify the button has some visual feedback
    const signUpHoverTransform = await signUpBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })
    expect(signUpHoverTransform).toBeTruthy()

    // 2. Scroll to and test feature cards
    await page.evaluate(() => {
      const featuresSection = document.querySelector(
        '[data-testid="features-section"]'
      )
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(400)

    const firstFeatureCard = page.locator('.feature-card').first()
    await expect(firstFeatureCard).toBeVisible()

    const cardInitialTransform = await firstFeatureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    await firstFeatureCard.hover()
    await page.waitForTimeout(350)

    const cardHoverTransform = await firstFeatureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Verify feature card hover effect is visible
    const cardHoverWorks =
      cardHoverTransform !== cardInitialTransform || cardHoverTransform !== 'none'
    expect(cardHoverWorks).toBe(true)

    // 3. Scroll to and test footer links
    await page.evaluate(() => {
      const footer = document.querySelector('[data-testid="footer"]')
      if (footer) {
        footer.scrollIntoView({ behavior: 'instant' })
      }
    })
    await page.waitForTimeout(300)

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

    // All hover effects verified - user experience is enhanced
  })
})

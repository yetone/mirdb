import { test, expect } from '@playwright/test'

/**
 * Hover States and Interactions Tests (UX)
 *
 * Verifies that interactive elements have proper hover states:
 * 1. CTA buttons display hover state (color change, scale, or shadow)
 * 2. Feature cards display hover effect (elevation, glow, or transform)
 * 3. Footer links display hover state (underline or color change)
 */

test.describe('Hover States and Interactions', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/', { waitUntil: 'networkidle' })
  })

  /**
   * Test Case 1: Hover over 'Get Started' button
   * Expected: Button displays hover state (color change, scale, or shadow)
   */
  test('Get Started button displays hover state', async ({ page }) => {
    // Find the Get Started button
    const getStartedButton = page.getByTestId('cta-get-started')
    await expect(getStartedButton).toBeVisible()

    // Get the initial styles before hover
    const initialStyles = await getStartedButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        backgroundColor: computed.backgroundColor,
        boxShadow: computed.boxShadow,
        transform: computed.transform,
        scale: computed.scale,
        filter: computed.filter,
      }
    })

    // Hover over the button
    await getStartedButton.hover()

    // Wait for transition to complete (button has transition-all duration-200)
    await page.waitForTimeout(300)

    // Get the styles after hover
    const hoverStyles = await getStartedButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        backgroundColor: computed.backgroundColor,
        boxShadow: computed.boxShadow,
        transform: computed.transform,
        scale: computed.scale,
        filter: computed.filter,
      }
    })

    // Verify that at least one visual property changed on hover
    // DaisyUI btn-primary has hover states that change background color or filter
    const hasHoverEffect =
      initialStyles.backgroundColor !== hoverStyles.backgroundColor ||
      initialStyles.boxShadow !== hoverStyles.boxShadow ||
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.scale !== hoverStyles.scale ||
      initialStyles.filter !== hoverStyles.filter

    expect(hasHoverEffect).toBe(true)

    // Verify the button has transition classes for smooth hover
    const hasTransition = await getStartedButton.evaluate((el) => {
      const classes = el.className
      return (
        classes.includes('transition') ||
        classes.includes('btn') // DaisyUI btn class includes transitions
      )
    })
    expect(hasTransition).toBe(true)
  })

  /**
   * Test Case 1b: Hover over 'Login' button (secondary CTA)
   * Expected: Button displays hover state
   */
  test('Login button displays hover state', async ({ page }) => {
    // Find the Login button
    const loginButton = page.getByTestId('cta-login')
    await expect(loginButton).toBeVisible()

    // Get the initial styles before hover
    const initialStyles = await loginButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        backgroundColor: computed.backgroundColor,
        borderColor: computed.borderColor,
        color: computed.color,
        boxShadow: computed.boxShadow,
        filter: computed.filter,
      }
    })

    // Hover over the button
    await loginButton.hover()

    // Wait for transition to complete
    await page.waitForTimeout(300)

    // Get the styles after hover
    const hoverStyles = await loginButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        backgroundColor: computed.backgroundColor,
        borderColor: computed.borderColor,
        color: computed.color,
        boxShadow: computed.boxShadow,
        filter: computed.filter,
      }
    })

    // Verify that at least one visual property changed on hover
    // btn-outline changes background/text color on hover
    const hasHoverEffect =
      initialStyles.backgroundColor !== hoverStyles.backgroundColor ||
      initialStyles.borderColor !== hoverStyles.borderColor ||
      initialStyles.color !== hoverStyles.color ||
      initialStyles.boxShadow !== hoverStyles.boxShadow ||
      initialStyles.filter !== hoverStyles.filter

    expect(hasHoverEffect).toBe(true)
  })

  /**
   * Test Case 2: Hover over feature cards
   * Expected: Cards display hover effect (elevation, glow, or transform)
   */
  test('feature cards display hover effect', async ({ page }) => {
    // Scroll to features section to ensure cards are visible
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for Framer Motion animations to complete
    await page.waitForTimeout(1500)

    // Get the first feature card
    const featureCards = page.getByTestId('feature-card')
    const firstCard = featureCards.first()
    await expect(firstCard).toBeVisible()

    // Get the initial styles before hover
    const initialStyles = await firstCard.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        boxShadow: computed.boxShadow,
        transform: computed.transform,
      }
    })

    // Hover over the card
    await firstCard.hover()

    // Wait for transition to complete (card has transition-shadow duration-300)
    await page.waitForTimeout(400)

    // Get the styles after hover
    const hoverStyles = await firstCard.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        boxShadow: computed.boxShadow,
        transform: computed.transform,
      }
    })

    // Verify that box-shadow changed (hover:shadow-2xl)
    // The card uses shadow-xl initially and hover:shadow-2xl on hover
    const shadowChanged = initialStyles.boxShadow !== hoverStyles.boxShadow

    expect(shadowChanged).toBe(true)

    // Verify the card has the hover:shadow-2xl class
    const hasHoverShadowClass = await firstCard.evaluate((el) => {
      return el.className.includes('hover:shadow-2xl')
    })
    expect(hasHoverShadowClass).toBe(true)

    // Verify the card has transition-shadow class for smooth effect
    const hasTransitionShadow = await firstCard.evaluate((el) => {
      return el.className.includes('transition-shadow')
    })
    expect(hasTransitionShadow).toBe(true)
  })

  /**
   * Test Case 2b: All feature cards have hover effects
   * Expected: Each card displays hover effect
   */
  test('all feature cards have consistent hover effects', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for animations
    await page.waitForTimeout(1500)

    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()

    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Check each card has the necessary hover classes
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      await expect(card).toBeVisible()

      const cardClasses = await card.evaluate((el) => el.className)

      // Verify hover shadow class
      expect(cardClasses).toContain('hover:shadow-2xl')

      // Verify transition class
      expect(cardClasses).toContain('transition-shadow')
    }
  })

  /**
   * Test Case 3: Hover over footer links
   * Expected: Links display hover state (underline or color change)
   */
  test('footer links display hover state', async ({ page }) => {
    // Scroll to footer section
    const footerSection = page.getByTestId('footer-section')
    await footerSection.scrollIntoViewIfNeeded()
    await expect(footerSection).toBeVisible()

    // Find the Home link in footer
    const homeLink = footerSection.getByRole('link', { name: 'Home' })
    await expect(homeLink).toBeVisible()

    // Get the initial color before hover
    const initialColor = await homeLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Hover over the link
    await homeLink.hover()

    // Wait for transition
    await page.waitForTimeout(200)

    // Get the color after hover
    const hoverColor = await homeLink.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    // Verify the color changed (hover:text-primary)
    expect(initialColor).not.toBe(hoverColor)

    // Verify the link has the necessary hover classes
    const linkClasses = await homeLink.evaluate((el) => el.className)
    expect(linkClasses).toContain('link-hover')
    expect(linkClasses).toContain('hover:text-primary')
    expect(linkClasses).toContain('transition-colors')
  })

  /**
   * Test Case 3b: All footer navigation links have hover states
   * Expected: Each link displays hover effect
   */
  test('all footer navigation links have hover states', async ({ page }) => {
    // Scroll to footer section
    const footerSection = page.getByTestId('footer-section')
    await footerSection.scrollIntoViewIfNeeded()

    // Test main navigation links
    const navLinks = ['Home', 'Login', 'Register']

    for (const linkName of navLinks) {
      const link = footerSection.getByRole('link', { name: linkName })
      await expect(link).toBeVisible()

      // Verify link has hover classes
      const linkClasses = await link.evaluate((el) => el.className)
      expect(linkClasses).toContain('link-hover')
      expect(linkClasses).toContain('hover:text-primary')
      expect(linkClasses).toContain('transition-colors')

      // Test actual hover color change
      const initialColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color
      })

      await link.hover()
      await page.waitForTimeout(200)

      const hoverColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color
      })

      expect(initialColor).not.toBe(hoverColor)
    }
  })

  /**
   * Test Case 3c: Footer legal links have hover states
   * Expected: Privacy and Terms links display hover effect
   */
  test('footer legal links have hover states', async ({ page }) => {
    // Scroll to footer section
    const footerSection = page.getByTestId('footer-section')
    await footerSection.scrollIntoViewIfNeeded()

    // Test legal links
    const legalLinks = ['Privacy', 'Terms']

    for (const linkName of legalLinks) {
      const link = footerSection.getByRole('link', { name: linkName })
      await expect(link).toBeVisible()

      // Verify link has hover classes
      const linkClasses = await link.evaluate((el) => el.className)
      expect(linkClasses).toContain('link-hover')
      expect(linkClasses).toContain('hover:text-primary')
      expect(linkClasses).toContain('transition-colors')

      // Test actual hover color change
      const initialColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color
      })

      await link.hover()
      await page.waitForTimeout(200)

      const hoverColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color
      })

      expect(initialColor).not.toBe(hoverColor)
    }
  })

  /**
   * Test Case: Navigation links in hero section have hover states
   * Expected: Features and Try It links display hover effect
   */
  test('hero navigation links have hover states', async ({ page }) => {
    // Test nav-features link
    const navFeatures = page.getByTestId('nav-features')
    await expect(navFeatures).toBeVisible()

    const navFeaturesClasses = await navFeatures.evaluate((el) => el.className)
    expect(navFeaturesClasses).toContain('link-hover')
    expect(navFeaturesClasses).toContain('hover:text-primary')
    expect(navFeaturesClasses).toContain('transition-colors')

    // Test nav-demo link
    const navDemo = page.getByTestId('nav-demo')
    await expect(navDemo).toBeVisible()

    const navDemoClasses = await navDemo.evaluate((el) => el.className)
    expect(navDemoClasses).toContain('link-hover')
    expect(navDemoClasses).toContain('hover:text-primary')
    expect(navDemoClasses).toContain('transition-colors')

    // Test actual hover effect on nav-features
    const initialColor = await navFeatures.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    await navFeatures.hover()
    await page.waitForTimeout(200)

    const hoverColor = await navFeatures.evaluate((el) => {
      return window.getComputedStyle(el).color
    })

    expect(initialColor).not.toBe(hoverColor)
  })

  /**
   * Test Case: Verify transition durations are appropriate
   * Expected: Transitions are smooth (not instant, not too slow)
   */
  test('hover transitions are smooth and appropriate', async ({ page }) => {
    // Check button transition duration
    const getStartedButton = page.getByTestId('cta-get-started')
    const buttonTransition = await getStartedButton.evaluate((el) => {
      return window.getComputedStyle(el).transitionDuration
    })

    // Button should have a transition duration (not 0s)
    expect(buttonTransition).not.toBe('0s')

    // Scroll to features
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(1500)

    // Check card transition duration
    const firstCard = page.getByTestId('feature-card').first()
    const cardTransition = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).transitionDuration
    })

    // Card should have a transition duration for shadow
    expect(cardTransition).not.toBe('0s')

    // Scroll to footer
    const footerSection = page.getByTestId('footer-section')
    await footerSection.scrollIntoViewIfNeeded()

    // Check link transition duration
    const homeLink = footerSection.getByRole('link', { name: 'Home' })
    const linkTransition = await homeLink.evaluate((el) => {
      return window.getComputedStyle(el).transitionDuration
    })

    // Link should have a transition duration
    expect(linkTransition).not.toBe('0s')
  })
})

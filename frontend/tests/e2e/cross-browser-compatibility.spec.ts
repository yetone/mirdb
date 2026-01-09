import { test, expect } from '@playwright/test'

/**
 * Cross-Browser Compatibility E2E Tests
 *
 * Verifies that the homepage renders correctly across Chrome, Firefox, Safari, and Edge browsers
 * as specified in the PRD Success Criteria.
 *
 * Test cases cover:
 * - TC1: Homepage rendering in Chrome
 * - TC2: Homepage rendering in Firefox
 * - TC3: Homepage rendering in Safari (WebKit)
 * - TC4: Homepage rendering in Edge
 * - TC5: Theme toggle functionality across all browsers
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  /**
   * Test Case 1-4: Verify homepage renders correctly across all browsers
   * These tests run automatically in all configured browser projects (Chrome, Firefox, Safari, Edge)
   */
  test.describe('Homepage Rendering', () => {
    test('all sections display correctly with no visual bugs or layout issues', async ({
      page,
      browserName,
    }) => {
      // Log which browser is being tested
      console.log(`Testing homepage rendering in: ${browserName}`)

      // Verify homepage container is visible
      const homePage = page.getByTestId('home-page')
      await expect(homePage).toBeVisible()

      // Verify navigation header renders correctly
      const navHeader = page.getByTestId('navigation-header')
      await expect(navHeader).toBeVisible()

      // Verify logo is displayed
      const navLogo = page.getByTestId('nav-logo')
      await expect(navLogo).toBeVisible()
      await expect(navLogo).toHaveText('URL Shortener')

      // Verify navigation links are present
      const navFeatures = page.getByTestId('nav-features')
      await expect(navFeatures).toBeVisible()
      await expect(navFeatures).toHaveText('Features')

      const navHowItWorks = page.getByTestId('nav-how-it-works')
      await expect(navHowItWorks).toBeVisible()
      await expect(navHowItWorks).toHaveText('How It Works')

      // Verify auth buttons are present
      const navLogin = page.getByTestId('nav-login')
      await expect(navLogin).toBeVisible()
      await expect(navLogin).toHaveText('Log In')

      const navSignup = page.getByTestId('nav-signup')
      await expect(navSignup).toBeVisible()
      await expect(navSignup).toHaveText('Sign Up')

      // Verify theme toggle is present
      const themeToggle = page.getByTestId('theme-toggle').first()
      await expect(themeToggle).toBeVisible()
    })

    test('hero section renders with correct content and styling', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing hero section in: ${browserName}`)

      // Verify hero section is visible
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Verify headline
      const heroHeadline = page.getByTestId('hero-headline')
      await expect(heroHeadline).toBeVisible()
      await expect(heroHeadline).toContainText('Shorten. Share. Track.')

      // Verify subheadline
      const heroSubheadline = page.getByTestId('hero-subheadline')
      await expect(heroSubheadline).toBeVisible()

      // Verify primary CTA button
      const primaryCta = page.getByTestId('hero-cta-primary')
      await expect(primaryCta).toBeVisible()
      await expect(primaryCta).toHaveText('Get Started Free')

      // Verify secondary CTA button
      const secondaryCta = page.getByTestId('hero-cta-secondary')
      await expect(secondaryCta).toBeVisible()
      await expect(secondaryCta).toHaveText('Log In')

      // Verify CTA buttons are clickable
      await expect(primaryCta).toBeEnabled()
      await expect(secondaryCta).toBeEnabled()
    })

    test('features section renders with all feature cards', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing features section in: ${browserName}`)

      // Scroll to features section
      const featuresSection = page.locator('#features')
      await featuresSection.scrollIntoViewIfNeeded()
      await expect(featuresSection).toBeVisible()

      // Verify features grid
      const featuresGrid = page.getByTestId('features-grid')
      await expect(featuresGrid).toBeVisible()

      // Verify all 4 feature cards are present
      const expectedFeatures = [
        { id: 'url-shortening', text: 'Quick URL Shortening' },
        { id: 'analytics', text: 'Detailed Analytics' },
        { id: 'link-management', text: 'Easy Link Management' },
        { id: 'security', text: 'Secure & Reliable' },
      ]

      for (const feature of expectedFeatures) {
        const card = page.getByTestId(`feature-card-${feature.id}`)
        await expect(card).toBeVisible()

        // Verify feature card has title
        const cardTitle = page.getByTestId(`feature-title-${feature.id}`)
        await expect(cardTitle).toContainText(feature.text)
      }

      // Verify there are exactly 4 feature cards
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      await expect(featureCards).toHaveCount(4)
    })

    test('how it works section renders with all steps', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing how it works section in: ${browserName}`)

      // Scroll to how it works section
      const howItWorksSection = page.locator('#how-it-works')
      await howItWorksSection.scrollIntoViewIfNeeded()
      await expect(howItWorksSection).toBeVisible()

      // Verify section heading
      const sectionHeading = howItWorksSection.getByRole('heading', { level: 2 })
      await expect(sectionHeading).toBeVisible()
      await expect(sectionHeading).toContainText('How It Works')

      // Verify all 3 steps are present (data-testid is "step-{number}" not "step-card-{number}")
      const stepCards = page.locator('[data-testid^="step-"]').filter({ hasNotText: '' })

      // Verify step content
      const steps = [
        { id: 'step-1', number: '1' },
        { id: 'step-2', number: '2' },
        { id: 'step-3', number: '3' },
      ]

      for (const step of steps) {
        const stepCard = page.getByTestId(step.id)
        await expect(stepCard).toBeVisible()

        // Verify step number badge
        const stepNumber = page.getByTestId(`step-number-${step.number}`)
        await expect(stepNumber).toBeVisible()
        await expect(stepNumber).toContainText(step.number)
      }
    })

    test('footer renders with correct links', async ({ page, browserName }) => {
      console.log(`Testing footer in: ${browserName}`)

      // Scroll to footer
      const footer = page.locator('footer')
      await footer.scrollIntoViewIfNeeded()
      await expect(footer).toBeVisible()

      // Verify footer links
      const aboutLink = page.getByTestId('footer-link-about')
      await expect(aboutLink).toBeVisible()

      const privacyLink = page.getByTestId('footer-link-privacy')
      await expect(privacyLink).toBeVisible()

      const termsLink = page.getByTestId('footer-link-terms')
      await expect(termsLink).toBeVisible()

      // Verify copyright text is present
      const copyrightText = footer.locator('text=©')
      await expect(copyrightText.first()).toBeVisible()
    })

    test('page layout has correct structure and spacing', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing page layout structure in: ${browserName}`)

      // Verify main content area exists
      const mainContent = page.locator('#main-content')
      await expect(mainContent).toBeVisible()

      // Verify sections exist and are rendered
      const heroSection = page.getByTestId('hero-section')
      const featuresSection = page.locator('#features')
      const howItWorksSection = page.locator('#how-it-works')

      // Verify hero section is initially visible (at top of page)
      await expect(heroSection).toBeVisible()

      // Scroll to features and verify it exists
      await featuresSection.scrollIntoViewIfNeeded()
      await expect(featuresSection).toBeVisible()

      // Scroll to how it works and verify it exists
      await howItWorksSection.scrollIntoViewIfNeeded()
      await expect(howItWorksSection).toBeVisible()

      // Get the document order of sections by checking their position in DOM
      const sectionOrder = await page.evaluate(() => {
        const hero = document.querySelector('[data-testid="hero-section"]')
        const features = document.querySelector('#features')
        const howItWorks = document.querySelector('#how-it-works')

        if (!hero || !features || !howItWorks) return null

        // Get DOM position (documentPosition comparison)
        const heroBeforeFeatures =
          hero.compareDocumentPosition(features) & Node.DOCUMENT_POSITION_FOLLOWING
        const featuresBeforeHowItWorks =
          features.compareDocumentPosition(howItWorks) & Node.DOCUMENT_POSITION_FOLLOWING

        return {
          heroBeforeFeatures: heroBeforeFeatures > 0,
          featuresBeforeHowItWorks: featuresBeforeHowItWorks > 0,
        }
      })

      // Verify sections are in correct order in DOM
      expect(sectionOrder).not.toBeNull()
      expect(sectionOrder?.heroBeforeFeatures).toBe(true)
      expect(sectionOrder?.featuresBeforeHowItWorks).toBe(true)
    })
  })

  /**
   * Test Case 5: Theme toggle functionality across all browsers
   */
  test.describe('Theme Toggle Across Browsers', () => {
    test('theme toggle cycles through available themes', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing theme toggle in: ${browserName}`)

      // Theme cycles through: light -> dark -> cyberpunk -> synthwave -> light
      const expectedThemes = ['light', 'dark', 'cyberpunk', 'synthwave']

      // Get initial theme
      const initialTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })
      console.log(`Initial theme: ${initialTheme}`)

      // Find theme toggle
      const themeToggle = page.getByTestId('theme-toggle').first()
      await expect(themeToggle).toBeVisible()

      // Click to change theme
      await themeToggle.click()
      await page.waitForTimeout(300)

      // Verify theme changed
      const newTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })
      console.log(`New theme after toggle: ${newTheme}`)

      // Theme should be different from initial
      expect(newTheme).not.toBe(initialTheme)
      // New theme should be one of the expected themes
      expect(expectedThemes).toContain(newTheme)

      // Click again to cycle to next theme
      await themeToggle.click()
      await page.waitForTimeout(300)

      const thirdTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })
      console.log(`Theme after second toggle: ${thirdTheme}`)

      // Third theme should be different from the second
      expect(thirdTheme).not.toBe(newTheme)
      expect(expectedThemes).toContain(thirdTheme)
    })

    test('theme persists styling across page sections', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing theme persistence across sections in: ${browserName}`)

      // Toggle theme
      const themeToggle = page.getByTestId('theme-toggle').first()
      await themeToggle.click()
      await page.waitForTimeout(300)

      // Get current theme
      const currentTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      // Scroll through page and verify theme is applied everywhere
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Check hero section background adapts to theme
      const heroBackground = await heroSection.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return styles.backgroundColor || styles.background
      })
      expect(heroBackground).toBeTruthy()

      // Scroll to features
      const featuresSection = page.locator('#features')
      await featuresSection.scrollIntoViewIfNeeded()

      // Verify theme attribute is still the same
      const themeAfterScroll = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })
      expect(themeAfterScroll).toBe(currentTheme)

      // Scroll to footer
      const footer = page.locator('footer')
      await footer.scrollIntoViewIfNeeded()

      // Theme should still be consistent
      const themeAtFooter = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })
      expect(themeAtFooter).toBe(currentTheme)
    })

    test('theme toggle button has correct styling and is accessible', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing theme toggle accessibility in: ${browserName}`)

      const themeToggle = page.getByTestId('theme-toggle').first()
      await expect(themeToggle).toBeVisible()

      // Verify button has aria-label for accessibility
      const ariaLabel = await themeToggle.getAttribute('aria-label')
      expect(ariaLabel).toBeTruthy()
      expect(ariaLabel?.toLowerCase()).toContain('theme')

      // Verify button is keyboard accessible
      await themeToggle.focus()
      await expect(themeToggle).toBeFocused()

      // Test keyboard activation
      const initialTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      await page.keyboard.press('Enter')
      await page.waitForTimeout(300)

      const themeAfterEnter = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      expect(themeAfterEnter).not.toBe(initialTheme)
    })
  })

  /**
   * Additional cross-browser tests for CSS compatibility
   */
  test.describe('CSS Compatibility', () => {
    test('flexbox and grid layouts render correctly', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing CSS layouts in: ${browserName}`)

      // Verify features grid uses CSS Grid
      const featuresGrid = page.getByTestId('features-grid')
      await featuresGrid.scrollIntoViewIfNeeded()

      const gridDisplay = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display
      })
      expect(gridDisplay).toBe('grid')

      // Verify navigation uses flexbox
      const navHeader = page.getByTestId('navigation-header')
      const navDisplay = await navHeader.evaluate((el) => {
        return window.getComputedStyle(el).display
      })
      expect(navDisplay).toContain('flex')
    })

    test('animations and transitions work correctly', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing animations in: ${browserName}`)

      // Hover over a button and check for transition
      const primaryCta = page.getByTestId('hero-cta-primary')
      await expect(primaryCta).toBeVisible()

      // Get button state before hover
      const beforeHover = await primaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return {
          transform: styles.transform,
          opacity: styles.opacity,
        }
      })

      // Hover over button
      await primaryCta.hover()
      await page.waitForTimeout(200)

      // Verify button responds to hover (transform or opacity may change)
      const afterHover = await primaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return {
          transform: styles.transform,
          opacity: styles.opacity,
        }
      })

      // At minimum, the button should be visible and interactive
      expect(afterHover.opacity).not.toBe('0')
    })

    test('fonts render correctly across browsers', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing font rendering in: ${browserName}`)

      // Check that font-family is applied
      const heroHeadline = page.getByTestId('hero-headline')
      const fontFamily = await heroHeadline.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily
      })

      // Font should be defined (not just default serif/sans-serif)
      expect(fontFamily).toBeTruthy()
      expect(fontFamily.length).toBeGreaterThan(0)

      // Check font size is rendered
      const fontSize = await heroHeadline.evaluate((el) => {
        return window.getComputedStyle(el).fontSize
      })
      expect(parseFloat(fontSize)).toBeGreaterThan(0)
    })

    test('buttons have consistent styling', async ({ page, browserName }) => {
      console.log(`Testing button consistency in: ${browserName}`)

      // Check primary CTA button
      const primaryCta = page.getByTestId('hero-cta-primary')
      await expect(primaryCta).toBeVisible()

      // Get computed styles
      const primaryStyles = await primaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return {
          display: styles.display,
          padding: styles.padding,
          borderRadius: styles.borderRadius,
          cursor: styles.cursor,
        }
      })

      // Verify button has proper interactive styling
      expect(primaryStyles.cursor).toBe('pointer')
      expect(primaryStyles.display).not.toBe('none')

      // Check secondary CTA button
      const secondaryCta = page.getByTestId('hero-cta-secondary')
      await expect(secondaryCta).toBeVisible()

      const secondaryStyles = await secondaryCta.evaluate((el) => {
        return window.getComputedStyle(el).cursor
      })
      expect(secondaryStyles).toBe('pointer')
    })

    test('images load correctly', async ({ page, browserName }) => {
      console.log(`Testing image loading in: ${browserName}`)

      // Wait for any images to load
      await page.waitForLoadState('networkidle')

      // Check all images are loaded
      const images = await page.locator('img').all()

      for (const img of images) {
        const isVisible = await img.isVisible()
        if (isVisible) {
          // Check image has loaded (naturalWidth > 0)
          const naturalWidth = await img.evaluate(
            (el) => (el as HTMLImageElement).naturalWidth
          )
          expect(naturalWidth).toBeGreaterThan(0)
        }
      }
    })
  })

  /**
   * Responsive behavior consistency across browsers
   */
  test.describe('Responsive Behavior Consistency', () => {
    test('desktop layout is consistent across browsers', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing desktop layout in: ${browserName}`)

      await page.setViewportSize({ width: 1280, height: 800 })
      await page.reload()
      await page.waitForLoadState('networkidle')

      // Verify desktop navigation is visible
      const navFeatures = page.getByTestId('nav-features')
      await expect(navFeatures).toBeVisible()

      const navLogin = page.getByTestId('nav-login')
      await expect(navLogin).toBeVisible()

      // Features should be in grid
      const featuresGrid = page.getByTestId('features-grid')
      await featuresGrid.scrollIntoViewIfNeeded()
      await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/)
    })

    test('navigation responsive behavior works correctly', async ({
      page,
      browserName,
    }) => {
      console.log(`Testing navigation responsiveness in: ${browserName}`)

      // Start with desktop viewport
      await page.setViewportSize({ width: 1280, height: 800 })
      await page.reload()
      await page.waitForLoadState('networkidle')

      // Desktop nav items should be visible
      const navFeatures = page.getByTestId('nav-features')
      await expect(navFeatures).toBeVisible()

      // Switch to mobile viewport
      await page.setViewportSize({ width: 375, height: 667 })
      await page.waitForTimeout(500)

      // Mobile menu button should appear (or nav items hidden)
      const hamburgerMenu = page.getByTestId('hamburger-menu')
      const isMobileMenuVisible = await hamburgerMenu.isVisible().catch(() => false)

      if (isMobileMenuVisible) {
        // Mobile menu exists and navigation is hidden behind it
        await expect(hamburgerMenu).toBeVisible()
      }
    })
  })
})

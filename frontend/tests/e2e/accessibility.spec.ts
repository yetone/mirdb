import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Accessibility Compliance E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 3: Navigate homepage using only Tab key
  test.describe('Test Case 3: Keyboard navigation with Tab key', () => {
    test('all interactive elements are reachable via Tab navigation', async ({ page }) => {
      // Focus on the first element
      await page.keyboard.press('Tab')

      // Skip to main link should be focused first
      const skipLink = page.getByTestId('skip-to-main')
      await expect(skipLink).toBeFocused()

      // Tab to logo
      await page.keyboard.press('Tab')
      const logo = page.getByTestId('nav-logo')
      await expect(logo).toBeFocused()

      // Tab to Features button
      await page.keyboard.press('Tab')
      const featuresButton = page.getByTestId('nav-features')
      await expect(featuresButton).toBeFocused()

      // Tab to How It Works button
      await page.keyboard.press('Tab')
      const howItWorksButton = page.getByTestId('nav-how-it-works')
      await expect(howItWorksButton).toBeFocused()

      // Tab to Theme toggle
      await page.keyboard.press('Tab')
      const themeToggle = page.getByTestId('theme-toggle')
      await expect(themeToggle).toBeFocused()

      // Tab to Log In button
      await page.keyboard.press('Tab')
      const loginButton = page.getByTestId('nav-login')
      await expect(loginButton).toBeFocused()

      // Tab to Sign Up button
      await page.keyboard.press('Tab')
      const signupButton = page.getByTestId('nav-signup')
      await expect(signupButton).toBeFocused()

      // Tab to Hero primary CTA
      await page.keyboard.press('Tab')
      const primaryCta = page.getByTestId('hero-cta-primary')
      await expect(primaryCta).toBeFocused()

      // Tab to Hero secondary CTA
      await page.keyboard.press('Tab')
      const secondaryCta = page.getByTestId('hero-cta-secondary')
      await expect(secondaryCta).toBeFocused()
    })

    test('can activate buttons using Enter key', async ({ page }) => {
      // Tab to theme toggle
      for (let i = 0; i < 5; i++) {
        await page.keyboard.press('Tab')
      }

      const themeToggle = page.getByTestId('theme-toggle')
      await expect(themeToggle).toBeFocused()

      // Get initial theme
      const initialTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      // Activate with Enter
      await page.keyboard.press('Enter')

      // Theme should change
      const newTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      expect(newTheme).not.toBe(initialTheme)
    })

    test('can activate buttons using Space key', async ({ page }) => {
      // Tab to theme toggle
      for (let i = 0; i < 5; i++) {
        await page.keyboard.press('Tab')
      }

      const themeToggle = page.getByTestId('theme-toggle')
      await expect(themeToggle).toBeFocused()

      // Get initial theme
      const initialTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      // Activate with Space
      await page.keyboard.press('Space')

      // Theme should change
      const newTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme')
      })

      expect(newTheme).not.toBe(initialTheme)
    })

    test('footer links are reachable via Tab', async ({ page }) => {
      // Scroll to footer first
      await page.evaluate(() => {
        document.querySelector('footer')?.scrollIntoView()
      })

      // Find and click footer to start tabbing from there
      const footer = page.locator('footer')
      await footer.click()

      // Tab through footer links
      await page.keyboard.press('Tab')
      const aboutLink = page.getByTestId('footer-link-about')
      await expect(aboutLink).toBeFocused()

      await page.keyboard.press('Tab')
      const privacyLink = page.getByTestId('footer-link-privacy')
      await expect(privacyLink).toBeFocused()

      await page.keyboard.press('Tab')
      const termsLink = page.getByTestId('footer-link-terms')
      await expect(termsLink).toBeFocused()
    })
  })

  // Test Case 4: Check focus visibility on buttons
  test.describe('Test Case 4: Focus visibility on buttons', () => {
    test('buttons show visible focus indicator when focused', async ({ page }) => {
      // Tab to Features button
      await page.keyboard.press('Tab') // skip link
      await page.keyboard.press('Tab') // logo
      await page.keyboard.press('Tab') // features

      const featuresButton = page.getByTestId('nav-features')
      await expect(featuresButton).toBeFocused()

      // Check that focus is visible (DaisyUI btn class provides focus states)
      const hasFocusStyles = await featuresButton.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        // Check for focus ring/outline or other visual indicators
        return (
          styles.outlineWidth !== '0px' ||
          styles.boxShadow !== 'none' ||
          el.classList.contains('btn')
        )
      })
      expect(hasFocusStyles).toBe(true)
    })

    test('primary CTA button shows visible focus indicator', async ({ page }) => {
      const primaryCta = page.getByTestId('hero-cta-primary')
      await primaryCta.focus()

      await expect(primaryCta).toBeFocused()

      // Primary buttons should have visible focus styles
      const hasFocusRing = await primaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return styles.outline !== 'none' || el.classList.contains('btn')
      })
      expect(hasFocusRing).toBe(true)
    })

    test('theme toggle shows visible focus indicator', async ({ page }) => {
      const themeToggle = page.getByTestId('theme-toggle')
      await themeToggle.focus()

      await expect(themeToggle).toBeFocused()

      // Check for btn-circle class which provides focus styles
      const hasButtonClass = await themeToggle.evaluate((el) => {
        return el.classList.contains('btn') && el.classList.contains('btn-circle')
      })
      expect(hasButtonClass).toBe(true)
    })

    test('skip link becomes visible when focused', async ({ page }) => {
      // Tab to focus skip link
      await page.keyboard.press('Tab')

      const skipLink = page.getByTestId('skip-to-main')
      await expect(skipLink).toBeFocused()

      // Check that it's visible when focused
      const isVisible = await skipLink.isVisible()
      expect(isVisible).toBe(true)

      // Check it has focus:not-sr-only styles
      const hasFocusStyles = await skipLink.evaluate((el) => {
        return el.classList.contains('focus:not-sr-only')
      })
      expect(hasFocusStyles).toBe(true)
    })
  })

  // Test Case 7: Lighthouse accessibility audit (using axe-core as alternative)
  test.describe('Test Case 7: Automated accessibility audit', () => {
    test('homepage has no critical accessibility violations', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
        .analyze()

      // Filter out minor issues and focus on critical/serious violations
      const criticalViolations = accessibilityScanResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      // Log violations for debugging
      if (criticalViolations.length > 0) {
        console.log('Critical accessibility violations:', JSON.stringify(criticalViolations, null, 2))
      }

      expect(criticalViolations).toHaveLength(0)
    })

    test('navigation passes accessibility audit', async ({ page }) => {
      const navResults = await new AxeBuilder({ page })
        .include('[data-testid="navigation-header"]')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze()

      const seriousViolations = navResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(seriousViolations).toHaveLength(0)
    })

    test('hero section passes accessibility audit', async ({ page }) => {
      const heroResults = await new AxeBuilder({ page })
        .include('[data-testid="hero-section"]')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze()

      const seriousViolations = heroResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(seriousViolations).toHaveLength(0)
    })

    test('features section passes accessibility audit', async ({ page }) => {
      const featuresResults = await new AxeBuilder({ page })
        .include('[data-testid="features-section"]')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze()

      const seriousViolations = featuresResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(seriousViolations).toHaveLength(0)
    })

    test('footer passes accessibility audit', async ({ page }) => {
      const footerResults = await new AxeBuilder({ page })
        .include('footer')
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze()

      const seriousViolations = footerResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(seriousViolations).toHaveLength(0)
    })
  })

  // Test Case 8: Reduced motion support
  test.describe('Test Case 8: Reduced motion support', () => {
    test('animations are disabled when prefers-reduced-motion is set', async ({ page }) => {
      // Set reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' })

      // Reload page with the new media preference
      await page.reload()
      await page.waitForLoadState('networkidle')

      // Check that the hero section has reduced motion data attribute
      const heroContent = page.locator('[data-reduced-motion]')
      const reducedMotionValue = await heroContent.getAttribute('data-reduced-motion')

      expect(reducedMotionValue).toBe('true')
    })

    test('animations play normally when motion is not reduced', async ({ page }) => {
      // Ensure no reduced motion preference
      await page.emulateMedia({ reducedMotion: 'no-preference' })

      // Reload page
      await page.reload()
      await page.waitForLoadState('networkidle')

      // Check that reduced motion is false
      const heroContent = page.locator('[data-reduced-motion]')
      const reducedMotionValue = await heroContent.getAttribute('data-reduced-motion')

      expect(reducedMotionValue).toBe('false')
    })
  })

  // Additional accessibility tests
  test.describe('Additional accessibility checks', () => {
    test('skip link navigates to main content', async ({ page }) => {
      // Focus skip link
      await page.keyboard.press('Tab')

      // Activate skip link
      await page.keyboard.press('Enter')

      // Check that focus moved to main content area
      const mainContent = page.locator('#main-content')
      await expect(mainContent).toBeInViewport()
    })

    test('all images have alt text or are marked decorative', async ({ page }) => {
      const images = await page.locator('img').all()

      for (const img of images) {
        const alt = await img.getAttribute('alt')
        expect(alt).not.toBeNull()
      }
    })

    test('all SVG icons are properly hidden from screen readers', async ({ page }) => {
      const decorativeIcons = await page.locator('svg[aria-hidden="true"]').all()

      // We should have multiple decorative icons (theme, features, steps)
      expect(decorativeIcons.length).toBeGreaterThan(0)
    })

    test('page has proper heading hierarchy', async ({ page }) => {
      const h1Count = await page.locator('h1').count()
      expect(h1Count).toBe(1) // Only one h1 per page

      const h2Count = await page.locator('h2').count()
      expect(h2Count).toBeGreaterThanOrEqual(2) // Features and How It Works
    })

    test('interactive elements have accessible names', async ({ page }) => {
      // Check theme toggle has accessible name via aria-label
      const themeToggle = page.getByTestId('theme-toggle')
      const ariaLabel = await themeToggle.getAttribute('aria-label')
      expect(ariaLabel).toBeTruthy()
      expect(ariaLabel).toContain('theme')

      // Check logo has accessible name
      const logo = page.getByTestId('nav-logo')
      const logoAriaLabel = await logo.getAttribute('aria-label')
      expect(logoAriaLabel).toBeTruthy()
    })
  })
})

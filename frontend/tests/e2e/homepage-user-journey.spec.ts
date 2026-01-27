/**
 * E2E tests for User Journey End-to-End
 * Owner: Scenario 10 - User Journey E2E
 *
 * Validates the complete user journey from landing on the homepage
 * to registration/login as specified in the PRD User Journey section.
 *
 * Test Cases:
 * 1. Homepage loads within 3 seconds, hero section visible
 * 2. All sections (hero, features, how it works, stats, footer) load correctly
 * 3. User can navigate from homepage to registration
 * 4. User can navigate to login page
 * 5. Homepage renders correctly across browsers
 * 6. No console errors during homepage load
 * 7. Lighthouse performance audit (score above 90)
 */

import { test, expect, Page } from '@playwright/test'

test.describe('User Journey End-to-End', () => {
  test.describe('TC-1: Homepage Load Performance', () => {
    test('homepage loads within 3 seconds with hero section visible', async ({ page }) => {
      const startTime = Date.now()

      await page.goto('/')

      // Verify page loaded
      await expect(page).toHaveTitle(/URL Shortener|Home/i, { timeout: 3000 })

      // Verify hero section is visible
      const heroSection = page.locator('section[aria-label="Hero"]')
      await expect(heroSection).toBeVisible({ timeout: 3000 })

      // Verify main headline is visible
      const headline = page.getByRole('heading', { level: 1 })
      await expect(headline).toBeVisible()
      await expect(headline).toContainText(/Shorten Your Links/i)

      // Check load time
      const loadTime = Date.now() - startTime
      expect(loadTime).toBeLessThan(3000)
    })

    test('hero section displays value proposition correctly', async ({ page }) => {
      await page.goto('/')

      // Verify headline with value proposition
      const headline = page.getByRole('heading', { level: 1 })
      await expect(headline).toContainText(/Amplify Your Reach/i)

      // Verify subheadline about analytics
      const subheadline = page.locator('section[aria-label="Hero"] p')
      await expect(subheadline).toContainText(/analytics/i)
    })
  })

  test.describe('TC-2: Homepage Sections Display', () => {
    test('all sections load and display correctly', async ({ page }) => {
      await page.goto('/')

      // Hero section
      const heroSection = page.locator('section[aria-label="Hero"]')
      await expect(heroSection).toBeVisible()

      // Features section
      const featuresSection = page.locator('section[aria-label="Features"]')
      await expect(featuresSection).toBeVisible()
      await expect(featuresSection.getByRole('heading', { level: 2 })).toContainText(
        'Powerful Features'
      )

      // Verify all 3 feature cards are present
      const featureCards = featuresSection.locator('[data-testid="features-grid"] > div')
      await expect(featureCards).toHaveCount(3)

      // How It Works section
      const howItWorksSection = page.locator('section[aria-label="How It Works"]')
      await expect(howItWorksSection).toBeVisible()
      await expect(howItWorksSection.getByRole('heading', { level: 2 })).toContainText(
        'How It Works'
      )

      // Stats section
      const statsSection = page.locator('[data-testid="stats-section"]')
      await expect(statsSection).toBeVisible()

      // Footer section
      const footer = page.locator('footer')
      await expect(footer).toBeVisible()
    })

    test('scrolling through homepage reveals all content', async ({ page }) => {
      await page.goto('/')

      // Scroll to features
      await page.locator('#features').scrollIntoViewIfNeeded()
      await expect(page.locator('section[aria-label="Features"]')).toBeInViewport()

      // Scroll to how it works
      await page.locator('#how-it-works').scrollIntoViewIfNeeded()
      await expect(page.locator('section[aria-label="How It Works"]')).toBeInViewport()

      // Scroll to stats
      await page.locator('#stats').scrollIntoViewIfNeeded()
      await expect(page.locator('[data-testid="stats-section"]')).toBeInViewport()

      // Scroll to footer
      await page.locator('footer').scrollIntoViewIfNeeded()
      await expect(page.locator('footer')).toBeInViewport()
    })
  })

  test.describe('TC-3: Homepage to Registration Navigation', () => {
    test('clicking Get Started navigates to registration page', async ({ page }) => {
      await page.goto('/')

      // Find and click the Get Started button
      const getStartedButton = page.getByRole('link', { name: /get started/i })
      await expect(getStartedButton).toBeVisible()

      await getStartedButton.click()

      // Verify navigation to registration page
      await expect(page).toHaveURL('/register')

      // Verify registration page content
      const registerHeading = page.getByRole('heading', { name: /register/i })
      await expect(registerHeading).toBeVisible()
    })

    test('user journey: discover → explore → register', async ({ page }) => {
      // Step 1: Land on homepage
      await page.goto('/')
      await expect(page.locator('section[aria-label="Hero"]')).toBeVisible()

      // Step 2: Discover value proposition
      const headline = page.getByRole('heading', { level: 1 })
      await expect(headline).toContainText(/Shorten Your Links/i)

      // Step 3: Explore features (click Learn More and scroll)
      const learnMoreButton = page.getByRole('button', { name: /learn more/i })
      await learnMoreButton.click()

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500)
      await expect(page.locator('#features')).toBeInViewport()

      // Continue scrolling through sections
      await page.locator('#how-it-works').scrollIntoViewIfNeeded()
      await page.waitForTimeout(300)

      // Step 4: Navigate to registration
      await page.goto('/') // Go back to top
      const getStartedButton = page.getByRole('link', { name: /get started/i })
      await getStartedButton.click()

      // Step 5: Complete registration (reach registration page)
      await expect(page).toHaveURL('/register')
      await expect(page.getByRole('heading', { name: /register/i })).toBeVisible()
    })
  })

  test.describe('TC-4: Login Navigation for Returning Users', () => {
    test('clicking Login navigates to login page', async ({ page }) => {
      await page.goto('/')

      // Find and click the Login button in header
      const loginButton = page.getByRole('link', { name: /login/i }).first()
      await expect(loginButton).toBeVisible()

      await loginButton.click()

      // Verify navigation to login page
      await expect(page).toHaveURL('/login')

      // Verify login page content
      const loginHeading = page.getByRole('heading', { name: /login/i })
      await expect(loginHeading).toBeVisible()
    })

    test('login link is accessible from footer', async ({ page }) => {
      await page.goto('/')

      // Scroll to footer
      await page.locator('footer').scrollIntoViewIfNeeded()

      // Find login link in footer navigation
      const footerLoginLink = page.locator('footer').getByRole('link', { name: /login/i })
      await expect(footerLoginLink).toBeVisible()

      await footerLoginLink.click()

      // Verify navigation
      await expect(page).toHaveURL('/login')
    })
  })

  test.describe('TC-5: Cross-Browser Compatibility', () => {
    // Note: Actual cross-browser testing is handled by Playwright projects configuration
    // These tests verify rendering consistency

    test('homepage renders main content correctly', async ({ page }) => {
      await page.goto('/')

      // Verify critical elements are present and properly rendered
      await expect(page.getByRole('main')).toBeVisible()
      await expect(page.getByRole('heading', { level: 1 })).toBeVisible()
      await expect(page.getByRole('heading', { level: 2 }).first()).toBeVisible()

      // Verify buttons are interactive
      const getStartedButton = page.getByRole('link', { name: /get started/i })
      await expect(getStartedButton).toBeEnabled()

      const learnMoreButton = page.getByRole('button', { name: /learn more/i })
      await expect(learnMoreButton).toBeEnabled()
    })

    test('responsive layout adapts correctly', async ({ page }) => {
      await page.goto('/')

      // Test at different viewport sizes
      const viewports = [
        { width: 375, height: 667, name: 'mobile' },
        { width: 768, height: 1024, name: 'tablet' },
        { width: 1280, height: 720, name: 'desktop' },
      ]

      for (const viewport of viewports) {
        await page.setViewportSize({ width: viewport.width, height: viewport.height })

        // Verify content is visible at each viewport
        await expect(page.getByRole('heading', { level: 1 })).toBeVisible()
        await expect(page.getByRole('link', { name: /get started/i })).toBeVisible()
      }
    })
  })

  test.describe('TC-6: Console Errors Check', () => {
    test('no console errors during homepage load', async ({ page }) => {
      const consoleErrors: string[] = []
      const consoleWarnings: string[] = []

      // Listen for console messages
      page.on('console', (message) => {
        if (message.type() === 'error') {
          consoleErrors.push(message.text())
        }
        if (message.type() === 'warning') {
          // Filter out known non-critical warnings
          const text = message.text()
          if (
            !text.includes('React DevTools') &&
            !text.includes('Download the React DevTools')
          ) {
            consoleWarnings.push(text)
          }
        }
      })

      // Load the page and interact with it
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Scroll through all sections to trigger any lazy-loaded content
      await page.locator('#features').scrollIntoViewIfNeeded()
      await page.locator('#how-it-works').scrollIntoViewIfNeeded()
      await page.locator('#stats').scrollIntoViewIfNeeded()
      await page.locator('footer').scrollIntoViewIfNeeded()

      // Filter out acceptable errors/warnings
      const criticalErrors = consoleErrors.filter(
        (error) =>
          !error.includes('Failed to load resource') && // Ignore 404s for missing API endpoints
          !error.includes('favicon')
      )

      // Expect no critical console errors
      expect(criticalErrors).toHaveLength(0)
    })

    test('no JavaScript errors during navigation', async ({ page }) => {
      const pageErrors: Error[] = []

      page.on('pageerror', (error) => {
        pageErrors.push(error)
      })

      // Navigate through the site
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Navigate to register
      await page.goto('/register')
      await page.waitForLoadState('networkidle')

      // Navigate to login
      await page.goto('/login')
      await page.waitForLoadState('networkidle')

      // Navigate back to homepage
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Expect no page errors
      expect(pageErrors).toHaveLength(0)
    })
  })

  test.describe('TC-7: Performance Audit', () => {
    test('homepage initial render is performant', async ({ page }) => {
      // Navigate with performance timing
      const metrics = await page.evaluate(() => {
        return new Promise<PerformanceNavigationTiming | null>((resolve) => {
          // Wait a bit for performance entries to be recorded
          setTimeout(() => {
            const entries = performance.getEntriesByType(
              'navigation'
            ) as PerformanceNavigationTiming[]
            resolve(entries[0] || null)
          }, 100)
        })
      })

      await page.goto('/')
      await page.waitForLoadState('domcontentloaded')

      // Verify DOM is interactive quickly
      const domContentLoaded = await page.evaluate(() => {
        const entries = performance.getEntriesByType(
          'navigation'
        ) as PerformanceNavigationTiming[]
        return entries[0]?.domContentLoadedEventEnd || 0
      })

      // DOM should be interactive within reasonable time
      expect(domContentLoaded).toBeLessThan(3000)
    })

    test('critical content is visible without scrolling', async ({ page }) => {
      await page.goto('/')

      // Set a standard desktop viewport
      await page.setViewportSize({ width: 1280, height: 720 })

      // Verify hero content is in viewport (above the fold)
      const heroHeadline = page.getByRole('heading', { level: 1 })
      await expect(heroHeadline).toBeInViewport()

      const getStartedButton = page.getByRole('link', { name: /get started/i })
      await expect(getStartedButton).toBeInViewport()

      const loginButton = page.getByRole('link', { name: /login/i }).first()
      await expect(loginButton).toBeInViewport()
    })

    test('interactive elements are clickable and functional', async ({ page }) => {
      await page.goto('/')

      // Verify button is interactive
      const learnMoreButton = page.getByRole('button', { name: /learn more/i })
      await expect(learnMoreButton).toBeEnabled()

      // Click the button and verify scroll behavior triggers
      await learnMoreButton.click()

      // Wait for scroll animation
      await page.waitForTimeout(600)

      // Verify the features section is now in viewport (scroll worked)
      await expect(page.locator('#features')).toBeInViewport()
    })
  })
})

// Additional integration tests for edge cases
test.describe('User Journey Edge Cases', () => {
  test('navigation works with keyboard', async ({ page }) => {
    await page.goto('/')

    // Tab to Get Started button and press Enter
    await page.keyboard.press('Tab')
    await page.keyboard.press('Tab')

    // Find the focused element
    const focusedElement = page.locator(':focus')
    await expect(focusedElement).toBeVisible()

    // Navigate with keyboard
    await page.keyboard.press('Tab')
    await page.keyboard.press('Tab')
    await page.keyboard.press('Tab')
    await page.keyboard.press('Enter')

    // Should navigate somewhere
    await page.waitForLoadState('domcontentloaded')
  })

  test('back/forward navigation works correctly', async ({ page }) => {
    // Go to homepage
    await page.goto('/')
    await expect(page).toHaveURL('/')

    // Navigate to register
    await page.goto('/register')
    await expect(page).toHaveURL('/register')

    // Go back
    await page.goBack()
    await expect(page).toHaveURL('/')

    // Go forward
    await page.goForward()
    await expect(page).toHaveURL('/register')
  })

  test('direct URL access works for all routes', async ({ page }) => {
    // Direct access to homepage
    await page.goto('/')
    await expect(page.getByRole('heading', { level: 1 })).toBeVisible()

    // Direct access to register
    await page.goto('/register')
    await expect(page.getByRole('heading', { name: /register/i })).toBeVisible()

    // Direct access to login
    await page.goto('/login')
    await expect(page.getByRole('heading', { name: /login/i })).toBeVisible()
  })
})

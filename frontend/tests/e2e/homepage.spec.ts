/**
 * Homepage E2E Tests
 *
 * End-to-end tests for the homepage functionality.
 * Tests navigation, CTA buttons, and user flows.
 */

import { test, expect } from '@playwright/test'

test.describe('Homepage Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 1: Navigate to '/' route - Hero renders with gradient
  test('hero section renders with gradient background', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const gradient = page.getByTestId('hero-gradient')
    await expect(gradient).toBeVisible()
  })

  // Test Case 2: Headline is visible
  test('displays headline "Shorten Links. Track Success."', async ({ page }) => {
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()
    await expect(headline).toHaveText('Shorten Links. Track Success.')
  })

  // Test Case 3: Subheadline is present
  test('displays subheadline explaining value proposition', async ({ page }) => {
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()
    await expect(subheadline).toContainText('Transform long URLs')
  })

  // Test Case 4: Click CTA navigates to /register
  test('clicking "Get Started Free" navigates to /register', async ({ page }) => {
    const ctaButton = page.getByTestId('hero-cta')
    await expect(ctaButton).toBeVisible()
    await expect(ctaButton).toHaveText(/Get Started Free/)

    await ctaButton.click()
    await expect(page).toHaveURL('/register')
  })

  // Test Case 5: All hero elements visible on 1920x1080 viewport
  test('all hero elements visible without scrolling on 1920x1080', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')

    const heroSection = page.getByTestId('hero-section')
    const headline = page.getByTestId('hero-headline')
    const subheadline = page.getByTestId('hero-subheadline')
    const ctaButton = page.getByTestId('hero-cta')

    // All elements should be visible in viewport
    await expect(heroSection).toBeInViewport()
    await expect(headline).toBeInViewport()
    await expect(subheadline).toBeInViewport()
    await expect(ctaButton).toBeInViewport()
  })
})

test.describe('Theme Compatibility', () => {
  test('hero section works with dark theme', async ({ page }) => {
    await page.goto('/')

    // Page should have theme attribute
    const html = page.locator('html')
    await expect(html).toHaveAttribute('data-theme')
  })
})

/**
 * Theme Support E2E Tests
 * Owner: Scenario 7 - Theme Support and Consistency
 *
 * Tests theme navigation inheritance and theme toggling.
 */
test.describe('Theme Support - Scenario 7', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/')
    await page.evaluate(() => localStorage.clear())
  })

  // Test Case 5: Navigate from dashboard (with theme) to homepage
  test('homepage inherits theme when navigating from dashboard', async ({ page }) => {
    // Set synthwave theme via localStorage (simulating dashboard selection)
    await page.evaluate(() => {
      localStorage.setItem('theme', 'synthwave')
    })

    // Navigate to homepage
    await page.goto('/')

    // Wait for theme to be applied
    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'synthwave'
    })

    // Verify the theme is applied
    const html = page.locator('html')
    await expect(html).toHaveAttribute('data-theme', 'synthwave')

    // Verify homepage sections are visible with theme
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const howItWorksSection = page.getByTestId('how-it-works-section')
    await expect(howItWorksSection).toBeVisible()
  })

  test('theme persists across page navigation', async ({ page }) => {
    // Set cyberpunk theme
    await page.evaluate(() => {
      localStorage.setItem('theme', 'cyberpunk')
    })

    // Navigate to homepage
    await page.goto('/')

    // Wait for theme application
    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'cyberpunk'
    })

    // Navigate to register and back
    const ctaButton = page.getByTestId('hero-cta')
    await ctaButton.click()
    await expect(page).toHaveURL('/register')

    // Go back to homepage
    await page.goto('/')

    // Theme should still be cyberpunk
    const html = page.locator('html')
    await expect(html).toHaveAttribute('data-theme', 'cyberpunk')
  })

  // Test Case 6: Toggle theme using ThemeToggle component on homepage
  test('theme changes are immediately reflected across all sections', async ({ page }) => {
    await page.goto('/')

    // Wait for page to load
    const themeToggle = page.getByTestId('theme-toggle')
    await expect(themeToggle).toBeVisible()

    // Get initial theme
    const html = page.locator('html')
    const initialTheme = await html.getAttribute('data-theme')

    // Open theme menu and click to change theme
    await themeToggle.click()

    // Wait for menu to be visible
    const themeMenu = page.getByTestId('theme-menu')
    await expect(themeMenu).toBeVisible()

    // Select a different theme (synthwave if not already)
    const targetTheme = initialTheme === 'synthwave' ? 'cyberpunk' : 'synthwave'
    const themeOption = page.getByTestId(`theme-option-${targetTheme}`)
    await themeOption.click()

    // Verify theme changed
    await expect(html).toHaveAttribute('data-theme', targetTheme)

    // Verify all sections are still visible and rendered correctly
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const howItWorksSection = page.getByTestId('how-it-works-section')
    await expect(howItWorksSection).toBeVisible()

    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible()

    const socialProofSection = page.getByTestId('social-proof-section')
    await expect(socialProofSection).toBeVisible()

    const footer = page.getByTestId('footer')
    await expect(footer).toBeVisible()
  })

  test('all four themes render correctly on homepage', async ({ page }) => {
    const themes = ['light', 'dark', 'cyberpunk', 'synthwave']

    for (const theme of themes) {
      // Set theme via localStorage
      await page.evaluate((t) => {
        localStorage.setItem('theme', t)
      }, theme)

      // Reload page to apply theme
      await page.goto('/')

      // Wait for theme application
      await page.waitForFunction((t) => {
        return document.documentElement.getAttribute('data-theme') === t
      }, theme)

      // Verify theme is applied
      const html = page.locator('html')
      await expect(html).toHaveAttribute('data-theme', theme)

      // Verify hero section renders
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Verify hero gradient (theme-aware)
      const heroGradient = page.getByTestId('hero-gradient')
      await expect(heroGradient).toBeVisible()

      // Verify CTA button
      const ctaButton = page.getByTestId('hero-cta')
      await expect(ctaButton).toBeVisible()
    }
  })

  test('theme toggle dropdown shows all theme options', async ({ page }) => {
    await page.goto('/')

    // Open theme toggle
    const themeToggle = page.getByTestId('theme-toggle')
    await themeToggle.click()

    // Verify all theme options are present
    await expect(page.getByTestId('theme-option-light')).toBeVisible()
    await expect(page.getByTestId('theme-option-dark')).toBeVisible()
    await expect(page.getByTestId('theme-option-cyberpunk')).toBeVisible()
    await expect(page.getByTestId('theme-option-synthwave')).toBeVisible()
  })

  test('active theme is indicated in theme menu', async ({ page }) => {
    // Set dark theme
    await page.evaluate(() => {
      localStorage.setItem('theme', 'dark')
    })

    await page.goto('/')

    // Wait for theme
    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'dark'
    })

    // Open theme toggle
    const themeToggle = page.getByTestId('theme-toggle')
    await themeToggle.click()

    // Verify dark theme option shows as active
    const darkOption = page.getByTestId('theme-option-dark')
    await expect(darkOption).toContainText('Active')
  })
})

// Test Case 6: How It Works Section - Steps arranged left-to-right on desktop
test.describe('How It Works Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('How It Works section is visible on page', async ({ page }) => {
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await expect(howItWorksSection).toBeVisible()
  })

  test('displays How It Works heading', async ({ page }) => {
    const heading = page.getByTestId('how-it-works-heading')
    await expect(heading).toBeVisible()
    await expect(heading).toContainText('How It Works')
  })

  test('displays exactly 3 step cards', async ({ page }) => {
    const stepCards = page.getByTestId('step-card')
    await expect(stepCards).toHaveCount(3)
  })

  test('steps are arranged left-to-right on desktop viewport', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 })
    await page.goto('/')

    const stepCards = page.getByTestId('step-card')
    await expect(stepCards).toHaveCount(3)

    // Get bounding boxes for all step cards
    const step1Box = await stepCards.nth(0).boundingBox()
    const step2Box = await stepCards.nth(1).boundingBox()
    const step3Box = await stepCards.nth(2).boundingBox()

    // Verify left-to-right arrangement (each step's x position is greater than previous)
    expect(step1Box).not.toBeNull()
    expect(step2Box).not.toBeNull()
    expect(step3Box).not.toBeNull()

    if (step1Box && step2Box && step3Box) {
      expect(step2Box.x).toBeGreaterThan(step1Box.x)
      expect(step3Box.x).toBeGreaterThan(step2Box.x)
    }
  })

  test('all steps are visible on desktop without scrolling', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')

    // Scroll to How It Works section first
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()

    const stepCards = page.getByTestId('step-card')
    const count = await stepCards.count()

    for (let i = 0; i < count; i++) {
      await expect(stepCards.nth(i)).toBeInViewport()
    }
  })

  test('each step displays number indicator', async ({ page }) => {
    const stepNumbers = page.getByTestId('step-number')
    await expect(stepNumbers).toHaveCount(3)

    await expect(stepNumbers.nth(0)).toContainText('1')
    await expect(stepNumbers.nth(1)).toContainText('2')
    await expect(stepNumbers.nth(2)).toContainText('3')
  })

  test('steps follow logical progression', async ({ page }) => {
    // Verify the 3 steps describe the process
    const stepTitles = page.getByTestId('step-title')
    await expect(stepTitles).toHaveCount(3)

    // Step 1 should relate to pasting URL
    await expect(stepTitles.nth(0)).toContainText(/paste|url/i)
    // Step 2 should relate to getting short link
    await expect(stepTitles.nth(1)).toContainText(/short|link/i)
    // Step 3 should relate to sharing and tracking
    await expect(stepTitles.nth(2)).toContainText(/share|track/i)
  })
})

/**
 * Navigation Integration E2E Tests
 * Owner: Scenario 11 - Navigation Integration
 *
 * Tests navigation between homepage and other pages including authentication flow.
 * Covers: registration CTA, login link, dashboard link, footer nav, and browser history.
 */
test.describe('Navigation Integration - Scenario 11', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage and cookies before each test
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.clear()
      sessionStorage.clear()
    })
  })

  // Test Case 1: Click 'Get Started Free' CTA button → navigates to /register
  test('clicking "Get Started Free" CTA navigates to /register', async ({ page }) => {
    await page.goto('/')

    // Find and click the hero CTA button
    const ctaButton = page.getByTestId('hero-cta')
    await expect(ctaButton).toBeVisible()
    await expect(ctaButton).toContainText('Get Started Free')

    await ctaButton.click()

    // Verify navigation to /register
    await expect(page).toHaveURL('/register')
  })

  // Test Case 2: Click login link/button from homepage → navigates to /login
  test('clicking login link navigates to /login', async ({ page }) => {
    await page.goto('/')

    // Find the login link in the navigation header
    const loginLink = page.getByTestId('nav-login-link')
    await expect(loginLink).toBeVisible()
    await expect(loginLink).toContainText('Login')

    await loginLink.click()

    // Verify navigation to /login
    await expect(page).toHaveURL('/login')
  })

  // Test Case 3: Visit homepage with valid JWT in localStorage → CTA changes to 'Go to Dashboard'
  test('authenticated user sees dashboard link instead of login/signup', async ({ page }) => {
    // Set up authentication by adding a token to localStorage
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('token', 'mock-jwt-token-for-testing')
    })

    // Reload to apply auth state
    await page.reload()

    // Verify dashboard link is visible (authenticated state)
    const dashboardLink = page.getByTestId('nav-dashboard-link')
    await expect(dashboardLink).toBeVisible()
    await expect(dashboardLink).toContainText('Go to Dashboard')

    // Verify login and signup links are NOT visible
    const loginLink = page.getByTestId('nav-login-link')
    await expect(loginLink).not.toBeVisible()

    const signupLink = page.getByTestId('nav-register-link')
    await expect(signupLink).not.toBeVisible()
  })

  // Test Case 4: Click dashboard link as authenticated user → navigates to /dashboard
  test('authenticated user can navigate to dashboard', async ({ page }) => {
    // Set up authentication
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('token', 'mock-jwt-token-for-testing')
    })
    await page.reload()

    // Click dashboard link
    const dashboardLink = page.getByTestId('nav-dashboard-link')
    await expect(dashboardLink).toBeVisible()
    await dashboardLink.click()

    // Verify navigation to /dashboard
    await expect(page).toHaveURL('/dashboard')
  })

  // Test Case 5: Click footer navigation links → each link navigates to correct route
  test('footer navigation links work correctly', async ({ page }) => {
    await page.goto('/')

    // Test Home link
    const homeLink = page.getByTestId('footer-link-home')
    await expect(homeLink).toBeVisible()
    await expect(homeLink).toHaveAttribute('href', '/')

    // Test Features link (anchor link to features section)
    const featuresLink = page.getByTestId('footer-link-features')
    await expect(featuresLink).toBeVisible()
    await expect(featuresLink).toHaveAttribute('href', '/#features')

    // Test Pricing link
    const pricingLink = page.getByTestId('footer-link-pricing')
    await expect(pricingLink).toBeVisible()
    await expect(pricingLink).toHaveAttribute('href', '/pricing')

    // Test About link
    const aboutLink = page.getByTestId('footer-link-about')
    await expect(aboutLink).toBeVisible()
    await expect(aboutLink).toHaveAttribute('href', '/about')

    // Test Contact link
    const contactLink = page.getByTestId('footer-link-contact')
    await expect(contactLink).toBeVisible()
    await expect(contactLink).toHaveAttribute('href', '/contact')

    // Test Privacy Policy link
    const privacyLink = page.getByTestId('footer-link-privacy-policy')
    await expect(privacyLink).toBeVisible()
    await expect(privacyLink).toHaveAttribute('href', '/privacy')

    // Test Terms of Service link
    const termsLink = page.getByTestId('footer-link-terms-of-service')
    await expect(termsLink).toBeVisible()
    await expect(termsLink).toHaveAttribute('href', '/terms')
  })

  // Test Case 6: Navigate from homepage to register and back → browser history works
  test('browser history navigation works correctly', async ({ page }) => {
    // Start on homepage
    await page.goto('/')
    await expect(page).toHaveURL('/')

    // Navigate to register using the hero CTA
    const ctaButton = page.getByTestId('hero-cta')
    await ctaButton.click()
    await expect(page).toHaveURL('/register')

    // Use browser back button
    await page.goBack()
    await expect(page).toHaveURL('/')

    // Verify homepage content is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Navigate forward
    await page.goForward()
    await expect(page).toHaveURL('/register')
  })

  // Additional test: Navigation header is present and properly structured
  test('navigation header is visible and contains expected elements', async ({ page }) => {
    await page.goto('/')

    // Verify navigation header exists
    const navHeader = page.getByTestId('navigation-header')
    await expect(navHeader).toBeVisible()

    // Verify logo/brand link
    const logoLink = page.getByTestId('nav-logo')
    await expect(logoLink).toBeVisible()
    await expect(logoLink).toContainText('LinkShort')

    // Verify unauthenticated navigation (login + signup)
    const loginLink = page.getByTestId('nav-login-link')
    const signupLink = page.getByTestId('nav-register-link')
    await expect(loginLink).toBeVisible()
    await expect(signupLink).toBeVisible()

    // Verify theme toggle is present
    const themeToggle = page.getByTestId('theme-toggle')
    await expect(themeToggle).toBeVisible()
  })

  // Test: Login link in nav navigates correctly
  test('clicking nav register link navigates to /register', async ({ page }) => {
    await page.goto('/')

    const signupLink = page.getByTestId('nav-register-link')
    await expect(signupLink).toBeVisible()
    await signupLink.click()

    await expect(page).toHaveURL('/register')
  })

  // Test: Navigation maintains theme across route changes
  test('theme persists when navigating between pages', async ({ page }) => {
    // Set theme to cyberpunk
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('theme', 'cyberpunk')
    })
    await page.reload()

    // Wait for theme to apply
    await page.waitForFunction(() => {
      return document.documentElement.getAttribute('data-theme') === 'cyberpunk'
    })

    // Navigate to register
    const ctaButton = page.getByTestId('hero-cta')
    await ctaButton.click()
    await expect(page).toHaveURL('/register')

    // Theme should still be cyberpunk
    const html = page.locator('html')
    await expect(html).toHaveAttribute('data-theme', 'cyberpunk')
  })
})

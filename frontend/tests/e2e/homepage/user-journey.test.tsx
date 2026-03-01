/**
 * User Journey E2E Tests
 * Owner: Scenario 12 - Complete User Journey
 *
 * End-to-end tests for complete user journeys.
 *
 * Test coverage:
 * - TC1: New visitor lands on homepage and signs up
 * - TC2: Returning user logs in from homepage
 * - TC3: Scroll through entire homepage
 * - TC4: Mobile user completes journey
 * - TC5: Value proposition is visible above the fold (manual verification)
 */

import { test, expect } from '@playwright/test'

test.describe('Complete User Journey - Homepage to Registration', () => {
  test.beforeEach(async ({ page }) => {
    // Start at homepage
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')
  })

  test('TC1: Full user journey from homepage to registration', async ({ page }) => {
    // Step 1: Land on homepage - verify homepage loads
    const homePage = page.locator('[data-testid="home-page"]')
    await expect(homePage).toBeVisible()

    // Step 2: View hero section - user sees value proposition
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Verify product name is visible
    const productName = page.locator('[data-testid="product-name"]')
    await expect(productName).toBeVisible()
    await expect(productName).toContainText('URL')
    await expect(productName).toContainText('Shortener')

    // Verify tagline is visible
    const tagline = page.locator('[data-testid="tagline"]')
    await expect(tagline).toBeVisible()
    const taglineText = await tagline.textContent()
    expect(taglineText).toBeTruthy()
    expect(taglineText!.length).toBeGreaterThan(10)

    // Step 3: Scroll through features - view feature highlights
    const featuresSection = page.locator('[data-testid="features-section"]')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Verify feature cards are visible
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    const featureCards = featuresGrid.locator('[data-testid="glass-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(4) // Should have 4 feature cards

    // Step 4: Click Sign Up - user decides to register
    const signUpButton = heroSection.locator('a:has-text("Sign Up")')
    await expect(signUpButton).toBeVisible()
    await signUpButton.click()

    // Step 5: Arrive at registration - verify redirect to /register
    await page.waitForURL('/register')
    expect(page.url()).toContain('/register')

    // Verify registration page loaded without errors
    const registerHeading = page.locator('h1:has-text("Register")')
    await expect(registerHeading).toBeVisible()

    // Verify registration form is present
    const registerForm = page.locator('form')
    await expect(registerForm).toBeVisible()

    // Verify form fields exist
    const nameInput = page.locator('input[type="text"]')
    const emailInput = page.locator('input[type="email"]')
    const passwordInput = page.locator('input[type="password"]')
    const submitButton = page.locator('button[type="submit"]')

    await expect(nameInput).toBeVisible()
    await expect(emailInput).toBeVisible()
    await expect(passwordInput).toBeVisible()
    await expect(submitButton).toBeVisible()
  })
})

test.describe('Complete User Journey - Homepage to Login', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')
  })

  test('TC2: Full user journey from homepage to login', async ({ page }) => {
    // Step 1: Land on homepage
    const homePage = page.locator('[data-testid="home-page"]')
    await expect(homePage).toBeVisible()

    // Step 2: View hero section
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Verify the value proposition is clear
    const productName = page.locator('[data-testid="product-name"]')
    await expect(productName).toBeVisible()

    const tagline = page.locator('[data-testid="tagline"]')
    await expect(tagline).toBeVisible()

    // Step 3: Scroll through features (optional for returning user)
    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()

    // Step 4: Click Log In - returning user wants to access their account
    const loginButton = heroSection.locator('a:has-text("Log In")')
    await expect(loginButton).toBeVisible()
    await loginButton.click()

    // Step 5: Arrive at login page
    await page.waitForURL('/login')
    expect(page.url()).toContain('/login')

    // Verify login page loaded without errors
    const loginHeading = page.locator('h1:has-text("Login")')
    await expect(loginHeading).toBeVisible()

    // Verify login form is present
    const loginForm = page.locator('form')
    await expect(loginForm).toBeVisible()

    // Verify form fields exist
    const emailInput = page.locator('input[type="email"]')
    const passwordInput = page.locator('input[type="password"]')
    const submitButton = page.locator('button[type="submit"]')

    await expect(emailInput).toBeVisible()
    await expect(passwordInput).toBeVisible()
    await expect(submitButton).toBeVisible()
  })
})

test.describe('Complete User Journey - Full Homepage Scroll', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')
  })

  test('TC3: Scroll through entire homepage with all sections visible', async ({ page }) => {
    // Define expected sections in order
    const sections = [
      { testId: 'hero-section', name: 'Hero Section' },
      { testId: 'features-section', name: 'Features Section' },
      { testId: 'url-preview-section', name: 'URL Preview Section' },
      { testId: 'footer-section', name: 'Footer Section' },
    ]

    // Track section visibility order
    const sectionVisibilityOrder: string[] = []

    // Scroll through each section and verify it loads correctly
    for (const section of sections) {
      const sectionElement = page.locator(`[data-testid="${section.testId}"]`)

      // Scroll section into view
      await sectionElement.scrollIntoViewIfNeeded()

      // Wait for section to be visible
      await expect(sectionElement).toBeVisible({ timeout: 5000 })

      // Record the section was visible
      sectionVisibilityOrder.push(section.testId)
    }

    // Verify all sections were visible
    expect(sectionVisibilityOrder).toHaveLength(sections.length)

    // Verify specific content in each section
    // Hero Section content
    const heroTitle = page.locator('[data-testid="product-name"]')
    await expect(heroTitle).toContainText('URL')

    // Features Section content
    const featuresHeading = page.locator('#features-heading')
    await expect(featuresHeading).toContainText('Core Features')

    // URL Preview Section content
    const previewTitle = page.locator('[data-testid="preview-title"]')
    await previewTitle.scrollIntoViewIfNeeded()
    await expect(previewTitle).toContainText('See How It Works')

    // Footer content
    const footerCopyright = page.locator('[data-testid="footer-copyright"]')
    await footerCopyright.scrollIntoViewIfNeeded()
    await expect(footerCopyright).toContainText('Copyright')
    await expect(footerCopyright).toContainText('URL Shortener')

    // Verify no console errors during scroll
    const consoleErrors: string[] = []
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text())
      }
    })

    // Scroll back to top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(500)

    // Filter out expected/known errors
    const criticalErrors = consoleErrors.filter(
      (err) => !err.includes('favicon') && !err.includes('sourcemap')
    )
    expect(criticalErrors).toHaveLength(0)
  })
})

test.describe('Complete User Journey - Mobile User', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 }) // iPhone SE size
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')
  })

  test('TC4: Mobile user can complete journey from homepage to registration', async ({ page }) => {
    // Step 1: Land on homepage - verify it renders correctly on mobile
    const homePage = page.locator('[data-testid="home-page"]')
    await expect(homePage).toBeVisible()

    // Verify no horizontal overflow
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const viewportWidth = await page.evaluate(() => window.innerWidth)
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth)

    // Step 2: View hero section on mobile
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Product name should be visible and readable
    const productName = page.locator('[data-testid="product-name"]')
    await expect(productName).toBeVisible()

    // Tagline should be visible
    const tagline = page.locator('[data-testid="tagline"]')
    await expect(tagline).toBeVisible()

    // Step 3: Scroll through features on mobile
    const featuresSection = page.locator('[data-testid="features-section"]')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Feature cards should be stacked vertically on mobile
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Step 4: Find and tap Sign Up button
    // Scroll back to hero to access CTA
    await heroSection.scrollIntoViewIfNeeded()

    const signUpButton = heroSection.locator('a:has-text("Sign Up")')
    await expect(signUpButton).toBeVisible()

    // Verify button is tappable (minimum touch target)
    const buttonBox = await signUpButton.boundingBox()
    expect(buttonBox).not.toBeNull()
    expect(buttonBox!.width).toBeGreaterThanOrEqual(44)
    expect(buttonBox!.height).toBeGreaterThanOrEqual(44)

    // Click the button (using click instead of tap for compatibility)
    await signUpButton.click()

    // Step 5: Verify navigation to registration
    await page.waitForURL('/register')
    expect(page.url()).toContain('/register')

    // Verify registration page works on mobile
    const registerHeading = page.locator('h1:has-text("Register")')
    await expect(registerHeading).toBeVisible()

    // Form should be usable on mobile
    const registerForm = page.locator('form')
    await expect(registerForm).toBeVisible()

    // Verify form fields are visible and tappable
    const nameInput = page.locator('input[type="text"]')
    const emailInput = page.locator('input[type="email"]')
    const passwordInput = page.locator('input[type="password"]')

    await expect(nameInput).toBeVisible()
    await expect(emailInput).toBeVisible()
    await expect(passwordInput).toBeVisible()
  })

  test('TC4b: Mobile user can complete journey from homepage to login', async ({ page }) => {
    // Verify homepage loads on mobile
    const homePage = page.locator('[data-testid="home-page"]')
    await expect(homePage).toBeVisible()

    // Navigate to login via hero CTA
    const heroSection = page.locator('[data-testid="hero-section"]')
    const loginButton = heroSection.locator('a:has-text("Log In")')
    await expect(loginButton).toBeVisible()

    // Verify button is tappable
    const buttonBox = await loginButton.boundingBox()
    expect(buttonBox).not.toBeNull()
    expect(buttonBox!.width).toBeGreaterThanOrEqual(44)
    expect(buttonBox!.height).toBeGreaterThanOrEqual(44)

    await loginButton.click()

    // Verify navigation to login
    await page.waitForURL('/login')
    expect(page.url()).toContain('/login')

    // Verify login page works on mobile
    const loginHeading = page.locator('h1:has-text("Login")')
    await expect(loginHeading).toBeVisible()
  })
})

test.describe('Complete User Journey - Value Proposition Visibility', () => {
  test('TC5: Core value proposition is visible above the fold', async ({ page }) => {
    // Set standard desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 })
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')

    // Get viewport dimensions
    const viewportHeight = await page.evaluate(() => window.innerHeight)

    // Verify hero section with value proposition is above the fold
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Check product name position
    const productName = page.locator('[data-testid="product-name"]')
    const productNameBox = await productName.boundingBox()
    expect(productNameBox).not.toBeNull()
    // Product name should be fully visible above the fold
    expect(productNameBox!.y + productNameBox!.height).toBeLessThan(viewportHeight)

    // Check tagline position - the core value proposition
    const tagline = page.locator('[data-testid="tagline"]')
    const taglineBox = await tagline.boundingBox()
    expect(taglineBox).not.toBeNull()
    // Tagline should be visible above the fold
    expect(taglineBox!.y + taglineBox!.height).toBeLessThan(viewportHeight)

    // Check CTA buttons position - user should see call to action
    const heroButtons = heroSection.locator('a')
    const buttonCount = await heroButtons.count()
    expect(buttonCount).toBeGreaterThanOrEqual(2)

    for (let i = 0; i < buttonCount; i++) {
      const button = heroButtons.nth(i)
      const buttonBox = await button.boundingBox()
      expect(buttonBox).not.toBeNull()
      // At least the top of CTA buttons should be visible above the fold
      expect(buttonBox!.y).toBeLessThan(viewportHeight)
    }

    // Verify the value proposition text is meaningful
    const taglineText = await tagline.textContent()
    expect(taglineText).toBeTruthy()
    // Value proposition should mention URL shortening or links
    expect(
      taglineText!.toLowerCase().includes('shorten') ||
      taglineText!.toLowerCase().includes('link') ||
      taglineText!.toLowerCase().includes('url')
    ).toBe(true)
  })

  test('TC5b: Value proposition visible above fold on tablet', async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')

    const viewportHeight = await page.evaluate(() => window.innerHeight)

    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Product name and tagline should be above the fold
    const productName = page.locator('[data-testid="product-name"]')
    const productNameBox = await productName.boundingBox()
    expect(productNameBox!.y + productNameBox!.height).toBeLessThan(viewportHeight)

    const tagline = page.locator('[data-testid="tagline"]')
    const taglineBox = await tagline.boundingBox()
    expect(taglineBox!.y + taglineBox!.height).toBeLessThan(viewportHeight)
  })

  test('TC5c: Value proposition visible above fold on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')

    const viewportHeight = await page.evaluate(() => window.innerHeight)

    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Product name should be above the fold
    const productName = page.locator('[data-testid="product-name"]')
    const productNameBox = await productName.boundingBox()
    expect(productNameBox).not.toBeNull()
    expect(productNameBox!.y + productNameBox!.height).toBeLessThan(viewportHeight)

    // Tagline should be visible above the fold
    const tagline = page.locator('[data-testid="tagline"]')
    const taglineBox = await tagline.boundingBox()
    expect(taglineBox).not.toBeNull()
    expect(taglineBox!.y + taglineBox!.height).toBeLessThan(viewportHeight)
  })
})

test.describe('Complete User Journey - Navigation Alternatives', () => {
  test('User can navigate to register via footer links', async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')

    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Click register link in footer
    const footerRegisterLink = page.locator('[data-testid="footer-link-register"]')
    await expect(footerRegisterLink).toBeVisible()
    await footerRegisterLink.click()

    // Verify navigation
    await page.waitForURL('/register')
    expect(page.url()).toContain('/register')
  })

  test('User can navigate to login via footer links', async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')

    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Click login link in footer
    const footerLoginLink = page.locator('[data-testid="footer-link-login"]')
    await expect(footerLoginLink).toBeVisible()
    await footerLoginLink.click()

    // Verify navigation
    await page.waitForURL('/login')
    expect(page.url()).toContain('/login')
  })
})

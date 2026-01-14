import { test, expect } from '@playwright/test'

/**
 * User Journey - New Visitor Conversion E2E Tests
 *
 * Scenario: Verify complete user journey from landing to registration
 *
 * Steps:
 * 1. Land on homepage as new visitor (not authenticated)
 * 2. View value proposition (hero section visible)
 * 3. Explore features (scroll to see features section)
 * 4. Decide to sign up (click 'Get Started' button)
 * 5. Arrive at registration (navigate to /register)
 */

test.describe('User Journey - New Visitor Conversion', () => {
  /**
   * Test Case 1: Complete user journey - land -> view -> click Get Started
   * Expected: User successfully navigates from homepage to /register
   */
  test('TC1: Complete user journey from landing to registration via Get Started CTA', async ({ page }) => {
    // Step 1: Land on homepage as new visitor
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify user is on homepage
    await expect(page).toHaveURL('/')

    // Step 2: View value proposition - hero section should be visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify headline is visible and communicates value
    const headline = page.getByRole('heading', { level: 1 })
    await expect(headline).toBeVisible()
    await expect(headline).toContainText('Shorten URLs. Track Results.')

    // Verify subheadline is visible
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()
    await expect(subheadline).toContainText('Create short, memorable links')

    // Step 3: User sees the CTA button clearly in the viewport
    const getStartedButton = page.getByTestId('cta-get-started')
    await expect(getStartedButton).toBeVisible()
    await expect(getStartedButton).toBeEnabled()

    // Verify the button text invites action
    await expect(getStartedButton).toContainText('Get Started')

    // Step 4: User clicks 'Get Started' button to decide to sign up
    await getStartedButton.click()

    // Step 5: User arrives at registration page
    await expect(page).toHaveURL('/register')

    // Verify registration page loaded successfully
    await page.waitForLoadState('networkidle')
  })

  /**
   * Test Case 2: Complete user journey - land -> scroll -> view features -> click Get Started
   * Expected: User can discover features and proceed to registration
   */
  test('TC2: User journey with feature discovery before registration', async ({ page }) => {
    // Step 1: Land on homepage as new visitor
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify user is on homepage
    await expect(page).toHaveURL('/')

    // Step 2: View value proposition - hero section visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // User sees the headline and understands the product
    const headline = page.getByRole('heading', { level: 1 })
    await expect(headline).toContainText('Shorten URLs')

    // Step 3: User scrolls down to explore features
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for any Framer Motion animations to complete
    await page.waitForTimeout(1000)

    // Verify features section is now visible
    await expect(featuresSection).toBeVisible()

    // Verify feature cards are visible (at least 3 features)
    const featureCards = page.getByTestId('feature-card')
    await expect(featureCards.first()).toBeVisible()
    const featureCount = await featureCards.count()
    expect(featureCount).toBeGreaterThanOrEqual(3)

    // Verify features section has a heading
    const featuresHeading = page.locator('#features-heading')
    await expect(featuresHeading).toBeVisible()
    await expect(featuresHeading).toContainText('Powerful Features')

    // Step 4: User scrolls back up and clicks 'Get Started'
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(500)

    const getStartedButton = page.getByTestId('cta-get-started')
    await expect(getStartedButton).toBeVisible()
    await getStartedButton.click()

    // Step 5: User arrives at registration page
    await expect(page).toHaveURL('/register')
  })

  /**
   * Test Case 3: Value proposition clarity timing
   * Expected: Value proposition is clear within 5 seconds of page load
   * Note: This test validates that key elements load quickly
   */
  test('TC3: Value proposition is clear within 5 seconds of page load', async ({ page }) => {
    // Start timing from navigation
    const startTime = Date.now()

    // Navigate to homepage
    await page.goto('/')

    // Wait for hero section to be visible (max 5 seconds implied by test timeout)
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible({ timeout: 5000 })

    // Verify headline is visible within 5 seconds
    const headline = page.getByRole('heading', { level: 1 })
    await expect(headline).toBeVisible({ timeout: 5000 })

    // Verify subheadline (value proposition detail) is visible within 5 seconds
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible({ timeout: 5000 })

    // Verify CTA button is visible within 5 seconds
    const ctaButton = page.getByTestId('cta-get-started')
    await expect(ctaButton).toBeVisible({ timeout: 5000 })

    // Calculate elapsed time
    const elapsedTime = Date.now() - startTime

    // Log timing for manual verification
    console.log(`Value proposition elements loaded in ${elapsedTime}ms`)

    // Assert that all elements loaded within 5 seconds
    expect(elapsedTime).toBeLessThan(5000)

    // Verify the value proposition content is meaningful
    await expect(headline).toContainText('Shorten URLs')
    await expect(subheadline).toContainText('Create short, memorable links')
    await expect(ctaButton).toContainText('Get Started')
  })

  /**
   * Additional test: Verify the complete conversion funnel accessibility
   */
  test('Conversion funnel is keyboard accessible', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Tab to the Get Started button
    const getStartedButton = page.getByTestId('cta-get-started')

    // Click via keyboard
    await getStartedButton.focus()
    await expect(getStartedButton).toBeFocused()
    await page.keyboard.press('Enter')

    // Verify navigation to register page
    await expect(page).toHaveURL('/register')
  })

  /**
   * Additional test: Verify login option is also available
   */
  test('Returning users can access login from homepage', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify login button is visible for returning users
    const loginButton = page.getByTestId('cta-login')
    await expect(loginButton).toBeVisible()
    await expect(loginButton).toContainText('Login')

    // Click login button
    await loginButton.click()

    // Verify navigation to login page
    await expect(page).toHaveURL('/login')
  })

  /**
   * Additional test: Verify smooth scroll navigation works for feature exploration
   */
  test('Users can navigate to features via smooth scroll', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Find the features navigation link in hero section
    const featuresNav = page.getByTestId('nav-features')
    await expect(featuresNav).toBeVisible()

    // Click to scroll to features
    await featuresNav.click()

    // Wait for smooth scroll animation
    await page.waitForTimeout(1000)

    // Verify features section is now in view
    const featuresSection = page.getByTestId('features-section')
    const isInViewport = await featuresSection.evaluate((el) => {
      const rect = el.getBoundingClientRect()
      return rect.top >= -100 && rect.top < window.innerHeight
    })
    expect(isInViewport).toBe(true)
  })
})

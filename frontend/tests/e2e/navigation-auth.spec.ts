import { test, expect } from '@playwright/test'

/**
 * Navigation to Authentication Pages E2E Tests
 * Tests for verifying users can navigate from homepage to registration and login pages
 * as specified in US-4 and US-5 of the PRD
 */
test.describe('Navigation to Authentication Pages', () => {
  // Test Case 1: Click 'Get Started Free' button on homepage navigates to /register
  test('clicking Get Started Free button navigates to /register page', async ({ page }) => {
    // Step 1: Navigate to homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify we're on the homepage
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Step 2: Click the 'Get Started Free' button in hero section
    const getStartedButton = page.getByTestId('hero-cta-primary')
    await expect(getStartedButton).toBeVisible()
    await expect(getStartedButton).toHaveText('Get Started Free')
    await getStartedButton.click()

    // Step 3: Verify navigation to /register page
    await expect(page).toHaveURL('/register')

    // Verify the register page is displayed
    const registerPage = page.getByTestId('register-page')
    await expect(registerPage).toBeVisible()
  })

  // Test Case 2: Click 'Sign Up' button in header navigates to /register
  test('clicking Sign Up button in header navigates to /register page', async ({ page }) => {
    // Step 1: Navigate to homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify we're on the homepage
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Step 2: Click the 'Sign Up' button in header
    const signUpButton = page.getByTestId('nav-signup')
    await expect(signUpButton).toBeVisible()
    await expect(signUpButton).toHaveText('Sign Up')
    await signUpButton.click()

    // Step 3: Verify navigation to /register page
    await expect(page).toHaveURL('/register')

    // Verify the register page is displayed
    const registerPage = page.getByTestId('register-page')
    await expect(registerPage).toBeVisible()
  })

  // Test Case 3: Click 'Log In' button in hero section navigates to /login
  test('clicking Log In button in hero section navigates to /login page', async ({ page }) => {
    // Step 1: Navigate to homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify we're on the homepage
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Step 2: Click the 'Log In' button in hero section
    const loginButton = page.getByTestId('hero-cta-secondary')
    await expect(loginButton).toBeVisible()
    await expect(loginButton).toHaveText('Log In')
    await loginButton.click()

    // Step 3: Verify navigation to /login page
    await expect(page).toHaveURL('/login')

    // Verify the login page is displayed
    const loginPage = page.getByTestId('login-page')
    await expect(loginPage).toBeVisible()
  })

  // Test Case 4: Click 'Log In' button in header navigates to /login
  test('clicking Log In button in header navigates to /login page', async ({ page }) => {
    // Step 1: Navigate to homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify we're on the homepage
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Step 2: Click the 'Log In' button in header
    const loginButton = page.getByTestId('nav-login')
    await expect(loginButton).toBeVisible()
    await expect(loginButton).toHaveText('Log In')
    await loginButton.click()

    // Step 3: Verify navigation to /login page
    await expect(page).toHaveURL('/login')

    // Verify the login page is displayed
    const loginPage = page.getByTestId('login-page')
    await expect(loginPage).toBeVisible()
  })

  // Full user flow: Navigate to register, then back to homepage, then to login
  test('complete navigation flow: homepage -> register -> homepage -> login', async ({ page }) => {
    // Step 1: Start at homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')
    await expect(page.getByTestId('home-page')).toBeVisible()

    // Step 2: Click Sign Up CTA and verify navigation to register
    await page.getByTestId('hero-cta-primary').click()
    await expect(page).toHaveURL('/register')
    await expect(page.getByTestId('register-page')).toBeVisible()

    // Step 4: Return to homepage using browser back
    await page.goBack()
    await expect(page).toHaveURL('/')
    await expect(page.getByTestId('home-page')).toBeVisible()

    // Step 5: Click Log In button and verify navigation
    await page.getByTestId('nav-login').click()
    await expect(page).toHaveURL('/login')

    // Step 6: Verify login page is displayed
    await expect(page.getByTestId('login-page')).toBeVisible()
  })
})

// Mobile navigation tests
test.describe('Navigation to Authentication Pages - Mobile', () => {
  test.use({
    viewport: { width: 375, height: 667 },
  })

  // Test mobile Sign Up button in mobile menu
  test('clicking Sign Up in mobile menu navigates to /register', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Open mobile menu
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await expect(hamburgerMenu).toBeVisible()
    await hamburgerMenu.click()

    // Click Sign Up in mobile menu
    const mobileSignUp = page.getByTestId('mobile-nav-signup')
    await expect(mobileSignUp).toBeVisible()
    await mobileSignUp.click()

    // Verify navigation to register page
    await expect(page).toHaveURL('/register')
    await expect(page.getByTestId('register-page')).toBeVisible()
  })

  // Test mobile Log In button in mobile menu
  test('clicking Log In in mobile menu navigates to /login', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Open mobile menu
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await expect(hamburgerMenu).toBeVisible()
    await hamburgerMenu.click()

    // Click Log In in mobile menu
    const mobileLogin = page.getByTestId('mobile-nav-login')
    await expect(mobileLogin).toBeVisible()
    await mobileLogin.click()

    // Verify navigation to login page
    await expect(page).toHaveURL('/login')
    await expect(page.getByTestId('login-page')).toBeVisible()
  })

  // Test hero section CTA buttons on mobile
  test('hero section CTA buttons work correctly on mobile', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Click Get Started Free on mobile
    const getStartedButton = page.getByTestId('hero-cta-primary')
    await expect(getStartedButton).toBeVisible()
    await getStartedButton.click()

    await expect(page).toHaveURL('/register')
    await expect(page.getByTestId('register-page')).toBeVisible()

    // Go back and test login button
    await page.goBack()
    await expect(page).toHaveURL('/')

    const loginButton = page.getByTestId('hero-cta-secondary')
    await expect(loginButton).toBeVisible()
    await loginButton.click()

    await expect(page).toHaveURL('/login')
    await expect(page.getByTestId('login-page')).toBeVisible()
  })
})

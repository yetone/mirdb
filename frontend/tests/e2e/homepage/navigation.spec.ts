/**
 * Navigation E2E Tests
 * Owner: Scenario 4 - Navigation Component
 *
 * Tests:
 * - Navigation elements are visible (logo, Features link, Login, Sign Up buttons)
 * - Logo click navigates to homepage
 * - Features link scrolls to section
 * - Login button redirects to /login
 * - Sign Up button redirects to /register
 * - Sticky navigation on scroll
 *
 * Requirements: REQ-5
 */

import { test, expect } from '@playwright/test'

test.describe('Navigation Component', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 1: Render navigation component - Logo, Features link, Login and Sign Up buttons are visible
  test('navigation contains logo, Features link, Login and Sign Up buttons', async ({ page }) => {
    // Check navbar is visible
    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()

    // Check logo is visible
    const logo = page.getByTestId('navbar-logo')
    await expect(logo).toBeVisible()
    await expect(logo).toHaveText('LinkShort')

    // Check Features link is visible
    const featuresLink = page.getByTestId('navbar-features-link')
    await expect(featuresLink).toBeVisible()
    await expect(featuresLink).toHaveText('Features')

    // Check Login button is visible
    const loginButton = page.getByTestId('navbar-login-button')
    await expect(loginButton).toBeVisible()
    await expect(loginButton).toHaveText('Login')

    // Check Sign Up button is visible
    const signUpButton = page.getByTestId('navbar-signup-button')
    await expect(signUpButton).toBeVisible()
    await expect(signUpButton).toHaveText('Sign Up')
  })

  // Test Case 2: Click logo/brand name - User is navigated to homepage ('/')
  test('logo click navigates to homepage', async ({ page }) => {
    // Navigate away from homepage first
    await page.goto('/login')
    await expect(page.getByTestId('login-page')).toBeVisible()

    // Click logo
    const logo = page.getByTestId('navbar-logo')
    await logo.click()

    // Verify we're on homepage
    await expect(page).toHaveURL('/')
    await expect(page.getByTestId('home-page')).toBeVisible()
  })

  // Test Case 3: Click Features link - Page smooth scrolls to features section
  test('Features link scrolls to features section', async ({ page }) => {
    // Get features section
    const featuresSection = page.getByTestId('features-section')

    // Verify features section exists but may not be in viewport initially
    await expect(featuresSection).toBeAttached()

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Click Features link
    const featuresLink = page.getByTestId('navbar-features-link')
    await featuresLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500)

    // Verify scroll position changed (scrolled down)
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)

    // Verify features section is now visible in viewport
    await expect(featuresSection).toBeInViewport()
  })

  // Test Case 4: Click Login button - User is redirected to /login page
  test('Login button redirects to /login page', async ({ page }) => {
    // Click Login button
    const loginButton = page.getByTestId('navbar-login-button')
    await loginButton.click()

    // Verify URL changed to /login
    await expect(page).toHaveURL('/login')

    // Verify login page is displayed
    await expect(page.getByTestId('login-page')).toBeVisible()
  })

  // Test Case 5: Click Sign Up button - User is redirected to /register page
  test('Sign Up button redirects to /register page', async ({ page }) => {
    // Click Sign Up button
    const signUpButton = page.getByTestId('navbar-signup-button')
    await signUpButton.click()

    // Verify URL changed to /register
    await expect(page).toHaveURL('/register')

    // Verify register page is displayed
    await expect(page.getByTestId('register-page')).toBeVisible()
  })

  // Test Case 6: Scroll down the page - Navigation remains visible (sticky/fixed header)
  test('navigation remains visible when scrolling (sticky header)', async ({ page }) => {
    // Verify navbar is visible initially
    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500))
    await page.waitForTimeout(100)

    // Verify navbar is still visible
    await expect(navbar).toBeVisible()

    // Check that navbar has fixed positioning
    const navbarClass = await navbar.getAttribute('class')
    expect(navbarClass).toContain('fixed')

    // Verify navbar is still in viewport at top of page
    const boundingBox = await navbar.boundingBox()
    expect(boundingBox).not.toBeNull()
    expect(boundingBox!.y).toBeLessThanOrEqual(0) // Fixed at top
  })

  // Additional test: verify navbar has proper ARIA attributes for accessibility
  test('navigation has proper accessibility attributes', async ({ page }) => {
    const navbar = page.getByTestId('navbar')

    // Check navigation role
    await expect(navbar).toHaveAttribute('role', 'navigation')

    // Check aria-label
    await expect(navbar).toHaveAttribute('aria-label', 'Main navigation')

    // Check logo has aria-label
    const logo = page.getByTestId('navbar-logo')
    await expect(logo).toHaveAttribute('aria-label', 'Go to homepage')
  })
})

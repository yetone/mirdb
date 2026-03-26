/**
 * Navigation E2E Tests
 * Owner: Scenario 4 - Navigation Component
 *
 * Tests:
 * - Logo click navigates to homepage
 * - Features link scrolls to section
 * - Login button redirects to /login
 * - Sign Up button redirects to /register
 * - Sticky navigation on scroll
 *
 * Framework: Playwright
 */

import { test, expect } from '@playwright/test'

test.describe('Navigation Component', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('renders navigation with all required elements', async ({ page }) => {
    // Verify navbar is visible
    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()

    // Verify logo is visible
    const logo = page.getByTestId('navbar-logo')
    await expect(logo).toBeVisible()
    await expect(logo).toHaveText('LinkShort')

    // Verify Features link is visible
    const featuresLink = page.getByTestId('navbar-features-link')
    await expect(featuresLink).toBeVisible()
    await expect(featuresLink).toHaveText('Features')

    // Verify Login button is visible
    const loginBtn = page.getByTestId('navbar-login-btn')
    await expect(loginBtn).toBeVisible()
    await expect(loginBtn).toHaveText('Login')

    // Verify Sign Up button is visible
    const signupBtn = page.getByTestId('navbar-signup-btn')
    await expect(signupBtn).toBeVisible()
    await expect(signupBtn).toHaveText('Sign Up')
  })

  test('logo click navigates to homepage', async ({ page }) => {
    // Navigate away from homepage first
    await page.goto('/login')
    await expect(page.getByTestId('login-page')).toBeVisible()

    // Go back to homepage and click logo
    await page.goto('/')
    const logo = page.getByTestId('navbar-logo')
    await logo.click()

    // Verify we're on homepage
    await expect(page).toHaveURL('/')
    await expect(page.getByTestId('home-page')).toBeVisible()
  })

  test('Features link smooth scrolls to features section', async ({ page }) => {
    // Click Features link
    const featuresLink = page.getByTestId('navbar-features-link')
    await featuresLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500)

    // Verify features section is in view
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeInViewport()
  })

  test('Login button redirects to /login page', async ({ page }) => {
    // Click Login button
    const loginBtn = page.getByTestId('navbar-login-btn')
    await loginBtn.click()

    // Verify redirect to /login
    await expect(page).toHaveURL('/login')
    await expect(page.getByTestId('login-page')).toBeVisible()
  })

  test('Sign Up button redirects to /register page', async ({ page }) => {
    // Click Sign Up button
    const signupBtn = page.getByTestId('navbar-signup-btn')
    await signupBtn.click()

    // Verify redirect to /register
    await expect(page).toHaveURL('/register')
    await expect(page.getByTestId('register-page')).toBeVisible()
  })

  test('navigation remains visible when scrolling (sticky header)', async ({ page }) => {
    // Initial check - navbar visible
    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()

    // Scroll down the page
    await page.evaluate(() => {
      window.scrollTo(0, 1000)
    })

    // Wait for scroll to complete
    await page.waitForTimeout(300)

    // Verify navbar is still visible (fixed position)
    await expect(navbar).toBeVisible()
    await expect(navbar).toBeInViewport()

    // Verify navbar has fixed positioning (check if it's at top of viewport)
    const navbarBox = await navbar.boundingBox()
    expect(navbarBox).toBeTruthy()
    expect(navbarBox!.y).toBeLessThanOrEqual(10) // Should be near top of viewport
  })
})

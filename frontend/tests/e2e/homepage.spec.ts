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

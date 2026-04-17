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

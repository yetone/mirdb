/**
 * E2E Tests for Hero Section
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Hero section is visible above the fold
 * - Gradient or pattern background is applied
 * - All hero elements are properly displayed
 *
 * Requirements: REQ-1, REQ-3
 */

import { test, expect } from '@playwright/test'

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('hero section is visible above the fold with gradient background', async ({ page }) => {
    // Check hero section exists
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Check hero is above the fold (viewport visible without scrolling)
    const heroBox = await heroSection.boundingBox()
    expect(heroBox).not.toBeNull()
    expect(heroBox!.y).toBeLessThan(100) // Hero starts near top of page

    // Check for gradient background styling
    const heroClass = await heroSection.getAttribute('class')
    expect(heroClass).toContain('gradient')

    // Verify headline is visible
    const headline = page.getByRole('heading', { level: 1 })
    await expect(headline).toBeVisible()

    // Verify description is visible
    const description = page.getByTestId('hero-description')
    await expect(description).toBeVisible()

    // Verify CTAs are visible
    const primaryCta = page.getByTestId('primary-cta')
    const secondaryCta = page.getByTestId('secondary-cta')
    await expect(primaryCta).toBeVisible()
    await expect(secondaryCta).toBeVisible()
  })

  test('hero section contains URL shortening value proposition', async ({ page }) => {
    const headline = page.getByRole('heading', { level: 1 })
    const headlineText = await headline.textContent()

    // Verify value proposition mentions URL shortening
    expect(headlineText?.toLowerCase()).toMatch(/shorten|url/i)
  })

  test('hero section CTAs are interactive', async ({ page }) => {
    const primaryCta = page.getByTestId('primary-cta')
    const secondaryCta = page.getByTestId('secondary-cta')

    // Check buttons are clickable (enabled)
    await expect(primaryCta).toBeEnabled()
    await expect(secondaryCta).toBeEnabled()

    // Check visual distinction - primary has filled style, secondary has outline
    const primaryClass = await primaryCta.getAttribute('class')
    const secondaryClass = await secondaryCta.getAttribute('class')

    expect(primaryClass).toContain('btn-primary')
    expect(secondaryClass).toContain('btn-outline')
  })
})

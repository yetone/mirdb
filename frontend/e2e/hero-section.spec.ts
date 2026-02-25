/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 (Hero Section Display)
 *
 * End-to-end tests for the hero section:
 * - Hero section renders above the fold with background styling
 */

import { test, expect } from '@playwright/test'

test.describe('Hero Section Display', () => {
  test('hero section renders above the fold with background styling', async ({ page }) => {
    await page.goto('/')

    // Verify hero section is present and visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify hero section is above the fold (has significant height)
    const boundingBox = await heroSection.boundingBox()
    expect(boundingBox).toBeTruthy()
    expect(boundingBox!.height).toBeGreaterThan(400)

    // Verify hero section has background styling (gradient classes)
    await expect(heroSection).toHaveClass(/bg-gradient/)

    // Verify all main elements are visible
    await expect(page.getByText('URL Shortening Service')).toBeVisible()
    await expect(page.getByText('Shorten Links. Track Insights. Share Smarter.')).toBeVisible()
    await expect(page.getByTestId('url-input')).toBeVisible()
    await expect(page.getByTestId('shorten-url-button')).toBeVisible()
    await expect(page.getByTestId('signup-button')).toBeVisible()
    await expect(page.getByTestId('login-button')).toBeVisible()
  })
})

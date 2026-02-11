/**
 * E2E tests for Hero section.
 * Owner: Scenario 2 - Hero Section
 */

import { test, expect } from '@playwright/test'

test.describe('Hero Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 5: Click primary CTA button navigates to Quick Start section
  test('clicking Get Started button scrolls to Quick Start section', async ({ page }) => {
    // Find and click the primary CTA button
    const getStartedButton = page.getByRole('button', { name: /Get Started/i })
    await expect(getStartedButton).toBeVisible()

    // Click the button
    await getStartedButton.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000)

    // Verify that we scrolled to the quick-start section
    const quickStartSection = page.locator('#quick-start')
    await expect(quickStartSection).toBeInViewport()
  })

  // Test Case 6: Click secondary GitHub CTA opens new tab with GitHub URL
  test('clicking GitHub button has correct link attributes', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.getByRole('link', { name: /GitHub/i })
    await expect(githubLink).toBeVisible()

    // Verify the href points to GitHub repository
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')

    // Verify security attributes
    await expect(githubLink).toHaveAttribute('target', '_blank')
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  test('hero section renders with headline and description', async ({ page }) => {
    // Check headline
    const headline = page.getByRole('heading', { level: 1 })
    await expect(headline).toBeVisible()
    await expect(headline).toContainText('MirDB')
    await expect(headline).toContainText('Persistent Key-Value Store')

    // Check description mentions Memcached
    const description = page.locator('.hero__description')
    await expect(description).toContainText('Memcached')
  })

  test('hero section has both CTA buttons visible', async ({ page }) => {
    const primaryButton = page.getByRole('button', { name: /Get Started/i })
    const secondaryButton = page.getByRole('link', { name: /GitHub/i })

    await expect(primaryButton).toBeVisible()
    await expect(secondaryButton).toBeVisible()
  })
})

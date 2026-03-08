import { test, expect } from '@playwright/test'

/**
 * E2E tests for External Links and Resources (Scenario 6)
 * Owner: Scenario 6 - External Links and Resources
 *
 * Tests verify that external links are present, accessible,
 * and navigate correctly.
 */

test.describe('External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 6: Click GitHub link - Link navigates to valid GitHub repository
  test('GitHub link navigates to valid GitHub repository', async ({ page, context }) => {
    // Find GitHub link in the header
    const githubLink = page.locator('a[href*="github.com"]').first()
    await expect(githubLink).toBeVisible()

    // Verify link has correct attributes before clicking
    await expect(githubLink).toHaveAttribute('target', '_blank')
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Get the href value
    const href = await githubLink.getAttribute('href')
    expect(href).toBeTruthy()
    expect(href).toContain('github.com')

    // Wait for the new page to open when clicking the link
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click(),
    ])

    // Wait for the new page to load
    await newPage.waitForLoadState('domcontentloaded')

    // Verify we navigated to a GitHub page
    const newPageUrl = newPage.url()
    expect(newPageUrl).toContain('github.com')

    // Close the new page
    await newPage.close()
  })

  // Verify GitHub link presence on page
  test('page contains GitHub link with github.com href', async ({ page }) => {
    const githubLinks = page.locator('a[href*="github.com"]')
    const count = await githubLinks.count()
    expect(count).toBeGreaterThan(0)
  })

  // Verify all external links have security attributes
  test('all external links have security attributes', async ({ page }) => {
    // Find all external links (target="_blank")
    const externalLinks = page.locator('a[target="_blank"]')
    const count = await externalLinks.count()

    if (count > 0) {
      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i)
        await expect(link).toHaveAttribute('rel', 'noopener noreferrer')
      }
    }
  })

  // Verify GitHub link opens in new tab
  test('GitHub link opens in new tab', async ({ page }) => {
    const githubLink = page.locator('a[href*="github.com"]').first()
    await expect(githubLink).toBeVisible()
    await expect(githubLink).toHaveAttribute('target', '_blank')
  })

  // Verify external link accessibility
  test('GitHub link has accessible name', async ({ page }) => {
    const githubLink = page.locator('a[href*="github.com"]').first()
    const accessibleName = await githubLink.evaluate((el) => {
      // Check aria-label, aria-labelledby, or text content
      return el.getAttribute('aria-label') || el.textContent?.trim()
    })
    expect(accessibleName).toBeTruthy()
    expect(accessibleName?.toLowerCase()).toMatch(/github/i)
  })
})

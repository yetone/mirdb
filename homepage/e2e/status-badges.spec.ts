import { test, expect } from '@playwright/test'

/**
 * E2E tests for Project Status Badges - Scenario 7
 * Tests the CircleCI build status badge functionality (REQ-7, US-4)
 */
test.describe('Project Status Badges', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 4: Click status badge - opens CircleCI build page in new tab
  test('CircleCI badge click opens build page in new tab', async ({ page, context }) => {
    // Get the CircleCI badge link
    const badgeLink = page.getByTestId('circleci-badge-link')
    await expect(badgeLink).toBeVisible()

    // Verify the link has target="_blank" for opening in new tab
    const target = await badgeLink.getAttribute('target')
    expect(target).toBe('_blank')

    // Verify the href contains CircleCI URL
    const href = await badgeLink.getAttribute('href')
    expect(href).toContain('circleci')
    expect(href).toContain('theseus-rs/mirdb')

    // Listen for new page (popup) when clicking
    const pagePromise = context.waitForEvent('page')

    // Click the badge
    await badgeLink.click()

    // Verify new tab was opened
    const newPage = await pagePromise
    await newPage.waitForLoadState('domcontentloaded')

    // Verify the new page URL contains circleci
    expect(newPage.url()).toContain('circleci')

    // Clean up
    await newPage.close()
  })

  test('CircleCI badge is visible in header', async ({ page }) => {
    // Get the status badges container in the header
    const statusBadges = page.getByTestId('status-badges')
    await expect(statusBadges).toBeVisible()

    // Verify the CircleCI badge image is visible
    const badgeImage = page.getByTestId('circleci-badge-image')
    await expect(badgeImage).toBeVisible()
  })

  test('CircleCI badge image has correct attributes', async ({ page }) => {
    const badgeImage = page.getByTestId('circleci-badge-image')

    // Verify src contains circleci
    const src = await badgeImage.getAttribute('src')
    expect(src).toContain('circleci')

    // Verify alt text mentions build status
    const alt = await badgeImage.getAttribute('alt')
    expect(alt?.toLowerCase()).toContain('build status')
  })

  test('CircleCI badge link has proper accessibility attributes', async ({ page }) => {
    const badgeLink = page.getByTestId('circleci-badge-link')

    // Verify proper security attributes for external link
    const rel = await badgeLink.getAttribute('rel')
    expect(rel).toContain('noopener')
    expect(rel).toContain('noreferrer')

    // Verify accessible label
    const ariaLabel = await badgeLink.getAttribute('aria-label')
    expect(ariaLabel).toBeTruthy()
    expect(ariaLabel?.toLowerCase()).toContain('build status')
  })
})

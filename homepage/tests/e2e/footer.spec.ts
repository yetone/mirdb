/**
 * Footer E2E Tests
 * Owner: Scenario 8 - Footer & External Links
 *
 * Tests for the footer functionality including:
 * - Test Case 4: Click GitHub link in footer - opens in new tab
 * - Test Case 7: Footer visibility on all viewports (desktop, tablet, mobile)
 */
import { test, expect } from '@playwright/test'

test.describe('Footer', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 4: Click GitHub link in footer - opens in new tab
  test('GitHub link in footer opens repository in new tab', async ({ page, context }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Find GitHub link in footer
    const githubLink = page.locator('[data-testid="github-link"]')
    await expect(githubLink).toBeVisible()

    // Verify href
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')

    // Verify target="_blank"
    await expect(githubLink).toHaveAttribute('target', '_blank')

    // Verify rel="noopener noreferrer"
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Test that clicking opens in a new tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ])

    // Verify new page opened with correct URL
    await expect(newPage).toHaveURL(/github\.com\/yetone\/mirdb/i)
  })

  // Test Case 7: Footer visibility on desktop (1920x1080)
  test('footer is visible and properly styled on desktop viewport', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 })

    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()

    // Verify footer is visible
    await expect(footer).toBeVisible()

    // Verify copyright is visible
    const copyright = page.locator('[data-testid="copyright"]')
    await expect(copyright).toBeVisible()
    await expect(copyright).toContainText('MirDB')

    // Verify license information is visible
    const license = page.locator('[data-testid="license"]')
    await expect(license).toBeVisible()
    await expect(license).toContainText('MIT License')

    // Verify GitHub link is visible
    const githubLink = page.locator('[data-testid="github-link"]')
    await expect(githubLink).toBeVisible()

    // Verify CircleCI badge is visible
    const badge = page.locator('[data-testid="circleci-badge"]')
    await expect(badge).toBeVisible()

    // Verify footer has proper layout (3-column grid on desktop)
    const footerContent = footer.locator('div').first()
    await expect(footerContent).toBeVisible()
  })

  // Test Case 7: Footer visibility on tablet (768x1024)
  test('footer is visible and properly styled on tablet viewport', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 })

    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()

    // Verify footer is visible
    await expect(footer).toBeVisible()

    // Verify key elements are visible
    const copyright = page.locator('[data-testid="copyright"]')
    await expect(copyright).toBeVisible()

    const license = page.locator('[data-testid="license"]')
    await expect(license).toBeVisible()

    const githubLink = page.locator('[data-testid="github-link"]')
    await expect(githubLink).toBeVisible()

    const badge = page.locator('[data-testid="circleci-badge"]')
    await expect(badge).toBeVisible()
  })

  // Test Case 7: Footer visibility on mobile (375x667)
  test('footer is visible and properly styled on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()

    // Verify footer is visible
    await expect(footer).toBeVisible()

    // Verify key elements are visible
    const copyright = page.locator('[data-testid="copyright"]')
    await expect(copyright).toBeVisible()

    const license = page.locator('[data-testid="license"]')
    await expect(license).toBeVisible()

    const githubLink = page.locator('[data-testid="github-link"]')
    await expect(githubLink).toBeVisible()

    const badge = page.locator('[data-testid="circleci-badge"]')
    await expect(badge).toBeVisible()

    // Verify footer fits within viewport width (footer-specific overflow check)
    const footerBox = await footer.boundingBox()
    const viewportWidth = 375
    expect(footerBox).not.toBeNull()
    expect(footerBox!.width).toBeLessThanOrEqual(viewportWidth)
  })

  // Test Case 7: Footer visibility on small mobile (320x568)
  test('footer is visible and properly styled on small mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 })

    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()

    // Verify footer is visible
    await expect(footer).toBeVisible()

    // Verify key elements are visible
    const copyright = page.locator('[data-testid="copyright"]')
    await expect(copyright).toBeVisible()

    const license = page.locator('[data-testid="license"]')
    await expect(license).toBeVisible()

    const githubLink = page.locator('[data-testid="github-link"]')
    await expect(githubLink).toBeVisible()

    // Verify content is readable (not cut off)
    const copyrightText = await copyright.textContent()
    expect(copyrightText).toContain('MirDB')
  })

  test('all external links in footer have proper security attributes', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()

    // Check GitHub link
    const githubLink = page.locator('[data-testid="github-link"]')
    await expect(githubLink).toHaveAttribute('target', '_blank')
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Check documentation link
    const docsLink = page.locator('[data-testid="docs-link"]')
    await expect(docsLink).toHaveAttribute('target', '_blank')
    await expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Check issues link
    const issuesLink = page.locator('[data-testid="issues-link"]')
    await expect(issuesLink).toHaveAttribute('target', '_blank')
    await expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Check CircleCI badge link
    const badgeLink = page.locator('[data-testid="circleci-badge-link"]')
    await expect(badgeLink).toHaveAttribute('target', '_blank')
    await expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Check license link
    const licenseLink = page.locator('[data-testid="license-link"]')
    await expect(licenseLink).toHaveAttribute('target', '_blank')
    await expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  test('CircleCI badge is clickable and links to build status', async ({ page, context }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()

    // Find CircleCI badge link
    const badgeLink = page.locator('[data-testid="circleci-badge-link"]')
    await expect(badgeLink).toBeVisible()

    // Verify href
    await expect(badgeLink).toHaveAttribute(
      'href',
      'https://dl.circleci.com/status-badge/redirect/gh/yetone/mirdb/tree/master'
    )

    // Verify badge image exists
    const badgeImage = page.locator('[data-testid="circleci-badge"]')
    await expect(badgeImage).toBeVisible()
    await expect(badgeImage).toHaveAttribute('alt', 'CircleCI build status')
  })

  test('footer has correct semantic structure', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]')
    await footer.scrollIntoViewIfNeeded()

    // Verify footer has role="contentinfo"
    await expect(footer).toHaveAttribute('role', 'contentinfo')

    // Verify Links section heading exists
    await expect(page.getByText('Links')).toBeVisible()

    // Verify Build Status section heading exists
    await expect(page.getByText('Build Status')).toBeVisible()
  })
})

/**
 * Footer Component E2E Tests
 * Owner: Scenario 9 - Footer Component
 *
 * Test coverage:
 * - GitHub link is present and opens in new tab
 * - Documentation link is present and opens in new tab
 */

import { test, expect } from '@playwright/test'

test.describe('Footer Component', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Scroll to footer to ensure it's visible
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))
    // Wait for the footer section to be rendered
    await page.waitForSelector('[data-testid="footer-section"]')
  })

  test('Test Case 2: GitHub link is present and functional', async ({ page, context }) => {
    // Verify the GitHub link exists in footer
    const githubLink = page.locator('[data-testid="footer-github-link"]')
    await expect(githubLink).toBeVisible()

    // Verify text contains GitHub
    await expect(githubLink).toContainText(/GitHub/i)

    // Verify the link has correct attributes for opening in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank')
    await expect(githubLink).toHaveAttribute('rel', /noopener/)

    // Verify the href points to a GitHub URL
    const href = await githubLink.getAttribute('href')
    expect(href).toMatch(/github\.com/)
    expect(href).toMatch(/mirdb/i)

    // Test that clicking opens a new page/tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ])

    // Verify the new page URL is the GitHub repository
    const newPageUrl = newPage.url()
    expect(newPageUrl).toMatch(/github\.com/)

    // Clean up
    await newPage.close()
  })

  test('Test Case 3: Documentation link is present and functional', async ({ page, context }) => {
    // Verify the documentation link exists in footer
    const docsLink = page.locator('[data-testid="footer-documentation-link"]')
    await expect(docsLink).toBeVisible()

    // Verify text contains Documentation
    await expect(docsLink).toContainText(/Documentation/i)

    // Verify the link has correct attributes for opening in new tab
    await expect(docsLink).toHaveAttribute('target', '_blank')
    await expect(docsLink).toHaveAttribute('rel', /noopener/)

    // Verify the href points to documentation
    const href = await docsLink.getAttribute('href')
    expect(href).toMatch(/github\.com/)
    expect(href).toMatch(/readme/i)

    // Test that clicking opens a new page/tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      docsLink.click()
    ])

    // Verify the new page URL is the documentation
    const newPageUrl = newPage.url()
    expect(newPageUrl).toMatch(/github\.com/)

    // Clean up
    await newPage.close()
  })

  test('Footer should display license and copyright information', async ({ page }) => {
    // Verify license information is displayed
    const license = page.locator('[data-testid="footer-license"]')
    await expect(license).toBeVisible()
    await expect(license).toContainText(/MIT/i)

    // Verify copyright notice is displayed
    const copyright = page.locator('[data-testid="footer-copyright"]')
    await expect(copyright).toBeVisible()

    // Verify current year is in copyright
    const currentYear = new Date().getFullYear().toString()
    await expect(copyright).toContainText(currentYear)

    // Verify copyright symbol
    await expect(copyright).toContainText('©')

    // Verify project name
    await expect(copyright).toContainText('MirDB')
  })
})

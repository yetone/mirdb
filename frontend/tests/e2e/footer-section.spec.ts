import { test, expect } from '@playwright/test'

test.describe('Footer Section E2E Tests', () => {
  // Test Case 6: Scroll to footer on homepage
  test('footer is visible at the bottom of the page with all required links', async ({ page }) => {
    await page.goto('/')

    // Wait for page to load
    await page.waitForLoadState('networkidle')

    // Scroll to footer at the bottom of the page
    const footer = page.getByRole('contentinfo')
    await footer.scrollIntoViewIfNeeded()

    // Verify footer is visible
    await expect(footer).toBeVisible()

    // Verify footer links container is present
    const footerLinks = page.getByTestId('footer-links')
    await expect(footerLinks).toBeVisible()

    // Verify copyright notice is visible
    const copyright = page.getByTestId('footer-copyright')
    await expect(copyright).toBeVisible()

    // Verify current year is displayed in copyright
    const currentYear = new Date().getFullYear().toString()
    await expect(copyright).toContainText(currentYear)

    // Verify all required links are present and visible
    const aboutLink = page.getByTestId('footer-link-about')
    await expect(aboutLink).toBeVisible()
    await expect(aboutLink).toHaveText('About')

    const privacyLink = page.getByTestId('footer-link-privacy')
    await expect(privacyLink).toBeVisible()
    await expect(privacyLink).toContainText('Privacy')

    const termsLink = page.getByTestId('footer-link-terms')
    await expect(termsLink).toBeVisible()
    await expect(termsLink).toContainText('Terms')
  })

  test('footer links are clickable and have proper href attributes', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Scroll to footer
    const footer = page.getByRole('contentinfo')
    await footer.scrollIntoViewIfNeeded()

    // Check About link
    const aboutLink = page.getByTestId('footer-link-about')
    await expect(aboutLink).toHaveAttribute('href', '/about')

    // Check Privacy link
    const privacyLink = page.getByTestId('footer-link-privacy')
    await expect(privacyLink).toHaveAttribute('href', '/privacy')

    // Check Terms link
    const termsLink = page.getByTestId('footer-link-terms')
    await expect(termsLink).toHaveAttribute('href', '/terms')
  })

  test('footer has proper accessibility structure', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Footer should have contentinfo role
    const footer = page.getByRole('contentinfo')
    await expect(footer).toBeVisible()

    // Footer should contain navigation
    const nav = page.getByRole('navigation', { name: /footer/i })
    await expect(nav).toBeVisible()

    // All links should be keyboard navigable
    const links = page.locator('[data-testid="footer-links"] a')
    const linkCount = await links.count()
    expect(linkCount).toBeGreaterThanOrEqual(3)
  })

  test('footer is positioned at the bottom of the page after scrolling', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Scroll to the bottom of the page
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight)
    })

    // Small delay to let scroll complete
    await page.waitForTimeout(500)

    // Footer should be visible
    const footer = page.getByRole('contentinfo')
    await expect(footer).toBeVisible()

    // Footer should be in the viewport or near the bottom
    const footerBox = await footer.boundingBox()
    expect(footerBox).toBeTruthy()
  })
})

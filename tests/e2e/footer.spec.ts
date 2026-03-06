/**
 * Footer Section E2E Tests
 * Owner: Scenario 8 - Footer Section
 *
 * Test coverage:
 * - Footer element presence
 * - GitHub link in footer
 * - Copyright/license information
 */
import { test, expect } from '@playwright/test';
import { waitForPageLoad, navigateToSection } from './test-utils';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Footer element exists in the DOM', async ({ page }) => {
    // Find the footer element
    const footer = page.locator('footer.footer');

    // Verify footer exists and is visible
    await expect(footer).toBeVisible();

    // Verify footer has role="contentinfo" for accessibility
    const role = await footer.getAttribute('role');
    expect(role).toBe('contentinfo');
  });

  test('TC2: Footer contains GitHub link with correct href', async ({ page }) => {
    // Find the GitHub link in the footer
    const githubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');

    // Verify the link exists and is visible
    await expect(githubLink).toBeVisible();

    // Verify the link text contains GitHub reference
    await expect(githubLink).toContainText(/GitHub/i);

    // Verify target="_blank" attribute for external link
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('TC3: Footer contains copyright or license information', async ({ page }) => {
    // Find the footer element
    const footer = page.locator('footer.footer');

    // Get footer text content
    const footerText = await footer.textContent();

    // Verify footer contains copyright symbol or the word "copyright" or license info
    const hasCopyright = footerText?.includes('©') ||
                         footerText?.toLowerCase().includes('copyright') ||
                         footerText?.toLowerCase().includes('license') ||
                         footerText?.toLowerCase().includes('mit');

    expect(hasCopyright).toBe(true);
  });

  test('Footer is positioned at the bottom of the page', async ({ page }) => {
    // Scroll to the footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer is visible
    await expect(footer).toBeVisible();

    // Verify footer is a child of body (at document level)
    const parentTag = await footer.evaluate(el => el.parentElement?.tagName);
    expect(parentTag).toBe('BODY');
  });

  test('Footer has proper semantic structure', async ({ page }) => {
    // Verify footer element is present
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer has the class "footer"
    await expect(page.locator('footer.footer')).toBeVisible();

    // Verify footer has contentinfo role for accessibility
    const footerWithRole = page.locator('footer[role="contentinfo"]');
    await expect(footerWithRole).toBeVisible();
  });

  test('Footer GitHub link has accessible screen reader text', async ({ page }) => {
    // Find the GitHub link in the footer
    const githubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');

    // Check for screen reader only text or aria-label
    const ariaLabel = await githubLink.getAttribute('aria-label');
    const srOnlyText = await githubLink.locator('.sr-only').textContent().catch(() => null);

    // Either aria-label or sr-only text should indicate external link
    const hasA11yText = ariaLabel?.includes('new tab') ||
                        ariaLabel?.includes('external') ||
                        srOnlyText?.includes('new tab') ||
                        srOnlyText?.includes('external');

    expect(hasA11yText).toBe(true);
  });

  test('Footer contains all required links', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Verify GitHub link exists
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify there's at least one link in the footer (GitHub)
    const linkCount = await footer.locator('a').count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });
});

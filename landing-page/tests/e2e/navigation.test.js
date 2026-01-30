/**
 * MirDB Landing Page - Navigation E2E Tests
 * Owner: Scenario 8 - Navigation Bar Functionality
 *
 * Tests verify:
 * - Navigation bar is visible with logo
 * - All section links are present and functional
 * - Smooth scroll functionality works
 * - GitHub link opens in new tab
 */

const { test, expect } = require('@playwright/test');
const { setupPage, waitForAnimations } = require('../helpers/test-utils');

test.describe('Navigation Bar Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  // Test Case 1: Check navigation bar contains logo
  test('TC1: MirDB logo is present in left side of navigation', async ({ page }) => {
    // Verify navigation bar exists
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Verify logo is present in navigation
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    // Verify logo text or link contains MirDB
    const logoText = await navLogo.textContent();
    expect(logoText).toContain('MirDB');

    // Verify logo is on the left side (has link to hero)
    const logoHref = await navLogo.getAttribute('href');
    expect(logoHref).toBe('#hero');
  });

  // Test Case 2: Check for Features navigation link
  test('TC2: Features link is present and anchors to features section', async ({ page }) => {
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Find Features link in navigation
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Verify link text
    const linkText = await featuresLink.textContent();
    expect(linkText).toContain('Features');

    // Verify href points to features section
    const href = await featuresLink.getAttribute('href');
    expect(href).toBe('#features');

    // Verify features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeAttached();
  });

  // Test Case 3: Check for Usage navigation link
  test('TC3: Usage link is present and anchors to usage section', async ({ page }) => {
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Find Usage link in navigation
    const usageLink = page.locator('.nav-links a[href="#usage"]');
    await expect(usageLink).toBeVisible();

    // Verify link text
    const linkText = await usageLink.textContent();
    expect(linkText).toContain('Usage');

    // Verify href points to usage section
    const href = await usageLink.getAttribute('href');
    expect(href).toBe('#usage');

    // Verify usage section exists
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeAttached();
  });

  // Test Case 4: Check for Architecture navigation link
  test('TC4: Architecture link is present and anchors to architecture section', async ({ page }) => {
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Find Architecture link in navigation
    const architectureLink = page.locator('.nav-links a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();

    // Verify link text
    const linkText = await architectureLink.textContent();
    expect(linkText).toContain('Architecture');

    // Verify href points to architecture section
    const href = await architectureLink.getAttribute('href');
    expect(href).toBe('#architecture');

    // Verify architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeAttached();
  });

  // Test Case 5: Click Features link - page smoothly scrolls to features section
  test('TC5: Clicking Features link smoothly scrolls to features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find and click Features link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for smooth scroll animation
    await waitForAnimations(page, 1000);

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is now in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash updated
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');
  });

  // Test Case 6: Click GitHub link opens repository in new tab
  test('TC6: GitHub link opens https://github.com/yetone/mirdb in new tab', async ({ page, context }) => {
    // Find GitHub link in navigation
    const githubLink = page.locator('.nav-links a', { hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    // Verify correct href
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify target="_blank" for new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify security attributes
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Listen for new page (popup) event
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ]);

    // Verify the new tab opened with correct URL
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com/yetone/mirdb');

    // Verify original page is still on the landing page (didn't navigate away)
    const originalUrl = page.url();
    expect(originalUrl).toContain('localhost');
  });

  // Additional test: Navigation bar is fixed/sticky at top
  test('Navigation bar remains visible when scrolling', async ({ page }) => {
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await waitForAnimations(page, 500);

    // Navigation should still be visible (sticky/fixed position)
    await expect(nav).toBeVisible();
  });

  // Additional test: Navigation has proper accessibility attributes
  test('Navigation has proper ARIA attributes', async ({ page }) => {
    const nav = page.locator('nav.nav');

    // Verify aria-label for screen readers
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');
  });
});

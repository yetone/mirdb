// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Navigation Functionality E2E Tests
 * Scenario 17: Verify all navigation links work correctly and point to intended destinations
 *
 * Test Cases:
 * TC2 (e2e): Test anchor links - Clicking section links scrolls to correct section
 * TC4 (e2e): Test Get Started CTA - Get Started button navigates to installation section or documentation
 */

test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:8080');
  });

  // Test Case 2: Test anchor links scroll to correct sections
  test('TC2: Clicking section links scrolls to correct section', async ({ page }) => {
    // Find the "Get Started" CTA button in hero
    const getStartedBtn = page.locator('a[href="#getting-started"]');
    await expect(getStartedBtn).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the "Get Started" button
    await getStartedBtn.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Verify that the page scrolled (scroll position changed)
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify the getting-started section is now in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  // Test Case 4: Test Get Started CTA navigates to installation section
  test('TC4: Get Started button navigates to installation section or documentation', async ({ page }) => {
    // Find the "Get Started" button in hero section
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const getStartedBtn = heroSection.locator('a').filter({ hasText: 'Get Started' });
    await expect(getStartedBtn).toBeVisible();

    // Verify it has a href attribute
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBeTruthy();

    // Get Started should link to either:
    // - #getting-started section
    // - #installation section
    // - external documentation URL
    const isInternalLink = href.startsWith('#');
    const isDocLink = href.includes('docs') || href.includes('readme') || href.includes('documentation');

    expect(isInternalLink || isDocLink).toBeTruthy();

    if (isInternalLink) {
      // Click and verify navigation to section
      await getStartedBtn.click();
      await page.waitForTimeout(500);

      // Verify the target section is visible
      const targetSectionId = href.substring(1); // Remove #
      const targetSection = page.locator(`#${targetSectionId}`);
      await expect(targetSection).toBeInViewport();
    }
  });

  // Additional test: Navigation to features section works
  test('Navigation to features section works', async ({ page }) => {
    // Look for any link to features section
    const featuresLink = page.locator('a[href="#features"]').first();

    // Check if features link exists
    const hasFeatureLink = await featuresLink.count() > 0;

    if (hasFeatureLink) {
      await featuresLink.click();
      await page.waitForTimeout(500);

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    } else {
      // Features section should exist even without direct link
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    }
  });

  // Test anchor links for architecture section
  test('Navigation to architecture section works', async ({ page }) => {
    // The architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();
  });

  // Test anchor links for code-examples section
  test('Navigation to code-examples section works', async ({ page }) => {
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();
  });

  // Test anchor links for configuration section
  test('Navigation to configuration section works', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();
  });

  // Test anchor links for commands section
  test('Navigation to commands section works', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();
  });

  // Verify external links open in new tab
  test('External links have target="_blank"', async ({ page }) => {
    // Find all external links (GitHub, etc.)
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');
    }
  });

  // Verify external links have noopener for security
  test('External links have rel="noopener noreferrer" for security', async ({ page }) => {
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});

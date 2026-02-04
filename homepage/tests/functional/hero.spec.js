/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section
 *
 * Test coverage:
 * - Hero section visibility and content
 * - Animated logo display
 * - CTA button functionality
 * - Above-the-fold requirements
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section renders with logo, heading, tagline, and description visible', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify logo is displayed
    const logo = heroSection.locator('img.hero__logo');
    await expect(logo).toBeVisible();

    // Verify heading is "MirDB"
    const heading = heroSection.locator('h1');
    await expect(heading).toContainText('MirDB');

    // Verify tagline
    const tagline = heroSection.locator('.hero__tagline');
    await expect(tagline).toContainText('A Persistent Key-Value Store with Memcached Protocol');

    // Verify description is present
    const description = heroSection.locator('.hero__description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Drop-in memcached replacement');
  });

  test('TC2: Animated logo displays with appropriate alt text for accessibility', async ({ page }) => {
    const logo = page.locator('#hero img.hero__logo');

    // Verify logo is visible
    await expect(logo).toBeVisible();

    // Verify it's a gif (animated logo)
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Verify alt text exists for accessibility
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(5);

    // Verify image has dimensions set for layout stability
    const width = await logo.getAttribute('width');
    const height = await logo.getAttribute('height');
    expect(width).toBeTruthy();
    expect(height).toBeTruthy();
  });

  test('TC3: Get Started button navigates to getting started section', async ({ page }) => {
    // Find the "Get Started" button
    const getStartedBtn = page.locator('#hero a.btn--primary:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    // Verify it links to the code-examples section (getting started/installation section)
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#code-examples');

    // Click and verify navigation
    await getStartedBtn.click();

    // Wait for scroll and verify URL hash
    await page.waitForTimeout(500); // Wait for smooth scroll
    const currentUrl = page.url();
    expect(currentUrl).toContain('#code-examples');

    // Verify the code-examples section is in viewport
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeInViewport();
  });

  test('TC4: View on GitHub button opens github.com/yetone/mirdb in new tab', async ({ page, context }) => {
    // Find the GitHub button
    const githubBtn = page.locator('#hero a:has-text("View on GitHub")');
    await expect(githubBtn).toBeVisible();

    // Verify href
    const href = await githubBtn.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in new tab
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Test that clicking opens a new page
    const pagePromise = context.waitForEvent('page');
    await githubBtn.click();
    const newPage = await pagePromise;

    // Verify the new page URL
    await newPage.waitForLoadState('domcontentloaded');
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    await newPage.close();
  });

  test('TC5: All hero content visible on 1366x768 viewport (above the fold)', async ({ page }) => {
    // Set viewport to 1366x768
    await page.setViewportSize({ width: 1366, height: 768 });
    await page.goto('/');

    // Verify hero section elements are in viewport
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();

    // Check logo is in viewport
    const logo = page.locator('#hero img.hero__logo');
    await expect(logo).toBeInViewport();

    // Check heading is in viewport
    const heading = page.locator('#hero h1');
    await expect(heading).toBeInViewport();

    // Check tagline is in viewport
    const tagline = page.locator('#hero .hero__tagline');
    await expect(tagline).toBeInViewport();

    // Check description is in viewport
    const description = page.locator('#hero .hero__description');
    await expect(description).toBeInViewport();

    // Check CTA buttons are in viewport
    const getStartedBtn = page.locator('#hero a:has-text("Get Started")');
    await expect(getStartedBtn).toBeInViewport();

    const githubBtn = page.locator('#hero a:has-text("View on GitHub")');
    await expect(githubBtn).toBeInViewport();

    // Additional check: Verify hero content fits within viewport height
    // Get hero section bounding box
    const heroBBox = await heroSection.boundingBox();
    const ctaBtnBBox = await githubBtn.boundingBox();

    // Verify the bottom of the CTA buttons is above the fold
    if (ctaBtnBBox) {
      expect(ctaBtnBBox.y + ctaBtnBBox.height).toBeLessThan(768);
    }
  });

  test('Hero section has proper accessibility structure', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Verify the section has a proper role or aria attribute
    const ariaLabelledBy = await heroSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBeTruthy();

    // Verify the heading is properly associated
    const headingId = await page.locator(`#${ariaLabelledBy}`).getAttribute('id');
    expect(headingId).toBeTruthy();

    // Verify CTA buttons are accessible
    const getStartedBtn = page.locator('#hero a:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    // GitHub button should have aria-label for external link indication
    const githubBtn = page.locator('#hero a:has-text("View on GitHub")');
    const githubAriaLabel = await githubBtn.getAttribute('aria-label');
    expect(githubAriaLabel).toContain('GitHub');
  });
});

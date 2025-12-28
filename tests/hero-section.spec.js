// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Hero Section Display
 * Scenario: Verify the hero section displays product name, tagline, logo,
 * and primary value proposition prominently
 */

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Product name 'MirDB' is displayed prominently
   * Input: Load homepage and inspect hero section
   * Expected: Product name 'MirDB' is displayed prominently
   */
  test('TC1: Product name MirDB is displayed prominently', async ({ page }) => {
    // Verify the hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify the product name is displayed
    const productName = page.locator('#product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify it's prominently displayed (h1 tag)
    const h1Element = page.locator('h1.product-name');
    await expect(h1Element).toBeVisible();
    await expect(h1Element).toHaveText('MirDB');
  });

  /**
   * Test Case 2: Tagline communicates persistent key-value store with memcached compatibility
   * Input: Check hero section tagline
   * Expected: Tagline communicates persistent key-value store with memcached compatibility
   */
  test('TC2: Tagline communicates persistent key-value store with memcached compatibility', async ({ page }) => {
    // Verify the tagline element exists and is visible
    const tagline = page.locator('#tagline');
    await expect(tagline).toBeVisible();

    // Get the tagline text and verify it contains key information
    const taglineText = await tagline.textContent();

    // Verify the tagline mentions persistence and memcached compatibility
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  /**
   * Test Case 3: Logo image is displayed and has appropriate alt text
   * Input: Verify logo presence
   * Expected: Logo image is displayed and has appropriate alt text
   */
  test('TC3: Logo image is displayed with appropriate alt text', async ({ page }) => {
    // Verify the hero logo exists
    const heroLogo = page.locator('#hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify it's an image element
    const logoImg = page.locator('img#hero-logo');
    await expect(logoImg).toBeVisible();

    // Verify the alt text is appropriate (contains MirDB or Logo)
    const altText = await logoImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toMatch(/mirdb|logo/);

    // Verify the image source is set
    const src = await logoImg.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toContain('logo');
  });

  /**
   * Test Case 4: Click 'Get Started' CTA scrolls to quick-start section
   * Input: Click 'Get Started' CTA button
   * Expected: Scrolls to or navigates to quick-start section
   */
  test('TC4: Get Started CTA scrolls to quick-start section', async ({ page }) => {
    // Verify the Get Started button exists
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify the button has correct href
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#quick-start');

    // Click the button
    await getStartedBtn.click();

    // Wait for scroll and verify the quick-start section is in view
    await page.waitForTimeout(500); // Allow time for smooth scroll

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify the quick-start section is now in the viewport
    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });

  /**
   * Test Case 5: Click 'View on GitHub' CTA opens GitHub repository in new tab
   * Input: Click 'View on GitHub' CTA button
   * Expected: Opens GitHub repository in new tab
   */
  test('TC5: View on GitHub CTA opens GitHub repository in new tab', async ({ page, context }) => {
    // Verify the GitHub button exists
    const githubBtn = page.locator('#github-btn');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveText('View on GitHub');

    // Verify the button has correct href pointing to GitHub
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it opens in a new tab (target="_blank")
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has rel="noopener noreferrer" for security
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new page
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubBtn.click()
    ]);

    // Verify the new page URL contains github
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com');
  });
});

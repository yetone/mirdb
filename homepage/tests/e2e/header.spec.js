/**
 * Header and Navigation Tests
 * Owner: Scenario 1 - Header and Navigation
 *
 * Tests:
 * - Logo presence and loading
 * - Tagline display
 * - Navigation link functionality
 * - Anchor scrolling behavior
 */

const { test, expect } = require('@playwright/test');
const { SELECTORS, waitForPageLoad, checkImageLoaded } = require('./test-utils');

test.describe('Header and Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  // Test Case 1: Page loads with header containing logo element with valid src attribute
  test('page loads with header containing logo element with valid src attribute', async ({ page }) => {
    // Verify header exists
    const header = page.locator(SELECTORS.header);
    await expect(header).toBeVisible();

    // Verify logo exists with valid src
    const logo = page.locator(SELECTORS.headerLogo);
    await expect(logo).toBeVisible();

    const src = await logo.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toContain('logo');
  });

  // Test Case 2: Logo image exists with alt text and loads without error
  test('logo image exists with alt text and loads without error', async ({ page }) => {
    const logo = page.locator(SELECTORS.headerLogo);

    // Check alt text exists
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toContain('mirdb');

    // Check image loads successfully
    const loaded = await checkImageLoaded(page, SELECTORS.headerLogo);
    expect(loaded).toBe(true);
  });

  // Test Case 3: Tagline text contains 'Persistent Key-Value Store' and 'Memcached'
  test('tagline contains required keywords', async ({ page }) => {
    const tagline = page.locator(SELECTORS.headerTagline);
    await expect(tagline).toBeVisible();

    const text = await tagline.textContent();
    expect(text.toLowerCase()).toContain('persistent');
    expect(text.toLowerCase()).toContain('key-value');
    expect(text.toLowerCase()).toContain('memcached');
  });

  // Test Case 4: Click navigation link 'Features' - page scrolls to features section
  test('clicking Features navigation link scrolls to features section', async ({ page }) => {
    const featuresLink = page.locator(`${SELECTORS.headerNavLinks}[href="#features"]`);
    await expect(featuresLink).toBeVisible();

    // Click the link
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify URL hash is updated
    const url = page.url();
    expect(url).toContain('#features');

    // Verify features section is in viewport
    const featuresSection = page.locator(SELECTORS.features);
    await expect(featuresSection).toBeInViewport();
  });

  // Test Case 5: Click navigation link 'Architecture' - page scrolls to architecture section
  test('clicking Architecture navigation link scrolls to architecture section', async ({ page }) => {
    const archLink = page.locator(`${SELECTORS.headerNavLinks}[href="#architecture"]`);
    await expect(archLink).toBeVisible();

    // Click the link
    await archLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify URL hash is updated
    const url = page.url();
    expect(url).toContain('#architecture');

    // Verify architecture section is in viewport
    const archSection = page.locator(SELECTORS.architecture);
    await expect(archSection).toBeInViewport();
  });

  // Test Case 6: GitHub link points to correct URL
  test('GitHub navigation link points to correct URL', async ({ page }) => {
    const githubLink = page.locator(`${SELECTORS.headerNavLinks}[href*="github.com"]`);
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  // Additional test: Verify all navigation links are present
  test('all navigation links are present and visible', async ({ page }) => {
    const navLinks = page.locator(SELECTORS.headerNavLinks);
    const count = await navLinks.count();

    // Should have at least 4 links: Features, Architecture, Usage, GitHub
    expect(count).toBeGreaterThanOrEqual(4);

    // Check each expected link
    const expectedLinks = ['Features', 'Architecture', 'Usage', 'GitHub'];
    for (const linkText of expectedLinks) {
      const link = page.locator(`${SELECTORS.headerNavLinks}:has-text("${linkText}")`);
      await expect(link).toBeVisible();
    }
  });

  // Additional test: Header is sticky
  test('header remains visible when scrolling', async ({ page }) => {
    // Scroll down
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(200);

    // Verify header is still visible
    const header = page.locator(SELECTORS.header);
    await expect(header).toBeVisible();
    await expect(header).toBeInViewport();
  });
});

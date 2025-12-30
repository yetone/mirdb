const { test, expect, chromium } = require('@playwright/test');
const { playAudit } = require('playwright-lighthouse');

/**
 * User Story Validation - First Impression (US-1)
 *
 * Scenario: Validate acceptance criteria for US-1: New developer quickly understands MirDB's purpose
 *
 * Steps:
 * 1. Load homepage fresh - Clear cache and load homepage as first-time visitor
 * 2. Time to understanding - Measure time from page load to clear headline visibility
 * 3. Value proposition clarity - Verify persistence + memcached compatibility is immediately clear
 *
 * Acceptance Criteria (from PRD):
 * - Given I land on the homepage
 * - When the page loads
 * - Then I see a clear headline explaining MirDB's purpose within 2 seconds
 * - And I can identify the primary value proposition (persistence + memcached compatibility)
 */

test.describe('US-1: First Impression Validation', () => {
  /**
   * Test Case 1: Load page and measure FCP
   * Input: Load page and measure FCP
   * Expected: Headline visible within 2 seconds of navigation start
   * Type: Integration
   */
  test.describe('Test Case 1: Headline visibility within 2 seconds', () => {
    // Configure longer timeout for performance tests
    test.setTimeout(60000);

    // Only run Lighthouse tests in Chromium as it requires Chrome DevTools Protocol
    test.skip(({ browserName }) => browserName !== 'chromium', 'Lighthouse only works with Chromium');

    let browser;
    let page;
    const PORT = 9222;

    test.beforeAll(async () => {
      // Launch browser with remote debugging port for Lighthouse
      browser = await chromium.launch({
        args: [`--remote-debugging-port=${PORT}`],
      });
    });

    test.afterAll(async () => {
      if (browser) {
        await browser.close();
      }
    });

    test.beforeEach(async () => {
      const context = await browser.newContext();
      page = await context.newPage();
    });

    test.afterEach(async () => {
      if (page) {
        await page.close();
      }
    });

    test('FCP measurement: First Contentful Paint under 2 seconds', async () => {
      // Step 1: Clear cache and load homepage as first-time visitor (fresh page load simulates cleared cache)
      await page.goto('http://localhost:3000/');

      // Run Lighthouse audit to measure FCP
      const result = await playAudit({
        page: page,
        port: PORT,
        thresholds: {
          performance: 50, // Set a low threshold since we're testing FCP specifically
        },
        config: {
          extends: 'lighthouse:default',
          settings: {
            onlyCategories: ['performance'],
          },
        },
        reports: {
          formats: {
            html: false,
            json: false,
          },
        },
      });

      // Get FCP metric in milliseconds
      const fcpAudit = result.lhr.audits['first-contentful-paint'];
      const fcpMs = fcpAudit.numericValue;

      console.log(`First Contentful Paint: ${fcpMs}ms`);

      // Step 2: Time to understanding - FCP should be under 2000ms (2 seconds)
      expect(fcpMs).toBeLessThan(2000);
    });

    test('Headline visibility: Hero headline visible within 2 seconds of navigation', async () => {
      const startTime = Date.now();

      // Step 1: Clear cache and load homepage as first-time visitor
      await page.goto('http://localhost:3000/');

      // Step 2: Measure time from page load to clear headline visibility
      const heroHeadline = page.locator('.hero-headline');
      await heroHeadline.waitFor({ state: 'visible', timeout: 2000 });

      const headlineVisibleTime = Date.now() - startTime;
      console.log(`Headline visible after: ${headlineVisibleTime}ms`);

      // Verify headline is visible within 2 seconds per acceptance criteria
      expect(headlineVisibleTime).toBeLessThan(2000);

      // Verify headline contains MirDB's purpose
      await expect(heroHeadline).toContainText('MirDB');
      await expect(heroHeadline).toContainText('Persistent Key-Value Store');
      await expect(heroHeadline).toContainText('Memcached Protocol');
    });
  });

  /**
   * Test Case 2: Check above-the-fold content
   * Input: Check above-the-fold content
   * Expected: Persistence and memcached compatibility mentioned in visible area before scrolling
   * Type: E2E
   */
  test.describe('Test Case 2: Above-the-fold value proposition', () => {
    test('Value proposition clarity: Persistence and memcached compatibility visible without scrolling', async ({ page }) => {
      // Set a standard desktop viewport to test above-the-fold content
      await page.setViewportSize({ width: 1280, height: 720 });

      // Step 1: Load homepage fresh as first-time visitor
      await page.goto('/');

      // Step 3: Value proposition clarity - Verify persistence + memcached compatibility is immediately clear

      // Get the viewport height to determine above-the-fold area
      const viewportHeight = 720;

      // Check hero section is above the fold (visible without scrolling)
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();
      // Hero section should start at top (within viewport)
      expect(heroBox.y).toBeLessThan(viewportHeight);

      // Verify the headline mentions memcached compatibility
      const headline = page.locator('.hero-headline');
      await expect(headline).toBeVisible();
      const headlineText = await headline.textContent();
      expect(headlineText.toLowerCase()).toContain('memcached');

      // Verify the headline mentions persistent storage
      expect(headlineText.toLowerCase()).toContain('persistent');

      // Verify the subheadline reinforces the value proposition
      const subheadline = page.locator('.hero-subheadline');
      await expect(subheadline).toBeVisible();
      const subheadlineText = await subheadline.textContent();
      // Subheadline: "The familiarity of memcached with the reliability of persistent storage"
      expect(subheadlineText.toLowerCase()).toContain('memcached');
      expect(subheadlineText.toLowerCase()).toContain('persistent');

      // Verify both headline and subheadline are above the fold (visible without scrolling)
      const headlineBox = await headline.boundingBox();
      const subheadlineBox = await subheadline.boundingBox();
      expect(headlineBox).not.toBeNull();
      expect(subheadlineBox).not.toBeNull();

      // The bottom of these elements should be within the viewport
      expect(headlineBox.y + headlineBox.height).toBeLessThan(viewportHeight);
      expect(subheadlineBox.y + subheadlineBox.height).toBeLessThan(viewportHeight);

      console.log('Value proposition clarity verified:');
      console.log(`- Headline: "${headlineText}"`);
      console.log(`- Subheadline: "${subheadlineText}"`);
      console.log('- Both persistence and memcached compatibility are mentioned above the fold');
    });

    test('Mobile viewport: Value proposition visible above the fold on mobile', async ({ page }) => {
      // Test on mobile viewport (iPhone-like dimensions)
      await page.setViewportSize({ width: 375, height: 667 });

      // Load homepage as first-time visitor
      await page.goto('/');

      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify headline contains both key value propositions
      const headline = page.locator('.hero-headline');
      await expect(headline).toBeVisible();
      const headlineText = await headline.textContent();
      expect(headlineText.toLowerCase()).toContain('persistent');
      expect(headlineText.toLowerCase()).toContain('memcached');

      // Verify headline is above the fold on mobile
      const headlineBox = await headline.boundingBox();
      expect(headlineBox).not.toBeNull();
      // On mobile, the headline should still be visible in the initial viewport
      expect(headlineBox.y).toBeLessThan(667);

      console.log('Mobile value proposition verified:');
      console.log(`- Headline visible at Y: ${headlineBox.y}px`);
      console.log(`- Mobile viewport height: 667px`);
    });

    test('Content completeness: All key messaging present in above-the-fold area', async ({ page }) => {
      // Standard desktop viewport
      await page.setViewportSize({ width: 1280, height: 720 });

      await page.goto('/');

      // Verify the complete value proposition chain is present:
      // 1. Project name (MirDB)
      const pageText = await page.textContent('.hero');
      expect(pageText).toContain('MirDB');

      // 2. Primary value prop: Persistent key-value store
      expect(pageText.toLowerCase()).toContain('persistent');
      expect(pageText.toLowerCase()).toContain('key-value');

      // 3. Protocol compatibility: Memcached
      expect(pageText.toLowerCase()).toContain('memcached');

      // 4. Clear call-to-action buttons are visible
      const getStartedButton = page.locator('.hero-cta').getByText('Get Started');
      const githubButton = page.locator('.hero-cta').getByText('View on GitHub');
      await expect(getStartedButton).toBeVisible();
      await expect(githubButton).toBeVisible();

      console.log('Above-the-fold content verification complete');
      console.log('- MirDB branding: Present');
      console.log('- Persistence messaging: Present');
      console.log('- Memcached compatibility: Present');
      console.log('- CTA buttons: Visible');
    });
  });
});

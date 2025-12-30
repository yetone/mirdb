const { test, expect, chromium } = require('@playwright/test');

/**
 * User Story Validation - First Impression (US-1)
 *
 * Scenario: Validate acceptance criteria for US-1: New developer quickly understands MirDB's purpose
 *
 * Steps:
 * 1. Load homepage fresh - Clear cache and load homepage as first-time visitor (simulating new developer discovery)
 * 2. Time to understanding - Measure time from page load to clear headline visibility (should be within 2 seconds per acceptance criteria)
 * 3. Value proposition clarity - Verify persistence + memcached compatibility is immediately clear (primary value prop should be obvious)
 *
 * Acceptance Criteria from PRD:
 * - Given I land on the homepage
 * - When the page loads
 * - Then I see a clear headline explaining MirDB's purpose within 2 seconds
 * - And I can identify the primary value proposition (persistence + memcached compatibility)
 */

test.describe('US-1: First Impression - User Story Validation', () => {
  // Test Case 1: Integration test for FCP and headline visibility
  test.describe('Test Case 1: Load page and measure FCP', () => {
    test('Headline visible within 2 seconds of navigation start', async ({ page }) => {
      // Step 1: Load homepage fresh - simulating new developer discovery
      // Clear any cached state by using a fresh page context
      const startTime = Date.now();

      // Navigate to homepage as a first-time visitor
      await page.goto('/');

      // Step 2: Time to understanding - Measure time from page load to clear headline visibility
      // Wait for the hero headline to be visible
      const heroHeadline = page.locator('.hero-headline, h1');
      await heroHeadline.first().waitFor({ state: 'visible', timeout: 2000 });

      const headlineVisibleTime = Date.now() - startTime;
      console.log(`Headline visible after: ${headlineVisibleTime}ms`);

      // Verify headline is visible within 2 seconds per acceptance criteria
      expect(headlineVisibleTime).toBeLessThan(2000);

      // Verify headline contains MirDB and explains purpose
      const headlineText = await heroHeadline.first().textContent();
      expect(headlineText).toContain('MirDB');
      expect(headlineText.toLowerCase()).toContain('persistent');
      expect(headlineText.toLowerCase()).toContain('key-value');

      // Verify headline clearly explains MirDB's purpose
      // Should contain: "MirDB: A Persistent Key-Value Store with Memcached Protocol"
      expect(headlineText.toLowerCase()).toMatch(/mirdb.*persistent.*key-value/i);
    });

    test('Page content is immediately meaningful to developers', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/');

      // Wait for meaningful content to appear
      const heroSection = page.locator('.hero');
      await heroSection.waitFor({ state: 'visible', timeout: 2000 });

      const contentVisibleTime = Date.now() - startTime;
      console.log(`Hero section visible after: ${contentVisibleTime}ms`);

      // Verify content loads within 2 seconds
      expect(contentVisibleTime).toBeLessThan(2000);

      // Verify subheadline provides additional context
      const subheadline = page.locator('.hero-subheadline');
      await expect(subheadline).toBeVisible();

      const subheadlineText = await subheadline.textContent();
      // Should mention familiarity of memcached and reliability of persistent storage
      expect(subheadlineText.toLowerCase()).toContain('memcached');
      expect(subheadlineText.toLowerCase()).toContain('persistent');
    });
  });

  // Test Case 2: E2E test for above-the-fold content
  test.describe('Test Case 2: Check above-the-fold content', () => {
    test('Persistence and memcached compatibility mentioned in visible area before scrolling', async ({ page }) => {
      // Set viewport to standard desktop size
      await page.setViewportSize({ width: 1280, height: 720 });

      await page.goto('/');

      // Step 3: Value proposition clarity - Verify persistence + memcached compatibility is immediately clear

      // Get the visible viewport content without scrolling
      const viewportHeight = 720;

      // Check hero section is above the fold
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Get hero section position
      const heroBoundingBox = await heroSection.boundingBox();
      expect(heroBoundingBox).not.toBeNull();

      // Verify hero section starts at the top (above the fold)
      expect(heroBoundingBox.y).toBeLessThan(viewportHeight);

      // Get all text visible above the fold
      const aboveTheFoldText = await page.evaluate((viewportHeight) => {
        const heroSection = document.querySelector('.hero');
        const valueSection = document.querySelector('.value-prop, #value-proposition');

        let text = '';

        // Get text from hero section
        if (heroSection) {
          text += heroSection.innerText + ' ';
        }

        // Check if value proposition section is visible above the fold
        if (valueSection) {
          const rect = valueSection.getBoundingClientRect();
          if (rect.top < viewportHeight) {
            text += valueSection.innerText;
          }
        }

        return text;
      }, viewportHeight);

      // Verify persistence is mentioned in above-the-fold content
      expect(aboveTheFoldText.toLowerCase()).toContain('persist');

      // Verify memcached compatibility is mentioned in above-the-fold content
      expect(aboveTheFoldText.toLowerCase()).toContain('memcached');

      // Verify it's clear this is a key-value store
      expect(aboveTheFoldText.toLowerCase()).toContain('key-value');
    });

    test('Hero headline is visible without scrolling on mobile viewport', async ({ page }) => {
      // Test mobile viewport
      await page.setViewportSize({ width: 375, height: 667 }); // iPhone SE dimensions

      await page.goto('/');

      // Verify headline is visible on mobile without scrolling
      const headline = page.locator('.hero-headline, h1');
      await expect(headline.first()).toBeVisible();

      // Get headline position to ensure it's above the fold
      const headlineBoundingBox = await headline.first().boundingBox();
      expect(headlineBoundingBox).not.toBeNull();
      expect(headlineBoundingBox.y + headlineBoundingBox.height).toBeLessThan(667);

      // Verify headline contains key messaging
      const headlineText = await headline.first().textContent();
      expect(headlineText).toContain('MirDB');
    });

    test('Value proposition visible above the fold on standard desktop', async ({ page }) => {
      // Standard desktop viewport
      await page.setViewportSize({ width: 1920, height: 1080 });

      await page.goto('/');

      // Hero section should be visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check for value proposition elements in the hero section
      const heroText = await heroSection.textContent();

      // Primary value props should be visible in hero area
      expect(heroText.toLowerCase()).toContain('persistent');
      expect(heroText.toLowerCase()).toContain('memcached');

      // Verify CTA buttons are visible above the fold
      const getStartedBtn = page.locator('a.btn-primary, .btn-primary').filter({ hasText: /get started/i });
      await expect(getStartedBtn.first()).toBeVisible();

      const githubBtn = page.locator('a.btn-secondary, .btn-secondary').filter({ hasText: /github/i });
      await expect(githubBtn.first()).toBeVisible();
    });

    test('New developer can identify primary value proposition immediately', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      await page.goto('/');

      // The primary value proposition: persistence + memcached compatibility
      // Should be identifiable in the first screen view

      // Check headline
      const headline = page.locator('.hero-headline, h1').first();
      await expect(headline).toBeVisible();
      const headlineText = await headline.textContent();

      // Headline should mention both Memcached and Persistent
      expect(headlineText.toLowerCase()).toContain('memcached');
      expect(headlineText.toLowerCase()).toContain('persistent');

      // Check subheadline reinforces the message
      const subheadline = page.locator('.hero-subheadline');
      await expect(subheadline).toBeVisible();
      const subheadlineText = await subheadline.textContent();

      // Subheadline should contrast memcached familiarity with persistent reliability
      expect(subheadlineText.toLowerCase()).toMatch(/memcached.*persistent|persistent.*memcached/);
    });
  });
});

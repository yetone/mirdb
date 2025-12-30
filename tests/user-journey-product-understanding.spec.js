// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

/**
 * User Journey - Product Understanding
 * Scenario: Verify a backend developer can quickly understand MirDB's value proposition
 *
 * This test suite validates the complete user journey for a developer landing on the
 * MirDB homepage to understand what the product is and how to get started.
 */
test.describe('User Journey - Product Understanding', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Time to understand product purpose from hero
   * Type: manual (validated via content verification)
   * Input: Time to understand product purpose from hero
   * Expected: Product name, tagline, and description are scannable within 5 seconds
   *
   * Note: This test validates that the essential information is present and visible
   * immediately upon page load. The "5 seconds" constraint is addressed by ensuring
   * all elements are visible without scrolling and contain concise, scannable content.
   */
  test('TC1: Product name, tagline, and description are immediately scannable', async ({ page }) => {
    // Step 1: Land on homepage - page is already loaded in beforeEach

    // Step 2: Read hero content - verify product purpose is understandable within 5 seconds
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify product name is prominently displayed
    const productName = page.locator('.hero-title');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify tagline is present and descriptive
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached Protocol');

    // Verify description exists and is scannable (not too long, not too short)
    const description = page.locator('.hero-description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();

    // Description should be 2-3 sentences (roughly 100-400 characters)
    expect(descriptionText.length).toBeGreaterThan(100);
    expect(descriptionText.length).toBeLessThan(500);

    // Description should mention key concepts that help understand the product
    expect(descriptionText.toLowerCase()).toContain('key-value');
    expect(descriptionText.toLowerCase()).toContain('memcached');

    // Verify hero content is visible without scrolling (above the fold)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).toBeTruthy();
    expect(heroBox.y).toBe(0); // Hero starts at top of page
  });

  /**
   * Test Case 2: Locate technical features section
   * Type: e2e
   * Input: Locate technical features section
   * Expected: Features section is visible with minimal scrolling and clearly organized
   */
  test('TC2: Features section is visible with minimal scrolling and clearly organized', async ({ page }) => {
    // Step 3: Scan features - quickly identify key technical capabilities

    // Verify features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features section has a clear title
    const sectionTitle = featuresSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Features');

    // Verify there are at least 3 feature cards (memcached, persistence, LSM)
    const featureCards = featuresSection.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify features are clearly organized with titles and descriptions
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const title = card.locator('.feature-title');
      const desc = card.locator('.feature-description');

      await expect(title).toBeVisible();
      await expect(desc).toBeVisible();

      // Each title should have meaningful text
      const titleText = await title.textContent();
      expect(titleText.length).toBeGreaterThan(5);

      // Each description should be detailed enough to understand the feature
      const descText = await desc.textContent();
      expect(descText.length).toBeGreaterThan(50);
    }

    // Verify key technical capabilities are represented
    const featureTexts = await featureCards.allTextContents();
    const allFeaturesText = featureTexts.join(' ').toLowerCase();

    expect(allFeaturesText).toContain('memcached');
    expect(allFeaturesText).toContain('persist');
    expect(allFeaturesText).toContain('lsm');

    // Verify features section is reachable with minimal scrolling
    // Click on features nav link and verify smooth navigation
    const featuresLink = page.locator('a[href="#features"]');
    if (await featuresLink.count() > 0) {
      await featuresLink.first().click();
      await page.waitForTimeout(500); // Wait for scroll animation

      // Verify features section is now in view
      await expect(featuresSection).toBeInViewport();
    }
  });

  /**
   * Test Case 3: Find code examples for evaluation
   * Type: e2e
   * Input: Find code examples for evaluation
   * Expected: Quick start code examples are accessible within 2-3 scrolls from hero
   */
  test('TC3: Quick start code examples are accessible within 2-3 scrolls from hero', async ({ page }) => {
    // Step 4: Find getting started info - locate code examples

    // Verify quick start section exists
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify section has a descriptive title
    const sectionTitle = quickStartSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Quick Start');

    // Verify there are code examples
    const codeBlocks = quickStartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(2); // At least startup and usage examples

    // Verify code examples contain practical, evaluable content
    const allCodeText = await codeBlocks.allTextContents();
    const combinedCodeText = allCodeText.join(' ');

    // Server startup command
    expect(combinedCodeText).toContain('mirdb');

    // Basic operations (SET/GET)
    expect(combinedCodeText.toLowerCase()).toContain('set');
    expect(combinedCodeText.toLowerCase()).toContain('get');

    // Expected responses that help user evaluate
    expect(combinedCodeText).toContain('STORED');
    expect(combinedCodeText).toContain('VALUE');

    // Verify code has syntax highlighting for readability
    const highlightedElements = quickStartSection.locator('.code-block [class^="code-"]');
    const highlightCount = await highlightedElements.count();
    expect(highlightCount).toBeGreaterThan(0);

    // Verify quick start is accessible from navigation (within 2-3 scrolls metaphorically)
    const quickStartLink = page.locator('a[href="#quick-start"]');
    if (await quickStartLink.count() > 0) {
      await page.goto(indexPath); // Reset to top
      await quickStartLink.first().click();
      await page.waitForTimeout(500);

      await expect(quickStartSection).toBeInViewport();
    }

    // Calculate relative position - quick start should be within reasonable distance
    const heroSection = page.locator('.hero');
    const heroBox = await heroSection.boundingBox();
    const quickStartBox = await quickStartSection.boundingBox();

    if (heroBox && quickStartBox) {
      // Quick start should be accessible (not more than ~3 viewport heights from hero)
      const viewportHeight = await page.evaluate(() => window.innerHeight);
      const distanceFromHero = quickStartBox.y - (heroBox.y + heroBox.height);

      // Within 3 viewport heights (2-3 scrolls)
      expect(distanceFromHero).toBeLessThan(viewportHeight * 3);
    }
  });

  /**
   * Test Case 4: Complete user journey from landing to GitHub
   * Type: e2e
   * Input: Complete user journey from landing to GitHub
   * Expected: User can navigate from homepage to GitHub repository in 2 clicks or less
   */
  test('TC4: User can navigate from homepage to GitHub in 2 clicks or less', async ({ page }) => {
    // User journey: Landing → GitHub (should be direct, 1 click)

    // Verify GitHub link is immediately visible in hero section (1 click)
    const heroSection = page.locator('.hero');
    const heroGithubLink = heroSection.locator('a[href*="github.com"]');

    // Primary path: GitHub link directly in hero
    const heroLinkCount = await heroGithubLink.count();
    expect(heroLinkCount).toBeGreaterThanOrEqual(1);

    // Verify the GitHub link is visible and clickable
    const primaryGithubLink = heroGithubLink.first();
    await expect(primaryGithubLink).toBeVisible();

    // Verify link text clearly indicates GitHub
    const linkText = await primaryGithubLink.textContent();
    expect(linkText.toLowerCase()).toMatch(/github|view on|source/);

    // Verify link points to correct GitHub URL
    const href = await primaryGithubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify proper security attributes for external link
    const rel = await primaryGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    const target = await primaryGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify there's also a GitHub link in footer (alternate path, still 1 click)
    const footerGithubLink = page.locator('footer a[href*="github.com"]');
    const footerLinkCount = await footerGithubLink.count();
    expect(footerLinkCount).toBeGreaterThanOrEqual(1);

    // Total navigation: Landing page → Click GitHub link = 1 click (less than 2 required)
    // Both paths (hero and footer) provide single-click access to GitHub
  });

  /**
   * Integration Test: Complete User Journey Flow
   * Validates the entire user journey as described in the scenario steps
   */
  test('Integration: Complete user journey from landing to product understanding', async ({ page }) => {
    // Step 1: Land on homepage - simulate developer landing from search
    await expect(page).toHaveTitle(/MirDB/);

    // Step 2: Read hero content - understand product purpose within 5 seconds
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // User immediately sees: product name, tagline, description
    await expect(page.locator('.hero-title')).toBeVisible();
    await expect(page.locator('.hero-tagline')).toBeVisible();
    await expect(page.locator('.hero-description')).toBeVisible();

    // Step 3: Scan features - quickly identify key technical capabilities
    // User scrolls or clicks to features
    await page.locator('a[href="#features"]').first().click();
    await page.waitForTimeout(300);

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // User identifies: memcached compatibility, persistence, LSM architecture
    await expect(features.locator('[data-feature="memcached"]')).toBeVisible();
    await expect(features.locator('[data-feature="persistence"]')).toBeVisible();
    await expect(features.locator('[data-feature="lsm"]')).toBeVisible();

    // Step 4: Find getting started info - locate code examples
    await page.locator('a[href="#quick-start"]').first().click();
    await page.waitForTimeout(300);

    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();

    // User evaluates ease of use via code examples
    const codeBlocks = quickStart.locator('.code-block');
    expect(await codeBlocks.count()).toBeGreaterThanOrEqual(2);

    // Final step: User can easily navigate to GitHub to explore further
    const githubLink = hero.locator('a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();

    // Journey complete: User understood product and can take action
  });
});

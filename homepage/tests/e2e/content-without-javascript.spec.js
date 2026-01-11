// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Content Without JavaScript E2E Tests for MirDB Homepage
 *
 * Scenario: Verify core content is accessible without JavaScript enabled
 *
 * These tests verify that the homepage works properly when JavaScript is disabled,
 * which is a key technical constraint from the PRD.
 */

// Use a test context with JavaScript disabled
test.use({ javaScriptEnabled: false });

test.describe('Content Without JavaScript', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the page with JavaScript disabled (configured via test.use above)
    await page.goto('/');
  });

  /**
   * Test Case 1: Hero section content visible without JavaScript
   * Input: Load page with JavaScript disabled
   * Expected: Hero section content visible without JavaScript
   */
  test('TC1: Hero section content should be visible without JavaScript', async ({ page }) => {
    // Verify hero section exists and is visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify hero subtitle is visible
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();
    await expect(heroSubtitle).toContainText('Persistent Key-Value Store');

    // Verify hero description is visible
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();
    await expect(heroDescription).toContainText('high-performance');

    // Verify CTA buttons are visible
    const primaryCta = page.locator('.hero-cta .btn-primary');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toHaveText('Get Started');

    const secondaryCta = page.locator('.hero-cta .btn-secondary');
    await expect(secondaryCta).toBeVisible();
    await expect(secondaryCta).toContainText('GitHub');
  });

  /**
   * Test Case 2: All feature descriptions visible without JavaScript
   * Input: Check features section without JS
   * Expected: All feature descriptions visible without JavaScript
   */
  test('TC2: All feature descriptions should be visible without JavaScript', async ({ page }) => {
    // Verify features section exists and is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify section title
    const sectionTitle = page.locator('#features-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Key Features');

    // Verify all 6 feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Verify each feature card has visible title and description
    const expectedFeatures = [
      { title: 'Memcached Compatible', descContains: 'Drop-in replacement' },
      { title: 'Persistent Storage', descContains: 'persists data to disk' },
      { title: 'High Performance', descContains: 'Rust' },
      { title: 'LSM Tree Architecture', descContains: 'Log-Structured Merge-tree' },
      { title: 'Skip List Memtable', descContains: 'skip list data structure' },
      { title: 'Automatic Compaction', descContains: 'compaction' },
    ];

    for (let i = 0; i < expectedFeatures.length; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Verify feature title is visible
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();
      await expect(title).toHaveText(expectedFeatures[i].title);

      // Verify feature description is visible
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
      await expect(description).toContainText(expectedFeatures[i].descContains);
    }

    // Verify feature icons are visible (SVG icons)
    const featureIcons = page.locator('.feature-icon');
    await expect(featureIcons).toHaveCount(6);
    for (let i = 0; i < 6; i++) {
      await expect(featureIcons.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 3: All navigation links functional without JavaScript
   * Input: Test links without JavaScript
   * Expected: All navigation links functional without JavaScript
   */
  test('TC3: All navigation links should be functional without JavaScript', async ({ page }) => {
    // Verify navigation bar is visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Verify logo link is functional
    const logoLink = page.locator('.logo');
    await expect(logoLink).toBeVisible();
    await expect(logoLink).toHaveAttribute('href', '#');

    // Verify nav links are visible and have proper hrefs
    const navLinks = page.locator('.nav-links a');
    await expect(navLinks).toHaveCount(3);

    // Features link
    const featuresLink = navLinks.nth(0);
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');
    await expect(featuresLink).toHaveText('Features');

    // Quick Start link
    const quickStartLink = navLinks.nth(1);
    await expect(quickStartLink).toBeVisible();
    await expect(quickStartLink).toHaveAttribute('href', '#quick-start');
    await expect(quickStartLink).toHaveText('Quick Start');

    // GitHub link
    const githubLink = navLinks.nth(2);
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveText('GitHub');

    // Verify footer links are visible and functional
    const footerLinks = page.locator('.footer-links a');
    await expect(footerLinks).toHaveCount(3);

    // GitHub footer link
    await expect(footerLinks.nth(0)).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Documentation footer link
    await expect(footerLinks.nth(1)).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/README.md');

    // Issues footer link
    await expect(footerLinks.nth(2)).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');

    // Verify CTA button links work (Get Started should go to #quick-start)
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');

    // Verify GitHub button in hero
    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Test that clicking an anchor link navigates to the section (without JS)
    await featuresLink.click();
    // The URL should include the hash
    await expect(page).toHaveURL(/#features/);
  });

  /**
   * Test Case 4: Code examples displayed without requiring JavaScript
   * Input: Verify code examples visible without JS
   * Expected: Code examples displayed without requiring JavaScript
   */
  test('TC4: Code examples should be displayed without requiring JavaScript', async ({ page }) => {
    // Verify quick-start section is visible
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify section title
    const sectionTitle = page.locator('#quickstart-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Quick Start');

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-block');
    await expect(codeBlocks).toHaveCount(2);

    // First code block - installation commands
    const installCodeBlock = codeBlocks.nth(0);
    await expect(installCodeBlock).toBeVisible();

    // Verify the code content is visible
    const installCode = installCodeBlock.locator('pre code');
    await expect(installCode).toBeVisible();

    // Verify installation commands are present
    const installCodeText = await installCode.textContent();
    expect(installCodeText).toContain('git clone');
    expect(installCodeText).toContain('cargo build --release');
    expect(installCodeText).toContain('./target/release/mirdb-server');

    // Second code block - usage example
    const usageCodeBlock = codeBlocks.nth(1);
    await expect(usageCodeBlock).toBeVisible();

    const usageCode = usageCodeBlock.locator('pre code');
    await expect(usageCode).toBeVisible();

    // Verify usage example commands are present
    const usageCodeText = await usageCode.textContent();
    expect(usageCodeText).toContain('telnet localhost 12333');
    expect(usageCodeText).toContain('set mykey');
    expect(usageCodeText).toContain('get mykey');
    expect(usageCodeText).toContain('STORED');
    expect(usageCodeText).toContain('VALUE');

    // Verify code headers show language
    const codeHeaders = page.locator('.code-header');
    await expect(codeHeaders.nth(0)).toBeVisible();
    await expect(codeHeaders.nth(1)).toBeVisible();

    // Verify language labels
    const langLabels = page.locator('.code-lang');
    await expect(langLabels.nth(0)).toHaveText('bash');
    await expect(langLabels.nth(1)).toHaveText('terminal');
  });

  /**
   * Additional test: Verify page structure is complete without JavaScript
   */
  test('Page structure should be complete without JavaScript', async ({ page }) => {
    // Verify all major sections are present
    await expect(page.locator('nav.navbar')).toBeVisible();
    await expect(page.locator('header.hero')).toBeVisible();
    await expect(page.locator('main#main-content')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Verify semantic HTML structure
    await expect(page.locator('h1')).toHaveCount(1);

    // Should have multiple h2 for sections
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(2);

    // Verify the page title
    await expect(page).toHaveTitle(/MirDB/);
  });

  /**
   * Additional test: Verify images have alt text and are visible without JS
   */
  test('Images should be visible without JavaScript', async ({ page }) => {
    // Verify logo image is present with alt text
    const logoImg = page.locator('.logo-img');
    await expect(logoImg).toBeVisible();
    await expect(logoImg).toHaveAttribute('alt', 'MirDB Logo');
  });
});

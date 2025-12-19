// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

// Mobile viewport sizes to test
const VIEWPORT_320 = { width: 320, height: 568 };  // Small mobile (iPhone SE)
const VIEWPORT_375 = { width: 375, height: 667 };  // Standard mobile (iPhone)
const VIEWPORT_768 = { width: 768, height: 1024 }; // Tablet (iPad)

test.describe('Mobile Responsiveness', () => {

  test.describe('320px Viewport Width', () => {
    test.use({ viewport: VIEWPORT_320 });

    test('Test Case 1: All content is readable without horizontal scrolling at 320px', async ({ page }) => {
      await page.goto(BASE_URL);

      // Check that there's no horizontal overflow on the body
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Body scroll width should not exceed viewport width (no horizontal scrolling)
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify key sections are visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify product name is visible and readable
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();

      // Check that product name fits within viewport
      const productNameBox = await productName.boundingBox();
      expect(productNameBox).not.toBeNull();
      expect(productNameBox.x).toBeGreaterThanOrEqual(0);
      expect(productNameBox.x + productNameBox.width).toBeLessThanOrEqual(viewportWidth);

      // Verify tagline is visible
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Verify value proposition is visible
      const valueProposition = page.locator('[data-testid="value-proposition"]');
      await expect(valueProposition).toBeVisible();

      // Verify CTA buttons are visible
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();

      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeVisible();
    });
  });

  test.describe('375px Viewport Width (iPhone)', () => {
    test.use({ viewport: VIEWPORT_375 });

    test('Test Case 2: Layout adapts properly for standard mobile size at 375px', async ({ page }) => {
      await page.goto(BASE_URL);

      // Verify no horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify hero section adapts properly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify feature cards are visible - they should stack vertically on mobile
      const featureCards = page.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBe(4);

      // Check that first feature card is visible
      const firstFeature = page.locator('[data-testid="feature-memcached"]');
      await expect(firstFeature).toBeVisible();

      // Verify all key sections are accessible by scrolling
      const sections = [
        '[data-testid="features-section"]',
        '[data-testid="architecture-section"]',
        '[data-testid="quickstart-section"]',
        '[data-testid="protocol-section"]',
        '[data-testid="configuration-section"]',
        '[data-testid="footer-section"]'
      ];

      for (const section of sections) {
        const element = page.locator(section);
        await element.scrollIntoViewIfNeeded();
        await expect(element).toBeVisible();
      }
    });
  });

  test.describe('768px Viewport Width (Tablet)', () => {
    test.use({ viewport: VIEWPORT_768 });

    test('Test Case 3: Layout adapts for tablet-sized screens at 768px', async ({ page }) => {
      await page.goto(BASE_URL);

      // Verify no horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify hero section
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // On tablet (768px md breakpoint), feature cards should be in 2-column grid
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards are visible
      const featureCards = page.locator('.feature-card');
      expect(await featureCards.count()).toBe(4);

      // Verify architecture section with its diagram
      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();

      const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(architectureDiagram).toBeVisible();

      // Verify quick start section
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await quickstartSection.scrollIntoViewIfNeeded();
      await expect(quickstartSection).toBeVisible();

      // Verify protocol section tables
      const protocolSection = page.locator('[data-testid="protocol-section"]');
      await protocolSection.scrollIntoViewIfNeeded();
      await expect(protocolSection).toBeVisible();

      const commandsTable = page.locator('[data-testid="commands-table"]');
      await expect(commandsTable).toBeVisible();
    });
  });

  test.describe('Mobile Navigation', () => {
    test.use({ viewport: VIEWPORT_320 });

    test('Test Case 4: Navigation is accessible via mobile-friendly controls', async ({ page }) => {
      await page.goto(BASE_URL);

      // Verify CTA buttons are accessible on mobile
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();

      // Verify buttons are not cut off
      const getStartedBox = await getStartedBtn.boundingBox();
      expect(getStartedBox).not.toBeNull();
      expect(getStartedBox.x).toBeGreaterThanOrEqual(0);

      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeVisible();

      // Check that internal navigation links work (Get Started -> Quick Start section)
      await getStartedBtn.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500);

      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeInViewport();

      // Verify footer navigation links
      const footerSection = page.locator('[data-testid="footer-section"]');
      await footerSection.scrollIntoViewIfNeeded();
      await expect(footerSection).toBeVisible();

      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();

      const footerDocsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(footerDocsLink).toBeVisible();
    });
  });

  test.describe('Touch Interactions', () => {
    test.use({ viewport: VIEWPORT_375 });

    test('Test Case 5: All interactive elements are touch-friendly (minimum 44px tap targets)', async ({ page }) => {
      await page.goto(BASE_URL);

      // Minimum recommended touch target size per WCAG guidelines
      const MIN_TAP_TARGET = 44;

      // Test CTA buttons in hero section
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();
      const getStartedBox = await getStartedBtn.boundingBox();
      expect(getStartedBox).not.toBeNull();
      expect(getStartedBox.height).toBeGreaterThanOrEqual(MIN_TAP_TARGET);
      expect(getStartedBox.width).toBeGreaterThanOrEqual(MIN_TAP_TARGET);

      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeVisible();
      const githubBox = await githubBtn.boundingBox();
      expect(githubBox).not.toBeNull();
      expect(githubBox.height).toBeGreaterThanOrEqual(MIN_TAP_TARGET);
      expect(githubBox.width).toBeGreaterThanOrEqual(MIN_TAP_TARGET);

      // Test copy buttons in quick start section
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await quickstartSection.scrollIntoViewIfNeeded();

      const copyButtonInstall = page.locator('[data-testid="copy-button-install"]');
      await expect(copyButtonInstall).toBeVisible();
      const copyInstallBox = await copyButtonInstall.boundingBox();
      expect(copyInstallBox).not.toBeNull();
      // Copy buttons should have at least 44px in either dimension for touch
      expect(Math.max(copyInstallBox.height, copyInstallBox.width)).toBeGreaterThanOrEqual(MIN_TAP_TARGET);

      // Test feature card icons (should be touch-friendly)
      const featuresSection = page.locator('[data-testid="features-section"]');
      await featuresSection.scrollIntoViewIfNeeded();

      const featureIcon = page.locator('[data-testid="feature-memcached-icon"]');
      await expect(featureIcon).toBeVisible();
      const iconBox = await featureIcon.boundingBox();
      expect(iconBox).not.toBeNull();
      expect(iconBox.height).toBeGreaterThanOrEqual(MIN_TAP_TARGET);
      expect(iconBox.width).toBeGreaterThanOrEqual(MIN_TAP_TARGET);

      // Test footer links
      const footerSection = page.locator('[data-testid="footer-section"]');
      await footerSection.scrollIntoViewIfNeeded();

      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();
      const footerGithubBox = await footerGithubLink.boundingBox();
      expect(footerGithubBox).not.toBeNull();
      expect(footerGithubBox.height).toBeGreaterThanOrEqual(MIN_TAP_TARGET);
    });
  });

  test.describe('Content Overflow Checks', () => {
    test.use({ viewport: VIEWPORT_320 });

    test('Code blocks handle overflow properly on small screens', async ({ page }) => {
      await page.goto(BASE_URL);

      // Navigate to quick start section with code blocks
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await quickstartSection.scrollIntoViewIfNeeded();

      // Check installation code block
      const installationCode = page.locator('[data-testid="installation-code"]');
      await expect(installationCode).toBeVisible();

      // Code blocks should have overflow-x-auto to allow horizontal scrolling within the block
      // but should not cause page-level horizontal scroll
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Check config example code block
      const configExample = page.locator('[data-testid="config-example"]');
      await expect(configExample).toBeVisible();

      // Check client connection example
      const clientConnection = page.locator('[data-testid="client-connection-example"]');
      await expect(clientConnection).toBeVisible();
    });

    test('Tables are accessible on small screens', async ({ page }) => {
      await page.goto(BASE_URL);

      // Navigate to protocol section with tables
      const protocolSection = page.locator('[data-testid="protocol-section"]');
      await protocolSection.scrollIntoViewIfNeeded();

      // Tables should be visible and accessible
      const commandsTable = page.locator('[data-testid="commands-table"]');
      await expect(commandsTable).toBeVisible();

      const responseCodesTable = page.locator('[data-testid="response-codes"]');
      await expect(responseCodesTable).toBeVisible();

      // Navigate to configuration section
      const configSection = page.locator('[data-testid="configuration-section"]');
      await configSection.scrollIntoViewIfNeeded();

      const networkConfigTable = page.locator('[data-testid="config-table-network"]');
      await expect(networkConfigTable).toBeVisible();

      // Verify no horizontal page overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);
    });
  });
});

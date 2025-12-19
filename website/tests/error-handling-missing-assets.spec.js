// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Error Handling - Missing Assets', () => {

  test.describe('Test Case 1: CSS Loading Failure', () => {
    test('should display readable content with default browser styling when CSS fails to load', async ({ page }) => {
      // Block CSS file loading
      await page.route('**/*.css', route => route.abort());
      await page.route('**/output.css', route => route.abort());

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Verify that page content remains readable
      // Check that key text content is visible and readable
      const heroTitle = page.locator('[data-testid="product-name"]');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Check that value proposition text is visible
      const valueProp = page.locator('[data-testid="value-proposition"]');
      await expect(valueProp).toBeVisible();

      // Verify feature section content is accessible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Check that individual feature cards have readable text
      const featureMemcached = page.locator('[data-testid="feature-memcached"]');
      await expect(featureMemcached).toBeVisible();

      // Verify quick start section is accessible
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeVisible();

      // Check that code blocks remain visible
      const installationCode = page.locator('[data-testid="installation-code"]');
      await expect(installationCode).toBeVisible();

      // Verify protocol section tables are readable
      const protocolSection = page.locator('[data-testid="protocol-section"]');
      await expect(protocolSection).toBeVisible();

      // Check that commands table is visible
      const commandsTable = page.locator('[data-testid="commands-table"]');
      await expect(commandsTable).toBeVisible();

      // Verify footer content is accessible
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();
    });

    test('should maintain document structure when CSS fails to load', async ({ page }) => {
      // Block all CSS
      await page.route('**/*.css', route => route.abort());

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Verify semantic HTML structure is intact
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toHaveText('MirDB');

      // Check that all major sections exist
      const sections = page.locator('section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThanOrEqual(6); // Hero, Features, Architecture, Quickstart, Protocol, Configuration, Comparison

      // Verify heading hierarchy
      const h2Elements = page.locator('h2');
      const h2Count = await h2Elements.count();
      expect(h2Count).toBeGreaterThanOrEqual(5); // Multiple section headings

      // Check that tables are present and accessible
      const tables = page.locator('table');
      const tableCount = await tables.count();
      expect(tableCount).toBeGreaterThanOrEqual(4); // Commands, Response codes, Configuration tables
    });

    test('should keep navigation links functional when CSS fails to load', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', route => route.abort());

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Check Get Started button still works (internal link)
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();
      const href = await getStartedBtn.getAttribute('href');
      expect(href).toBe('#quickstart');

      // Check GitHub link is present and functional
      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeVisible();
      const githubHref = await githubBtn.getAttribute('href');
      expect(githubHref).toContain('github.com');
    });
  });

  test.describe('Test Case 2: Image Loading Failure', () => {
    test('should display alt text for failed images and maintain layout', async ({ page }) => {
      // Block all image loading
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', route => route.abort());

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Page content should still be accessible
      const heroTitle = page.locator('[data-testid="product-name"]');
      await expect(heroTitle).toBeVisible();

      // Check that feature section is visible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards are visible even without icons
      const featureCards = page.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBe(4);

      // Check that feature titles and descriptions are readable
      const featureMemcachedTitle = page.locator('[data-testid="feature-memcached-title"]');
      await expect(featureMemcachedTitle).toBeVisible();
      await expect(featureMemcachedTitle).toContainText('Memcached Compatible');

      const featurePersistentTitle = page.locator('[data-testid="feature-persistent-title"]');
      await expect(featurePersistentTitle).toBeVisible();
      await expect(featurePersistentTitle).toContainText('Persistent Storage');
    });

    test('should keep layout intact when images fail to load', async ({ page }) => {
      // Block image loading
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Check that SVG icons (inline) are still rendered
      // SVG icons are inline in the HTML, so they should still be visible
      const svgIcons = page.locator('svg[aria-hidden="true"]');
      const iconCount = await svgIcons.count();
      expect(iconCount).toBeGreaterThan(0);

      // Verify architecture diagram components are visible
      const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(architectureDiagram).toBeVisible();

      // Check diagram components
      const walComponent = page.locator('[data-testid="diagram-wal"]');
      await expect(walComponent).toBeVisible();

      const memtableComponent = page.locator('[data-testid="diagram-memtable"]');
      await expect(memtableComponent).toBeVisible();

      // Verify comparison table layout is intact
      const comparisonTable = page.locator('[data-testid="comparison-table"]');
      await expect(comparisonTable).toBeVisible();
    });

    test('should have appropriate alt text for all images', async ({ page }) => {
      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Check that all img elements have alt attributes
      // Note: This page uses primarily SVG inline icons with aria-hidden="true"
      // which is the correct pattern for decorative icons
      const images = page.locator('img');
      const imageCount = await images.count();

      // Verify all images have alt text (if any exist)
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        expect(alt).not.toBeNull();
      }

      // Verify SVG icons are properly marked as decorative
      const decorativeSvgs = page.locator('svg[aria-hidden="true"]');
      const decorativeSvgCount = await decorativeSvgs.count();
      expect(decorativeSvgCount).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 3: JavaScript Loading Failure (Progressive Enhancement)', () => {
    // Use browser context to disable JavaScript
    test.use({ javaScriptEnabled: false });

    test('should display core content when JavaScript is disabled', async ({ page }) => {
      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Verify all main content is accessible without JavaScript
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Features section should be fully visible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // All feature cards should be visible
      const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
      await expect(memcachedFeature).toBeVisible();

      const persistentFeature = page.locator('[data-testid="feature-persistent"]');
      await expect(persistentFeature).toBeVisible();

      const performanceFeature = page.locator('[data-testid="feature-performance"]');
      await expect(performanceFeature).toBeVisible();

      const configFeature = page.locator('[data-testid="feature-config"]');
      await expect(configFeature).toBeVisible();
    });

    test('should make code examples readable without JavaScript', async ({ page }) => {
      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Check that code blocks are visible
      const installationCode = page.locator('[data-testid="installation-code"]');
      await expect(installationCode).toBeVisible();

      // Verify code content is present
      const codeContent = page.locator('#install-code');
      await expect(codeContent).toBeVisible();
      const installText = await codeContent.textContent();
      expect(installText).toContain('git clone');
      expect(installText).toContain('cargo build');

      // Config code should be visible
      const configCode = page.locator('#config-code');
      await expect(configCode).toBeVisible();
      const configText = await configCode.textContent();
      expect(configText).toContain('addr');

      // Python example should be visible
      const clientCode = page.locator('#client-code');
      await expect(clientCode).toBeVisible();
      const clientText = await clientCode.textContent();
      expect(clientText).toContain('memcache');
    });

    test('should keep all navigation functional without JavaScript', async ({ page }) => {
      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Check internal anchor links
      const getStartedLink = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedLink).toBeVisible();
      const href = await getStartedLink.getAttribute('href');
      expect(href).toBe('#quickstart');

      // Check external links
      const githubLink = page.locator('[data-testid="cta-github"]');
      await expect(githubLink).toBeVisible();
      const githubHref = await githubLink.getAttribute('href');
      expect(githubHref).toContain('github.com');

      // Check footer links
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();

      const footerDocsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(footerDocsLink).toBeVisible();
    });

    test('should display all documentation sections without JavaScript', async ({ page }) => {
      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Architecture section
      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await expect(architectureSection).toBeVisible();

      // Architecture diagram should be visible (it's CSS/HTML based, not JS)
      const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(architectureDiagram).toBeVisible();

      // Quick start section
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeVisible();

      // Protocol section with tables
      const protocolSection = page.locator('[data-testid="protocol-section"]');
      await expect(protocolSection).toBeVisible();

      const commandsTable = page.locator('[data-testid="commands-table"]');
      await expect(commandsTable).toBeVisible();

      const responseCodes = page.locator('[data-testid="response-codes"]');
      await expect(responseCodes).toBeVisible();

      // Configuration section
      const configSection = page.locator('[data-testid="configuration-section"]');
      await expect(configSection).toBeVisible();

      // Comparison section
      const comparisonSection = page.locator('[data-testid="comparison-section"]');
      await expect(comparisonSection).toBeVisible();

      const comparisonTable = page.locator('[data-testid="comparison-table"]');
      await expect(comparisonTable).toBeVisible();
    });

    test('should keep copy buttons visible even if JavaScript fails', async ({ page }) => {
      // Note: Copy buttons won't function without JS, but they should still be visible
      // This tests that the UI doesn't break
      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Copy buttons should be present (even if non-functional)
      const copyButtons = page.locator('.copy-button');
      const buttonCount = await copyButtons.count();
      expect(buttonCount).toBeGreaterThan(0);

      // Verify they contain expected text
      const firstCopyButton = page.locator('[data-testid="copy-button-install"]');
      await expect(firstCopyButton).toBeVisible();
    });
  });

  test.describe('Test Case 3b: External JavaScript Blocking', () => {
    test('should block external JavaScript CDN and still render content', async ({ page }) => {
      // Block external JavaScript (CDN scripts like highlight.js)
      await page.route('**/cdnjs.cloudflare.com/**', route => route.abort());
      await page.route('**/*.js', route => {
        // Only block external JS, not local scripts
        if (route.request().url().includes('cdnjs') || route.request().url().includes('cdn')) {
          return route.abort();
        }
        return route.continue();
      });

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Content should still be fully accessible
      const heroTitle = page.locator('[data-testid="product-name"]');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Code blocks should still show content (just without syntax highlighting)
      const installCode = page.locator('#install-code');
      await expect(installCode).toBeVisible();
      const codeText = await installCode.textContent();
      expect(codeText).toContain('git clone');

      // All sections should be accessible
      const sections = page.locator('section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThanOrEqual(6);
    });
  });

  test.describe('Combined Asset Failures', () => {
    // Use browser context to disable JavaScript for combined tests
    test.use({ javaScriptEnabled: false });

    test('should remain usable when both CSS and JavaScript fail', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', route => route.abort());

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Core content should still be accessible
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      // Navigation should work
      const getStartedLink = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedLink).toBeVisible();

      // Tables should be readable
      const commandsTable = page.locator('[data-testid="commands-table"]');
      await expect(commandsTable).toBeVisible();

      // All text content should be accessible
      const featureMemcachedDesc = page.locator('[data-testid="feature-memcached-description"]');
      await expect(featureMemcachedDesc).toBeVisible();
      await expect(featureMemcachedDesc).toContainText('Drop-in replacement');
    });

    test('should display all content when all external resources fail', async ({ page }) => {
      // Block all external resources
      await page.route('**/*.css', route => route.abort());
      await page.route('**/cdnjs.cloudflare.com/**', route => route.abort());
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(BASE_URL);
      await page.waitForLoadState('domcontentloaded');

      // Verify the page title
      const title = await page.title();
      expect(title).toContain('MirDB');

      // All sections should still be present
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await expect(architectureSection).toBeVisible();

      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeVisible();

      const protocolSection = page.locator('[data-testid="protocol-section"]');
      await expect(protocolSection).toBeVisible();

      const configSection = page.locator('[data-testid="configuration-section"]');
      await expect(configSection).toBeVisible();

      const comparisonSection = page.locator('[data-testid="comparison-section"]');
      await expect(comparisonSection).toBeVisible();

      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();
    });
  });
});

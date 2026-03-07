/**
 * Progressive Enhancement E2E Tests
 * Owner: Scenario 13 - Progressive Enhancement
 *
 * Tests:
 * - Core content renders without JavaScript
 * - Navigation links work without JS (standard anchor behavior)
 * - No critical information requires JavaScript to view
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = `file://${path.join(__dirname, '../../index.html')}`;

test.describe('Progressive Enhancement (Scenario 13)', () => {
  // Use a context with JavaScript disabled for all tests in this suite
  test.use({ javaScriptEnabled: false });

  test.describe('Test Case 1: Core content renders without JavaScript', () => {
    test('Hero section is visible without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Verify hero section elements are visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check hero title
      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Check hero tagline
      const heroTagline = page.locator('.hero__tagline');
      await expect(heroTagline).toBeVisible();
      await expect(heroTagline).toContainText('Persistent Key-Value Store');

      // Check hero description
      const heroDescription = page.locator('.hero__description');
      await expect(heroDescription).toBeVisible();

      // Check hero logo
      const heroLogo = page.locator('.hero__logo');
      await expect(heroLogo).toBeVisible();
    });

    test('Features section is visible without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Verify features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Check features title
      const featuresTitle = page.locator('#features-title');
      await expect(featuresTitle).toBeVisible();
      await expect(featuresTitle).toHaveText('Key Features');

      // Check feature cards are visible (at least 4)
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify each feature card is visible
      for (let i = 0; i < 4; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // Check feature card titles are present
      const tokioCard = page.locator('[data-feature="tokio"] .feature-card__title');
      await expect(tokioCard).toContainText('Tokio');

      const memcachedCard = page.locator('[data-feature="memcached"] .feature-card__title');
      await expect(memcachedCard).toContainText('Memcached');

      const skiplistCard = page.locator('[data-feature="skiplist"] .feature-card__title');
      await expect(skiplistCard).toContainText('Skip-list');

      const compactionCard = page.locator('[data-feature="compaction"] .feature-card__title');
      await expect(compactionCard).toContainText('Compaction');
    });

    test('Quick-start section is visible without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Verify quick-start section
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Check quickstart title
      const quickstartTitle = page.locator('#quickstart-title');
      await expect(quickstartTitle).toBeVisible();
      await expect(quickstartTitle).toHaveText('Quick Start');

      // Check code blocks are visible
      const codeBlocks = page.locator('.code-block');
      await expect(codeBlocks.first()).toBeVisible();

      // Verify installation code content is readable
      const installationCode = page.locator('[data-code-block="installation"] .code-block__code');
      await expect(installationCode).toBeVisible();
      await expect(installationCode).toContainText('git clone');
      await expect(installationCode).toContainText('cargo build');

      // Verify usage code content is readable
      const usageCode = page.locator('[data-code-block="usage"] .code-block__code');
      await expect(usageCode).toBeVisible();
      await expect(usageCode).toContainText('SET');
      await expect(usageCode).toContainText('GET');
    });

    test('Terminal demo section is visible without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Verify terminal demo section
      const terminalSection = page.locator('#terminal-demo');
      await expect(terminalSection).toBeVisible();

      // Check terminal window is visible
      const terminal = page.locator('.terminal');
      await expect(terminal).toBeVisible();

      // Check terminal body with commands is visible
      const terminalBody = page.locator('.terminal__body');
      await expect(terminalBody).toBeVisible();

      // Verify terminal commands are readable
      await expect(terminalBody).toContainText('SET');
      await expect(terminalBody).toContainText('GET');
      await expect(terminalBody).toContainText('DELETE');
      await expect(terminalBody).toContainText('STORED');
    });

    test('Architecture section is visible without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Verify architecture section
      const archSection = page.locator('#architecture');
      await expect(archSection).toBeVisible();

      // Check architecture title
      const archTitle = page.locator('#architecture-title');
      await expect(archTitle).toBeVisible();
      await expect(archTitle).toContainText('LSM Tree Architecture');

      // Check architecture description
      const archDescription = page.locator('.architecture__description');
      await expect(archDescription).toBeVisible();

      // Check architecture diagram
      const archDiagram = page.locator('.architecture__diagram img');
      await expect(archDiagram).toBeAttached();
    });
  });

  test.describe('Test Case 2: Navigation links work without JavaScript', () => {
    test('Navigation links are present and use standard anchors', async ({ page }) => {
      await page.goto(indexPath);

      // Check navigation exists
      const nav = page.locator('nav.nav');
      await expect(nav).toBeVisible();

      // Check navigation links
      const featuresLink = page.locator('.nav__link[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveAttribute('href', '#features');

      const usageLink = page.locator('.nav__link[href="#terminal-demo"]');
      await expect(usageLink).toBeVisible();
      await expect(usageLink).toHaveAttribute('href', '#terminal-demo');

      const quickstartLink = page.locator('.nav__link[href="#quickstart"]');
      await expect(quickstartLink).toBeVisible();
      await expect(quickstartLink).toHaveAttribute('href', '#quickstart');

      const archLink = page.locator('.nav__link[href="#architecture"]');
      await expect(archLink).toBeVisible();
      await expect(archLink).toHaveAttribute('href', '#architecture');
    });

    test('Clicking Features link scrolls to features section', async ({ page }) => {
      await page.goto(indexPath);

      // Click the features navigation link
      const featuresLink = page.locator('.nav__link[href="#features"]');
      await featuresLink.click();

      // Wait for navigation
      await page.waitForTimeout(300);

      // Verify URL hash changed
      expect(page.url()).toContain('#features');

      // Verify features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('Clicking Quick Start link scrolls to quickstart section', async ({ page }) => {
      await page.goto(indexPath);

      // Click the quickstart navigation link
      const quickstartLink = page.locator('.nav__link[href="#quickstart"]');
      await quickstartLink.click();

      // Wait for navigation
      await page.waitForTimeout(300);

      // Verify URL hash changed
      expect(page.url()).toContain('#quickstart');

      // Verify quickstart section is in viewport
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });

    test('External GitHub link works without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Check GitHub link exists with proper attributes
      const githubLink = page.locator('a.github-link');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubLink).toHaveAttribute('target', '_blank');
    });

    test('Skip to main content link works without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Focus on skip link
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeAttached();

      // Verify skip link has correct href
      await expect(skipLink).toHaveAttribute('href', '#main-content');
    });

    test('Footer GitHub link works without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Check footer GitHub link
      const footerGithubLink = page.locator('.footer a[href="https://github.com/yetone/mirdb"]');
      await expect(footerGithubLink).toBeVisible();
      await expect(footerGithubLink).toHaveText(/View on GitHub/);
    });
  });

  test.describe('Test Case 3: No critical information requires JavaScript', () => {
    test('All text content is server-rendered (not JS-injected)', async ({ page }) => {
      await page.goto(indexPath);

      // All critical text should be visible without JS
      // Hero section
      await expect(page.locator('.hero__title')).toHaveText('MirDB');
      await expect(page.locator('.hero__tagline')).toContainText('Persistent Key-Value Store');

      // Features section
      await expect(page.locator('.section__title').first()).toBeVisible();

      // Terminal demo
      await expect(page.locator('.terminal__command').first()).toBeVisible();

      // Quick start
      await expect(page.locator('.code-block__code').first()).toBeVisible();

      // Architecture
      await expect(page.locator('.architecture__description')).toBeVisible();
    });

    test('Images have proper alt text for no-JS fallback', async ({ page }) => {
      await page.goto(indexPath);

      // Check logo images have alt text
      const logoImages = page.locator('img[alt*="Logo"]');
      const logoCount = await logoImages.count();
      expect(logoCount).toBeGreaterThanOrEqual(1);

      // Check architecture diagram has alt text
      const archImage = page.locator('.architecture__image');
      const archAlt = await archImage.getAttribute('alt');
      expect(archAlt).toBeTruthy();
      expect(archAlt.length).toBeGreaterThan(20); // Descriptive alt text

      // Check usage gif has alt text
      const usageGif = page.locator('.terminal-demo__usage-gif');
      const usageAlt = await usageGif.getAttribute('alt');
      expect(usageAlt).toBeTruthy();
    });

    test('No elements hidden behind JS-only interactions', async ({ page }) => {
      await page.goto(indexPath);

      // Verify no elements have display:none that would require JS to show
      // Check main sections are all visible
      const sections = ['#features', '#terminal-demo', '#quickstart', '#architecture'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await expect(section).toBeVisible();

        // Verify the section is not using visibility:hidden or opacity:0
        const visibility = await section.evaluate(el => getComputedStyle(el).visibility);
        expect(visibility).toBe('visible');

        const opacity = await section.evaluate(el => getComputedStyle(el).opacity);
        expect(parseFloat(opacity)).toBeGreaterThan(0);
      }
    });

    test('Copy buttons are visible but non-functional without JS (graceful degradation)', async ({ page }) => {
      await page.goto(indexPath);

      // Copy buttons should be present in the DOM
      const copyButtons = page.locator('.code-block__copy-btn');
      const buttonCount = await copyButtons.count();

      // Buttons should exist but clicking them should not cause errors
      // In a no-JS environment, they simply don't do anything
      expect(buttonCount).toBeGreaterThanOrEqual(2);

      // Buttons should be visible (not hidden)
      for (let i = 0; i < buttonCount; i++) {
        await expect(copyButtons.nth(i)).toBeVisible();
      }
    });

    test('Theme toggle button is present but theme defaults work without JS', async ({ page }) => {
      await page.goto(indexPath);

      // Theme toggle button should be present
      const themeToggle = page.locator('.theme-toggle');
      await expect(themeToggle).toBeVisible();

      // Page should have a default theme that works without JS
      // Check that CSS custom properties are applied
      const body = page.locator('body');
      const backgroundColor = await body.evaluate(el => getComputedStyle(el).backgroundColor);

      // Background color should be set (not transparent or default)
      expect(backgroundColor).toBeTruthy();
      expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('Code blocks are readable without copy functionality', async ({ page }) => {
      await page.goto(indexPath);

      // Installation code block content
      const installCode = page.locator('[data-code-block="installation"] code');
      await expect(installCode).toBeVisible();
      const installText = await installCode.textContent();

      // Verify critical installation commands are visible
      expect(installText).toContain('git clone https://github.com/yetone/mirdb.git');
      expect(installText).toContain('cargo build --release');
      expect(installText).toContain('./target/release/mirdb-server');

      // Usage code block content
      const usageCode = page.locator('[data-code-block="usage"] code');
      await expect(usageCode).toBeVisible();
      const usageText = await usageCode.textContent();

      // Verify critical usage commands are visible
      expect(usageText).toContain('telnet localhost 12333');
      expect(usageText).toContain('SET mykey');
      expect(usageText).toContain('GET mykey');
      expect(usageText).toContain('DELETE mykey');
    });

    test('Page layout is functional without JavaScript', async ({ page }) => {
      await page.goto(indexPath);

      // Check that header is properly positioned
      const header = page.locator('header.header');
      await expect(header).toBeVisible();

      // Check that main content is accessible
      const main = page.locator('main#main-content');
      await expect(main).toBeVisible();

      // Check that footer is visible
      const footer = page.locator('footer.footer');
      await expect(footer).toBeVisible();

      // Verify page has proper structure (header, main, footer)
      const headerY = await header.evaluate(el => el.getBoundingClientRect().top);
      const mainY = await main.evaluate(el => el.getBoundingClientRect().top);
      const footerY = await footer.evaluate(el => el.getBoundingClientRect().top);

      // Elements should be in correct order vertically
      expect(headerY).toBeLessThan(mainY);
    });
  });
});

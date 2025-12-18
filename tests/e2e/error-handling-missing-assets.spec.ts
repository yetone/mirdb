import { test, expect } from '@playwright/test';

/**
 * Error Handling - Missing Assets Tests
 *
 * These tests verify graceful handling when assets fail to load,
 * implementing progressive enhancement principles.
 */

test.describe('Error Handling - Missing Assets', () => {

  test.describe('Test Case 1: CSS Disabled - Content Readability', () => {
    test('content is readable and accessible without CSS', async ({ page }) => {
      // Block all CSS files from loading
      await page.route('**/*.css', (route) => route.abort());
      await page.route('**/prism*.css', (route) => route.abort());

      await page.goto('/');

      // Verify page loads without errors
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Verify main content is present and readable
      const heroHeading = page.locator('h1');
      await expect(heroHeading).toBeVisible();
      await expect(heroHeading).toContainText('MirDB');

      // Verify tagline is readable
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('persistent key-value store');

      // Verify navigation links are present
      const navLinks = page.locator('.nav-links a');
      await expect(navLinks.first()).toBeVisible();

      // Verify features section content is accessible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const featureHeading = page.locator('#features h2');
      await expect(featureHeading).toContainText('Key Features');

      // Verify feature cards text is readable
      const featureCards = page.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBeGreaterThan(0);

      for (let i = 0; i < featureCount; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();
        const cardTitle = card.locator('h3');
        await expect(cardTitle).toBeVisible();
      }

      // Verify getting started section is present
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Verify code blocks are present and readable
      const codeBlocks = page.locator('pre code');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      // Verify commands section is accessible
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();

      // Verify footer is present
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('text content has sufficient natural contrast without CSS', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', (route) => route.abort());

      await page.goto('/');

      // Without CSS, browser default styling should make text readable
      // The h1 should be visible with default browser styling
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      // Paragraphs should be readable
      const paragraphs = page.locator('p');
      const paragraphCount = await paragraphs.count();
      expect(paragraphCount).toBeGreaterThan(0);

      // First paragraph should be visible
      await expect(paragraphs.first()).toBeVisible();
    });

    test('semantic HTML structure provides readable document flow without CSS', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', (route) => route.abort());

      await page.goto('/');

      // Verify semantic structure is present
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      const header = page.locator('header.hero');
      await expect(header).toBeVisible();

      const main = page.locator('main');
      await expect(main).toBeVisible();

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Verify headings create logical hierarchy (h1, h2, h3)
      const h1 = page.locator('h1');
      await expect(h1).toHaveCount(1);

      const h2s = page.locator('h2');
      expect(await h2s.count()).toBeGreaterThan(0);

      const h3s = page.locator('h3');
      expect(await h3s.count()).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 2: JS Disabled - Core Functionality', () => {
    test('core content is accessible without JavaScript', async ({ browser }) => {
      // Create context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Verify main content is present
      const heroHeading = page.locator('h1');
      await expect(heroHeading).toBeVisible();
      await expect(heroHeading).toContainText('MirDB');

      // Verify tagline is visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // Verify all main sections are visible
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      const commands = page.locator('#commands');
      await expect(commands).toBeVisible();

      const architecture = page.locator('#architecture');
      await expect(architecture).toBeVisible();

      await context.close();
    });

    test('navigation links work without JavaScript', async ({ browser }) => {
      // Create context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Verify all navigation links are present
      const navLinks = page.locator('.nav-links a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(5);

      // Verify internal anchor links have valid hrefs
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      const architectureLink = page.locator('.nav-links a[href="#architecture"]');
      await expect(architectureLink).toBeVisible();

      const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
      await expect(gettingStartedLink).toBeVisible();

      const commandsLink = page.locator('.nav-links a[href="#commands"]');
      await expect(commandsLink).toBeVisible();

      // Click on Features link and verify it navigates
      await featuresLink.click();

      // Verify the features section is in view (URL should have #features)
      await expect(page).toHaveURL(/#features$/);

      // Navigate to getting started
      await gettingStartedLink.click();
      await expect(page).toHaveURL(/#getting-started$/);

      await context.close();
    });

    test('external links have correct attributes without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Verify GitHub link in navigation
      const githubNavLink = page.locator('.nav-links a[href*="github"]');
      await expect(githubNavLink).toBeVisible();
      await expect(githubNavLink).toHaveAttribute('target', '_blank');
      await expect(githubNavLink).toHaveAttribute('rel', 'noopener noreferrer');

      // Verify GitHub button in hero section
      const githubHeroButton = page.locator('.hero-buttons a[href*="github"]');
      await expect(githubHeroButton).toBeVisible();
      await expect(githubHeroButton).toHaveAttribute('target', '_blank');

      // Verify footer GitHub link
      const githubFooterLink = page.locator('footer a[href*="github"]');
      await expect(githubFooterLink).toBeVisible();

      await context.close();
    });

    test('code blocks are readable without syntax highlighting', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Verify code blocks are present and contain text
      const codeBlocks = page.locator('pre code');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      // Verify bash code block contains expected content
      const bashCode = page.locator('pre code.language-bash');
      await expect(bashCode.first()).toBeVisible();
      const bashText = await bashCode.first().textContent();
      expect(bashText).toContain('git clone');

      // Verify Python code block contains expected content
      const pythonCode = page.locator('pre code.language-python');
      await expect(pythonCode.first()).toBeVisible();
      const pythonText = await pythonCode.first().textContent();
      expect(pythonText).toContain('pymemcache');

      // Verify TOML code block contains expected content
      const tomlCode = page.locator('pre code.language-toml');
      await expect(tomlCode.first()).toBeVisible();
      const tomlText = await tomlCode.first().textContent();
      expect(tomlText).toContain('addr');

      await context.close();
    });

    test('CTA buttons work as links without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Verify "Get Started" button is a proper link
      const getStartedBtn = page.locator('.hero-buttons a[href="#getting-started"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');

      // Click and verify navigation works
      await getStartedBtn.click();
      await expect(page).toHaveURL(/#getting-started$/);

      await context.close();
    });
  });

  test.describe('Test Case 3: Image Fallbacks', () => {
    test('SVG architecture diagram has proper accessible fallback', async ({ page }) => {
      await page.goto('/');

      // Verify SVG has role="img" for accessibility
      const svg = page.locator('.architecture-diagram');
      await expect(svg).toBeVisible();
      await expect(svg).toHaveAttribute('role', 'img');

      // Verify SVG has aria-label for screen readers
      await expect(svg).toHaveAttribute('aria-label', 'LSM Tree Architecture Diagram');

      // Verify SVG has title element for tooltip/fallback
      const svgTitle = page.locator('.architecture-diagram title');
      await expect(svgTitle).toHaveText('MirDB LSM Tree Architecture');
    });

    test('broken external images would show alt text', async ({ page }) => {
      // Block all image requests to simulate broken images
      await page.route('**/*.png', (route) => route.abort());
      await page.route('**/*.jpg', (route) => route.abort());
      await page.route('**/*.jpeg', (route) => route.abort());
      await page.route('**/*.gif', (route) => route.abort());
      await page.route('**/*.webp', (route) => route.abort());

      await page.goto('/');

      // Page should still load and be functional
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // All img tags should have alt attributes
      const images = page.locator('img');
      const imageCount = await images.count();

      // If there are images, verify they have alt attributes
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');
        // Alt should exist (can be empty string for decorative images)
        expect(altText).not.toBeNull();
      }

      // SVG diagram should still be visible as it's inline
      const architectureDiagram = page.locator('.architecture-diagram');
      await expect(architectureDiagram).toBeVisible();
    });

    test('feature icons use Unicode characters as fallback', async ({ page }) => {
      await page.goto('/');

      // Verify feature icons are using Unicode/emoji characters
      // These don't require external assets to load
      const featureIcons = page.locator('.feature-icon');
      const iconCount = await featureIcons.count();
      expect(iconCount).toBe(4);

      // Each icon should be visible and contain text (Unicode character)
      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        await expect(icon).toBeVisible();
        const iconText = await icon.textContent();
        expect(iconText?.trim().length).toBeGreaterThan(0);
      }
    });

    test('page remains functional with failed CDN resources', async ({ page }) => {
      // Block CDN requests (Prism.js for syntax highlighting)
      await page.route('**/cdnjs.cloudflare.com/**', (route) => route.abort());

      await page.goto('/');

      // Page should still load
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Main content should be visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Code blocks should still be present (just without highlighting)
      const codeBlocks = page.locator('pre code');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      // Navigation should work
      const navLinks = page.locator('.nav-links a');
      await expect(navLinks.first()).toBeVisible();
    });

    test('inline SVG diagram renders without external dependencies', async ({ page }) => {
      // Block all external resources
      await page.route('**/cdnjs.cloudflare.com/**', (route) => route.abort());
      await page.route('**/*.css', (route) => route.abort());

      await page.goto('/');

      // SVG is inline, so it should still render
      const svg = page.locator('.architecture-diagram');
      await expect(svg).toBeVisible();

      // Verify SVG elements are present
      const svgRects = page.locator('.architecture-diagram rect');
      expect(await svgRects.count()).toBeGreaterThan(0);

      const svgTexts = page.locator('.architecture-diagram text');
      expect(await svgTexts.count()).toBeGreaterThan(0);
    });
  });

  test.describe('Progressive Enhancement Verification', () => {
    test('page provides base functionality without any enhancements', async ({ browser }) => {
      // Create context with JS disabled and block CSS
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      // Block CSS and CDN resources
      await page.route('**/*.css', (route) => route.abort());
      await page.route('**/cdnjs.cloudflare.com/**', (route) => route.abort());

      await page.goto('/');

      // Verify core functionality still works
      // 1. Main heading is visible
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // 2. Navigation links are functional
      const navLinks = page.locator('.nav-links a[href^="#"]');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(4);

      // 3. All section content is accessible
      const sections = ['#features', '#getting-started', '#commands', '#architecture'];
      for (const section of sections) {
        const sectionElement = page.locator(section);
        await expect(sectionElement).toBeVisible();
      }

      // 4. Code examples are readable
      const codeBlocks = page.locator('pre code');
      expect(await codeBlocks.count()).toBeGreaterThan(0);

      // 5. Footer information is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      await context.close();
    });

    test('semantic HTML provides meaningful structure', async ({ page }) => {
      await page.goto('/');

      // Verify proper semantic elements are used
      const nav = page.locator('nav[role="navigation"]');
      await expect(nav).toBeVisible();

      const header = page.locator('header[role="banner"]');
      await expect(header).toBeVisible();

      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Sections should have IDs for anchor navigation
      const featuresSection = page.locator('section#features');
      await expect(featuresSection).toBeVisible();

      const gettingStartedSection = page.locator('section#getting-started');
      await expect(gettingStartedSection).toBeVisible();
    });
  });
});

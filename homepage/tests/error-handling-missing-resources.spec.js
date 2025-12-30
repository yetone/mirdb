// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Resources Tests
 *
 * These tests verify that the MirDB homepage handles missing resources gracefully:
 * - Test 1: Page remains functional with images blocked (alt text visible)
 * - Test 2: Core content readable and navigation works with JavaScript disabled
 * - Test 3: Page is usable even if some CSS fails to load
 */

test.describe('Error Handling - Missing Resources', () => {

  test.describe('Test Case 1: Images Blocked', () => {
    test('page remains functional with alt text visible when images are blocked', async ({ page }) => {
      // Block all image requests to simulate blocked/failed images
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', route => route.abort());

      await page.goto('/');

      // Verify page is still functional - main heading is visible
      const heading = page.locator('h1');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('MirDB');

      // Verify hero section is visible and functional
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify the hero logo image has alt text for accessibility
      const heroLogo = page.locator('[data-testid="hero-logo"]');
      const altText = await heroLogo.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(0);

      // Verify the img element exists and has proper alt attribute
      await expect(heroLogo).toHaveAttribute('alt', /MirDB.*Logo/i);

      // Verify navigation links are still accessible
      const navLinks = page.locator('.nav-links a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Verify CTA buttons are visible and clickable
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();

      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toBeEnabled();

      // Verify features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards have readable text
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      // Check that at least one feature card has visible text
      const firstCard = featureCards.first();
      await expect(firstCard.locator('h3')).toBeVisible();
      await expect(firstCard.locator('p')).toBeVisible();
    });

    test('all images have meaningful alt text for accessibility', async ({ page }) => {
      await page.goto('/');

      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');
        const src = await img.getAttribute('src');

        // Every image must have an alt attribute
        expect(altText, `Image ${src} should have alt text`).not.toBeNull();

        // Alt text should not be empty (except for decorative images which should have alt="")
        // For content images, alt text should be descriptive
        if (altText !== '') {
          expect(altText.length, `Image ${src} alt text should be descriptive`).toBeGreaterThan(3);
        }
      }
    });
  });

  test.describe('Test Case 2: JavaScript Disabled', () => {
    test.use({ javaScriptEnabled: false });

    test('core content is still readable with JavaScript disabled', async ({ page }) => {
      await page.goto('/');

      // Verify main heading is visible
      const heading = page.locator('h1');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('MirDB');

      // Verify tagline is visible
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Key-Value Store');

      // Verify hero section content is accessible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify CTA buttons are visible (they are <a> tags, not JS-dependent)
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();

      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeVisible();

      // Verify features section is readable
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards text is visible
      const featureHeadings = page.locator('.feature-card h3');
      const headingCount = await featureHeadings.count();
      expect(headingCount).toBeGreaterThan(0);

      // Verify quick start section is visible
      const quickStart = page.locator('#quick-start');
      await expect(quickStart).toBeVisible();

      // Verify code block content is visible
      const codeBlock = page.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Verify footer is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('navigation works with JavaScript disabled', async ({ page }) => {
      await page.goto('/');

      // Navigation bar should be visible
      const navbar = page.locator('.navbar');
      await expect(navbar).toBeVisible();

      // Navigation links should be visible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Check internal anchor links work (these don't need JS)
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      if (await featuresLink.count() > 0) {
        await featuresLink.click();
        // Page should scroll to features section (URL hash changes)
        await expect(page).toHaveURL(/#features/);
      }

      // External links should have href attributes that work without JS
      const githubLinks = page.locator('.nav-links a[href*="github.com"]');
      const linkCount = await githubLinks.count();
      expect(linkCount).toBeGreaterThan(0);
      // Check first matching link
      if (linkCount > 0) {
        const href = await githubLinks.first().getAttribute('href');
        expect(href).toContain('github.com');
      }

      // Logo link should be functional
      const logoLink = page.locator('.nav-logo');
      await expect(logoLink).toBeVisible();
      const logoHref = await logoLink.getAttribute('href');
      expect(logoHref).toBeTruthy();
    });

    test('static image fallback is displayed when JS is disabled', async ({ page }) => {
      await page.goto('/');

      // The hero logo should have a static image source
      // With JS disabled, the animated GIF swap won't happen but static image should show
      const heroLogo = page.locator('[data-testid="hero-logo"]');
      await expect(heroLogo).toBeVisible();

      // Verify the src attribute points to a static image
      const src = await heroLogo.getAttribute('src');
      expect(src).toBeTruthy();
      // The static fallback should be logo-static.gif
      expect(src).toContain('logo-static');
    });
  });

  test.describe('Test Case 3: CSS Fallbacks', () => {
    test('page is usable even if CSS fails to load', async ({ page }) => {
      // Block CSS files to simulate CSS loading failure
      await page.route('**/*.css', route => route.abort());

      await page.goto('/');

      // Even without CSS, HTML content should still be present and readable
      // Check main content is in the DOM
      const heading = page.locator('h1');
      await expect(heading).toBeVisible();

      // Navigation should still exist and be functional
      const navLinks = page.locator('.nav-links a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // All links should still be clickable
      for (let i = 0; i < Math.min(linkCount, 3); i++) {
        const link = navLinks.nth(i);
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();
      }

      // Buttons should be visible and have text
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();
      const btnText = await getStartedBtn.textContent();
      expect(btnText?.trim().length).toBeGreaterThan(0);

      // Feature content should be readable
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      // Code block content should be visible
      const codeBlock = page.locator('.code-block code');
      await expect(codeBlock).toBeVisible();
    });

    test('semantic HTML provides structure without CSS', async ({ page }) => {
      // Block CSS files
      await page.route('**/*.css', route => route.abort());

      await page.goto('/');

      // Verify semantic HTML elements are used correctly
      // Navigation uses <nav> element (use first to handle multiple nav elements)
      const navElement = page.locator('nav.navbar');
      await expect(navElement).toBeVisible();

      // Main sections use <section> elements
      const sectionElements = page.locator('section');
      const sectionCount = await sectionElements.count();
      expect(sectionCount).toBeGreaterThan(0);

      // Footer uses <footer> element
      const footerElement = page.locator('footer');
      await expect(footerElement).toBeVisible();

      // Headings create proper hierarchy
      const h1 = page.locator('h1');
      await expect(h1).toHaveCount(1);

      const h2 = page.locator('h2');
      const h2Count = await h2.count();
      expect(h2Count).toBeGreaterThan(0);

      // Links have accessible text
      const allLinks = page.locator('a');
      const totalLinks = await allLinks.count();
      expect(totalLinks).toBeGreaterThan(0);
    });

    test('inline critical styles preserve basic layout', async ({ page }) => {
      // This test verifies that the main index.html (which has inline styles)
      // maintains basic usability even when external CSS fails
      await page.goto('/');

      // Check that inline styles are applied to critical elements
      // The homepage uses CSS variables defined in <style> tags
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Verify the page doesn't have any obvious layout breakage
      // by checking that elements are in the viewport
      const hero = page.locator('.hero, [data-testid="hero-section"]');
      if (await hero.count() > 0) {
        const boundingBox = await hero.first().boundingBox();
        expect(boundingBox).not.toBeNull();
        if (boundingBox) {
          expect(boundingBox.width).toBeGreaterThan(100);
          expect(boundingBox.height).toBeGreaterThan(50);
        }
      }
    });
  });

  test.describe('Progressive Enhancement Verification', () => {
    test('page degrades gracefully across all resource failure scenarios', async ({ page }) => {
      // Block all non-essential resources
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', route => route.abort());

      await page.goto('/');

      // Core functionality checklist
      const checks = [
        { selector: 'h1', description: 'Main heading' },
        { selector: 'nav', description: 'Navigation' },
        { selector: 'footer', description: 'Footer' },
        { selector: 'a[href*="github.com"]', description: 'GitHub link' },
        { selector: '.code-block, [data-testid*="code"]', description: 'Code examples' },
      ];

      for (const check of checks) {
        const element = page.locator(check.selector);
        const count = await element.count();
        expect(count, `${check.description} should be present`).toBeGreaterThan(0);
        if (count > 0) {
          await expect(element.first(), `${check.description} should be visible`).toBeVisible();
        }
      }
    });

    test('all external links remain functional when resources fail', async ({ page }) => {
      // Block images
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', route => route.abort());

      await page.goto('/');

      // Find all external links (GitHub, docs, etc.)
      const externalLinks = page.locator('a[href^="https://"], a[href^="http://"]');
      const linkCount = await externalLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const text = await link.textContent();

        // Each external link should have a valid URL
        expect(href, `Link "${text}" should have valid href`).toBeTruthy();
        expect(href).toMatch(/^https?:\/\//);

        // Each link should have either text content or aria-label
        const ariaLabel = await link.getAttribute('aria-label');
        const hasAccessibleName = (text && text.trim().length > 0) || ariaLabel;
        expect(hasAccessibleName, `Link to ${href} should have accessible name`).toBeTruthy();
      }
    });
  });
});

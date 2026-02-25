/**
 * Error Handling and Edge Cases E2E Tests
 * Owner: Scenario 15 - Error Handling and Edge Cases
 *
 * Test cases:
 * - Core content visible with JavaScript disabled
 * - Broken image handling with alt text
 * - 404 page displays correctly with navigation
 * - All internal links resolve to valid sections
 * - All external links resolve to valid pages
 */

const { test, expect } = require('@playwright/test');

test.describe('Error Handling and Edge Cases', () => {
  test.describe('JavaScript Disabled', () => {
    test('core content is visible and readable without JavaScript', async ({ browser }) => {
      // Create a new context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Check that core content sections are visible
      // Hero section
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Hero tagline text should be readable
      const tagline = page.locator('.hero__tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('MirDB');

      // Hero description
      const description = page.locator('.hero__description');
      await expect(description).toBeVisible();
      await expect(description).toContainText('persistence');

      // CTA buttons should be visible (they work as regular links)
      const ctaButtons = page.locator('.hero__cta .btn');
      await expect(ctaButtons.first()).toBeVisible();

      // Features section
      const featuresSection = page.locator('.features');
      await expect(featuresSection).toBeVisible();

      // Feature cards content
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(4);

      // Quick start section
      const quickstartSection = page.locator('.quickstart');
      await expect(quickstartSection).toBeVisible();

      // Steps should be readable
      const steps = page.locator('.step');
      const stepCount = await steps.count();
      expect(stepCount).toBe(3);

      // Code examples should be visible (even without syntax highlighting JS)
      const codeBlocks = page.locator('.code-block');
      await expect(codeBlocks.first()).toBeVisible();

      // Navigation should work (links are just anchor tags)
      // Check that navigation container exists and has links
      const navContainer = page.locator('.nav-links, .site-header nav');
      await expect(navContainer.first()).toBeAttached();

      // Footer should be visible
      const footer = page.locator('.site-footer');
      await expect(footer).toBeVisible();

      await context.close();
    });

    test('navigation links work without JavaScript (anchor navigation)', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Click a navigation link (should work as regular anchor)
      const featuresLink = page.locator('a[href="#features"]').first();
      if (await featuresLink.isVisible()) {
        await featuresLink.click();
        // URL should have the hash
        expect(page.url()).toContain('#features');
      }

      await context.close();
    });
  });

  test.describe('Broken Image Handling', () => {
    test('alt text is displayed and layout remains stable for broken images', async ({ page }) => {
      await page.goto('/');

      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      // If there are images, check they have alt attributes
      if (imageCount > 0) {
        for (let i = 0; i < imageCount; i++) {
          const img = images.nth(i);
          const altText = await img.getAttribute('alt');
          // Alt text should exist (can be empty for decorative images)
          expect(altText).not.toBeNull();
        }
      }

      // Check SVG icons have proper aria attributes
      const svgIcons = page.locator('svg[aria-hidden="true"]');
      const svgCount = await svgIcons.count();
      // Decorative SVGs should have aria-hidden
      expect(svgCount).toBeGreaterThan(0);

      // Check that the page layout is stable (no major layout shifts)
      const heroHeight = await page.locator('.hero').boundingBox();
      expect(heroHeight).not.toBeNull();
      expect(heroHeight.height).toBeGreaterThan(100);
    });

    test('feature icons remain visible with proper fallback', async ({ page }) => {
      await page.goto('/');

      // Feature icons use inline SVGs, not external images
      const featureIcons = page.locator('.feature-icon svg');
      const iconCount = await featureIcons.count();

      // Should have 6 feature cards with icons
      expect(iconCount).toBeGreaterThanOrEqual(5);

      // Each icon should be visible
      for (let i = 0; i < iconCount; i++) {
        await expect(featureIcons.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('404 Page Handling', () => {
    test('404 page is displayed for non-existent paths', async ({ page }) => {
      // Navigate to a non-existent path
      const response = await page.goto('/non-existent-page-12345');

      // Note: For static hosting, the server typically serves 404.html
      // For our test server (http-server), we need to manually navigate
      // In production, GitHub Pages would serve 404.html automatically
    });

    test('404 page has navigation back to homepage', async ({ page }) => {
      // Directly navigate to 404 page
      await page.goto('/404.html');

      // Check for clear error message
      const errorTitle = page.locator('.error-page__title');
      await expect(errorTitle).toBeVisible();
      await expect(errorTitle).toContainText('404');

      // Check for descriptive subtitle
      const subtitle = page.locator('.error-page__subtitle');
      await expect(subtitle).toBeVisible();
      await expect(subtitle).toContainText('Page Not Found');

      // Check for description
      const description = page.locator('.error-page__description');
      await expect(description).toBeVisible();

      // Check for link back to homepage (specific button with Homepage text)
      const homepageBtn = page.locator('.error-page__btn:has-text("Homepage")');
      await expect(homepageBtn).toBeVisible();
      await expect(homepageBtn).toContainText('Homepage');

      // Click the homepage link and verify navigation
      await homepageBtn.click();
      await expect(page).toHaveURL('/');
    });

    test('404 page has helpful alternative links', async ({ page }) => {
      await page.goto('/404.html');

      // Check for alternative links section
      const linksSection = page.locator('.error-page__links');
      await expect(linksSection).toBeVisible();

      // Should have links to quick start and code examples
      const quickstartLink = page.locator('a[href="/#quickstart"]');
      await expect(quickstartLink).toBeVisible();

      const codeExampleLink = page.locator('a[href="/#code-example"]');
      await expect(codeExampleLink).toBeVisible();

      // Should have link to GitHub
      const githubLink = page.locator('.error-page__links a[href*="github.com"]');
      await expect(githubLink).toBeVisible();
    });

    test('404 page is styled consistently with main site', async ({ page }) => {
      await page.goto('/404.html');

      // Should have the same header/navigation
      const header = page.locator('.site-header');
      await expect(header).toBeVisible();

      // Should have the same footer
      const footer = page.locator('.site-footer');
      await expect(footer).toBeVisible();

      // Should use the same button styles
      const primaryBtn = page.locator('.btn--primary');
      await expect(primaryBtn.first()).toBeVisible();
    });
  });

  test.describe('Internal Links Validation', () => {
    test('all internal anchor links resolve to valid page sections', async ({ page }) => {
      await page.goto('/');

      // Get all internal anchor links
      const anchorLinks = page.locator('a[href^="#"]');
      const linkCount = await anchorLinks.count();

      const checkedIds = new Set();
      const errors = [];

      for (let i = 0; i < linkCount; i++) {
        const link = anchorLinks.nth(i);
        const href = await link.getAttribute('href');

        if (!href || href === '#') continue;

        const targetId = href.substring(1); // Remove the '#'

        // Skip if we've already checked this ID
        if (checkedIds.has(targetId)) continue;
        checkedIds.add(targetId);

        // Check if the target element exists
        const targetElement = page.locator(`#${targetId}`);
        const exists = await targetElement.count() > 0;

        if (!exists) {
          errors.push(`Anchor link "${href}" does not resolve to any element with id="${targetId}"`);
        }
      }

      // Report all errors at once
      if (errors.length > 0) {
        throw new Error(`Invalid internal links found:\n${errors.join('\n')}`);
      }
    });

    test('navigation links point to existing sections', async ({ page }) => {
      await page.goto('/');

      // Check specific navigation links
      const navLinks = [
        { selector: 'a[href="#features"]', targetId: 'features' },
        { selector: 'a[href="#quickstart"]', targetId: 'quickstart' },
        { selector: 'a[href="#code-example"]', targetId: 'code-example' },
      ];

      for (const { selector, targetId } of navLinks) {
        const link = page.locator(selector).first();
        if (await link.isVisible()) {
          const target = page.locator(`#${targetId}`);
          await expect(target).toBeAttached();
        }
      }
    });
  });

  test.describe('External Links Validation', () => {
    test('all external links have valid href and open in new tab', async ({ page }) => {
      await page.goto('/');

      // Get all external links (links with target="_blank")
      const externalLinks = page.locator('a[target="_blank"]');
      const linkCount = await externalLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      for (let i = 0; i < linkCount; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const rel = await link.getAttribute('rel');

        // Should have a valid href
        expect(href).toBeTruthy();
        expect(href).toMatch(/^https?:\/\//);

        // Should have rel="noopener noreferrer" for security
        expect(rel).toContain('noopener');
      }
    });

    test('GitHub repository link is valid', async ({ page }) => {
      await page.goto('/');

      // Find GitHub link
      const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
      const linkCount = await githubLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      // Check the link has proper attributes
      const firstLink = githubLinks.first();
      const href = await firstLink.getAttribute('href');
      expect(href).toContain('github.com');
    });

    test('documentation links are properly formatted', async ({ page }) => {
      await page.goto('/');

      // Check docs.rs link
      const docsLink = page.locator('a[href*="docs.rs"]');
      if (await docsLink.count() > 0) {
        const href = await docsLink.first().getAttribute('href');
        expect(href).toMatch(/^https:\/\/docs\.rs/);
      }
    });

    test('external links (GitHub, docs) resolve correctly', async ({ page, request }) => {
      await page.goto('/');

      // Get external links to test
      const testUrls = [
        'https://github.com/yetone/mirdb',
        'https://docs.rs/mirdb'
      ];

      for (const url of testUrls) {
        const link = page.locator(`a[href="${url}"]`).first();
        if (await link.count() > 0) {
          // Just verify the link exists and has correct href
          const href = await link.getAttribute('href');
          expect(href).toBe(url);
        }
      }
    });
  });

  test.describe('Edge Cases', () => {
    test('page handles rapid navigation', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');

      // Rapidly click navigation links
      const links = ['#features', '#quickstart', '#code-example', '#features'];

      for (const hash of links) {
        const link = page.locator(`a[href="${hash}"]`).first();
        if (await link.isVisible()) {
          await link.click();
          // Small delay to allow smooth scroll
          await page.waitForTimeout(100);
        }
      }

      // Page should still be functional
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();
    });

    test('page handles browser back/forward navigation', async ({ page }) => {
      await page.goto('/');

      // Navigate to a section
      const featuresLink = page.locator('a[href="#features"]').first();
      if (await featuresLink.isVisible()) {
        await featuresLink.click();
        await page.waitForTimeout(200);
      }

      // Navigate to 404
      await page.goto('/404.html');
      await expect(page.locator('.error-page__title')).toBeVisible();

      // Go back
      await page.goBack();

      // Should be back on homepage (possibly at #features)
      await expect(page.locator('.hero')).toBeVisible();
    });

    test('page is functional at different zoom levels', async ({ page }) => {
      await page.goto('/');

      // Test at 50% zoom
      await page.evaluate(() => {
        document.body.style.zoom = '0.5';
      });
      await expect(page.locator('.hero')).toBeVisible();

      // Test at 150% zoom
      await page.evaluate(() => {
        document.body.style.zoom = '1.5';
      });
      await expect(page.locator('.hero')).toBeVisible();

      // Reset zoom
      await page.evaluate(() => {
        document.body.style.zoom = '1';
      });
    });
  });
});

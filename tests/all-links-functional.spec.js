const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for All Links Functional Scenario
 * Verifies all links on the page are functional and not broken
 * Covers: href validation, internal anchor links, external link responses, and security attributes
 */
test.describe('All Links Functional', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: All links have valid href attributes', () => {
    test('TC1.1: All anchor elements have non-empty href attributes', async ({ page }) => {
      // Get all anchor elements on the page
      const allLinks = page.locator('a');
      const linkCount = await allLinks.count();

      // Verify there are links on the page
      expect(linkCount).toBeGreaterThan(0);

      // Check each link has a non-empty href
      for (let i = 0; i < linkCount; i++) {
        const link = allLinks.nth(i);
        const href = await link.getAttribute('href');

        // Verify href exists and is not empty
        expect(href, `Link ${i + 1} should have a non-empty href`).toBeTruthy();
        expect(href.length, `Link ${i + 1} href should not be empty`).toBeGreaterThan(0);
      }
    });

    test('TC1.2: No links have href="#" or href="javascript:"', async ({ page }) => {
      // Check for invalid placeholder hrefs
      const invalidHashLinks = page.locator('a[href="#"]');
      const invalidJsLinks = page.locator('a[href^="javascript:"]');

      // Verify no placeholder links exist
      await expect(invalidHashLinks).toHaveCount(0);
      await expect(invalidJsLinks).toHaveCount(0);
    });

    test('TC1.3: All links are clickable elements', async ({ page }) => {
      const allLinks = page.locator('a');
      const linkCount = await allLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = allLinks.nth(i);
        const isEnabled = await link.isEnabled();
        expect(isEnabled, `Link ${i + 1} should be enabled`).toBe(true);
      }
    });
  });

  test.describe('Test Case 2: Internal anchor links point to existing element IDs', () => {
    test('TC2.1: Features anchor link points to existing section', async ({ page }) => {
      // Verify the anchor link exists
      const featuresLink = page.locator('a[href="#features"]');
      await expect(featuresLink.first()).toBeVisible();

      // Verify the target element exists
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });

    test('TC2.2: Quick Start anchor link points to existing section', async ({ page }) => {
      // Verify the anchor link exists
      const quickstartLink = page.locator('a[href="#quickstart"]');
      await expect(quickstartLink.first()).toBeVisible();

      // Verify the target element exists
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();
    });

    test('TC2.3: Commands anchor link points to existing section', async ({ page }) => {
      // Verify the anchor link exists
      const commandsLink = page.locator('a[href="#commands"]');
      await expect(commandsLink.first()).toBeVisible();

      // Verify the target element exists
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();
    });

    test('TC2.4: All internal anchor links have corresponding target elements', async ({ page }) => {
      // Get all internal anchor links (starting with #)
      const anchorLinks = page.locator('a[href^="#"]');
      const anchorCount = await anchorLinks.count();

      // Verify each anchor link has a matching element
      for (let i = 0; i < anchorCount; i++) {
        const link = anchorLinks.nth(i);
        const href = await link.getAttribute('href');

        // Extract the ID from the href (remove the # prefix)
        const targetId = href.substring(1);

        // Verify the target element exists on the page
        const targetElement = page.locator(`#${targetId}`);
        await expect(targetElement, `Target element for ${href} should exist`).toBeVisible();
      }
    });

    test('TC2.5: Clicking internal anchor links navigates to correct section', async ({ page }) => {
      // Test Features link navigation
      const featuresLink = page.locator('header .nav-links a[href="#features"]');
      await featuresLink.click();
      await page.waitForTimeout(500);

      // Verify URL hash changed
      expect(page.url()).toContain('#features');

      // Verify features section is visible and in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Check if the section is in viewport by checking its bounding box
      const boundingBox = await featuresSection.boundingBox();
      const viewportSize = page.viewportSize();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox.y).toBeLessThan(viewportSize.height);
    });
  });

  test.describe('Test Case 3: External links return 200 status', () => {
    test('TC3.1: GitHub repository main link returns successful response', async ({ page, context }) => {
      // Get the GitHub link
      const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]').first();
      const href = await githubLink.getAttribute('href');

      // Make request to verify it's not a 404
      const response = await context.request.get(href);
      expect(response.status()).not.toBe(404);
      expect(response.ok()).toBe(true);
    });

    test('TC3.2: Documentation link returns successful response', async ({ page, context }) => {
      // Get the documentation link
      const docsLink = page.locator('a[href="https://github.com/yetone/mirdb#readme"]').first();
      const href = await docsLink.getAttribute('href');

      // Make request to verify it's not a 404
      const response = await context.request.get(href);
      expect(response.status()).not.toBe(404);
      expect(response.ok()).toBe(true);
    });

    test('TC3.3: Issues link returns successful response', async ({ page, context }) => {
      // Get the issues link
      const issuesLink = page.locator('a[href="https://github.com/yetone/mirdb/issues"]').first();
      const href = await issuesLink.getAttribute('href');

      // Make request to verify it's not a 404
      const response = await context.request.get(href);
      expect(response.status()).not.toBe(404);
      expect(response.ok()).toBe(true);
    });

    test('TC3.4: All external links return successful HTTP responses', async ({ page, context }) => {
      // Get all external links (starting with http:// or https://)
      const externalLinks = page.locator('a[href^="http"]');
      const linkCount = await externalLinks.count();

      // Collect unique URLs
      const uniqueUrls = new Set();
      for (let i = 0; i < linkCount; i++) {
        const href = await externalLinks.nth(i).getAttribute('href');
        uniqueUrls.add(href);
      }

      // Verify each unique external URL returns a successful response
      for (const url of uniqueUrls) {
        const response = await context.request.get(url);
        expect(response.status(), `External link ${url} should not return 404`).not.toBe(404);
        expect(response.ok(), `External link ${url} should return successful response`).toBe(true);
      }
    });
  });

  test.describe('Test Case 4: External links have target="_blank" and rel="noopener"', () => {
    test('TC4.1: External links open in new tab with target="_blank"', async ({ page }) => {
      // Get all external links
      const externalLinks = page.locator('a[href^="http"]');
      const linkCount = await externalLinks.count();

      // Verify each external link has target="_blank"
      for (let i = 0; i < linkCount; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');

        expect(target, `External link ${href} should have target="_blank"`).toBe('_blank');
      }
    });

    test('TC4.2: External links have rel="noopener" for security', async ({ page }) => {
      // Get all external links with target="_blank"
      const externalLinks = page.locator('a[href^="http"][target="_blank"]');
      const linkCount = await externalLinks.count();

      // Verify each external link has rel containing "noopener"
      for (let i = 0; i < linkCount; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const rel = await link.getAttribute('rel');

        expect(rel, `External link ${href} should have rel attribute`).toBeTruthy();
        expect(rel, `External link ${href} should contain "noopener"`).toContain('noopener');
      }
    });

    test('TC4.3: Navigation external links have security attributes', async ({ page }) => {
      // Get navigation external links (Docs and GitHub)
      const navExternalLinks = page.locator('header .nav-links a[href^="http"]');
      const linkCount = await navExternalLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = navExternalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        expect(target, `Nav link ${href} should have target="_blank"`).toBe('_blank');
        expect(rel, `Nav link ${href} should have rel="noopener"`).toContain('noopener');
      }
    });

    test('TC4.4: Footer external links have security attributes', async ({ page }) => {
      // Get footer external links
      const footerExternalLinks = page.locator('footer .footer-links a[href^="http"]');
      const linkCount = await footerExternalLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = footerExternalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        expect(target, `Footer link ${href} should have target="_blank"`).toBe('_blank');
        expect(rel, `Footer link ${href} should have rel="noopener"`).toContain('noopener');
      }
    });

    test('TC4.5: Hero CTA external link has security attributes', async ({ page }) => {
      // Get primary CTA button (GitHub link)
      const primaryCTA = page.locator('#primary-cta');
      const href = await primaryCTA.getAttribute('href');
      const target = await primaryCTA.getAttribute('target');
      const rel = await primaryCTA.getAttribute('rel');

      expect(target, `Primary CTA should have target="_blank"`).toBe('_blank');
      expect(rel, `Primary CTA should have rel="noopener"`).toContain('noopener');
    });
  });

  test.describe('Comprehensive link validation', () => {
    test('Summary: Total link count and types verification', async ({ page }) => {
      // Count all links
      const allLinks = page.locator('a');
      const totalLinks = await allLinks.count();

      // Count internal anchor links
      const anchorLinks = page.locator('a[href^="#"]');
      const anchorCount = await anchorLinks.count();

      // Count external links
      const externalLinks = page.locator('a[href^="http"]');
      const externalCount = await externalLinks.count();

      // Log counts for verification
      console.log(`Total links: ${totalLinks}`);
      console.log(`Internal anchor links: ${anchorCount}`);
      console.log(`External links: ${externalCount}`);

      // Verify we have expected link types
      expect(totalLinks).toBeGreaterThan(0);
      expect(anchorCount).toBeGreaterThan(0);
      expect(externalCount).toBeGreaterThan(0);
    });
  });
});

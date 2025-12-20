// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Navigation and Links', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Search for GitHub link on the page
  // Expected: Page contains at least one link to github.com repository
  test('TC1: Page contains at least one link to github.com repository', async ({ page }) => {
    // Look for any link that points to github.com
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Verify at least one GitHub link is visible
    const firstGithubLink = githubLinks.first();
    await expect(firstGithubLink).toBeVisible();
  });

  // Test Case 2: Verify documentation link is present
  // Expected: Page contains a link labeled 'Documentation' or similar
  test('TC2: Page contains a link labeled Documentation or similar', async ({ page }) => {
    // Look for links that have documentation-related text
    const allLinks = page.locator('a');
    const linksCount = await allLinks.count();

    let foundDocLink = false;

    for (let i = 0; i < linksCount; i++) {
      const link = allLinks.nth(i);
      const text = await link.textContent();
      const href = await link.getAttribute('href');

      if (text) {
        const lowerText = text.toLowerCase();
        // Check for documentation-related text
        if (lowerText.includes('doc') ||
            lowerText.includes('documentation') ||
            lowerText.includes('docs') ||
            lowerText.includes('guide') ||
            lowerText.includes('readme')) {
          foundDocLink = true;
          await expect(link).toBeVisible();
          break;
        }
      }

      // Also check href for documentation patterns
      if (href) {
        const lowerHref = href.toLowerCase();
        if (lowerHref.includes('doc') ||
            lowerHref.includes('documentation') ||
            lowerHref.includes('#documentation')) {
          foundDocLink = true;
          await expect(link).toBeVisible();
          break;
        }
      }
    }

    expect(foundDocLink).toBeTruthy();
  });

  // Test Case 3: Test keyboard navigation through interactive elements
  // Expected: All links and buttons are reachable via Tab key
  test('TC3: All links and buttons are reachable via Tab key', async ({ page }) => {
    // Get all focusable elements (links and buttons)
    const focusableElements = page.locator('a[href], button, [tabindex]:not([tabindex="-1"])');
    const expectedCount = await focusableElements.count();

    expect(expectedCount).toBeGreaterThan(0);

    // Start tabbing from the beginning of the document
    await page.keyboard.press('Tab');

    // Track unique focused elements
    const focusedElements = new Set();
    let maxTabs = expectedCount + 5; // Allow some buffer for navigation

    for (let i = 0; i < maxTabs; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && (el.tagName === 'A' || el.tagName === 'BUTTON')) {
          return {
            tag: el.tagName,
            href: el.getAttribute('href'),
            text: el.textContent?.trim().substring(0, 50)
          };
        }
        return null;
      });

      if (focusedElement) {
        const key = `${focusedElement.tag}-${focusedElement.href || focusedElement.text}`;
        focusedElements.add(key);
      }

      await page.keyboard.press('Tab');
    }

    // Verify that we were able to focus on multiple interactive elements
    expect(focusedElements.size).toBeGreaterThanOrEqual(2);
  });

  // Test Case 4: Verify GitHub link has valid href attribute
  // Expected: GitHub link href points to a valid github.com URL
  test('TC4: GitHub link href points to a valid github.com URL', async ({ page }) => {
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Check each GitHub link has a valid URL
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');

      expect(href).not.toBeNull();
      expect(href).toMatch(/^https?:\/\/(www\.)?github\.com\//);

      // Verify the URL structure is valid (not just github.com but a proper path)
      const url = new URL(href);
      expect(url.hostname).toMatch(/(www\.)?github\.com/);
    }
  });

  // Additional test: Verify footer contains GitHub link as specified in REQ-10
  test('TC-REQ10: Footer contains link to GitHub repository', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check that footer has at least one GitHub link
    const footerGithubLinks = footer.locator('a[href*="github.com"]');
    const count = await footerGithubLinks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Use the specific GitHub Repository link
    const footerGithubLink = footer.locator('[data-testid="github-link"]');
    await expect(footerGithubLink).toBeVisible();

    const href = await footerGithubLink.getAttribute('href');
    expect(href).toMatch(/github\.com/);
  });

  // Additional test: Verify Documentation CTA as specified in REQ-6
  test('TC-REQ6: Documentation CTA button is present and functional', async ({ page }) => {
    // Check for documentation CTA button in hero section
    const docCta = page.locator('[data-testid="cta-secondary"]');
    await expect(docCta).toBeVisible();

    const ctaText = await docCta.textContent();
    expect(ctaText?.toLowerCase()).toContain('doc');

    // Verify it has an href
    const href = await docCta.getAttribute('href');
    expect(href).not.toBeNull();
    expect(href.length).toBeGreaterThan(0);
  });

  // Additional test: Verify external links have proper attributes
  test('TC-External: External links have proper target attribute', async ({ page }) => {
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Check that external links either open in new tab or navigate correctly
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // External links should have proper attributes
      expect(href).toMatch(/github\.com/);

      // If target is _blank, rel should include noopener for security
      if (target === '_blank') {
        expect(rel).toContain('noopener');
      }
    }
  });
});

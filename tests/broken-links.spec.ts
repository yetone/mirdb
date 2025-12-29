import { test, expect } from '@playwright/test';

test.describe('Error Handling - Broken Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('all internal anchor links reference existing element IDs', async ({ page }) => {
    // Test Case 1: Scan page for anchor links
    // Expected: All internal anchor links (#section) reference existing element IDs

    // Collect all anchor links with hash references
    const internalLinks = await page.locator('a[href^="#"]').all();

    // Skip if no internal links found
    if (internalLinks.length === 0) {
      // No internal links to validate
      return;
    }

    const missingTargets: string[] = [];

    for (const link of internalLinks) {
      const href = await link.getAttribute('href');

      // Skip empty or '#' only hrefs (handled in separate test)
      if (!href || href === '#') {
        continue;
      }

      // Extract the target ID from the href
      const targetId = href.substring(1);

      // Check if an element with this ID exists on the page
      const targetElement = page.locator(`#${targetId}`);
      const count = await targetElement.count();

      if (count === 0) {
        missingTargets.push(href);
      }
    }

    // All internal links should point to existing IDs
    expect(missingTargets).toEqual([]);
  });

  test('GitHub repository links return HTTP 200', async ({ request }) => {
    // Test Case 2: Verify GitHub links are valid
    // Expected: GitHub repository links return HTTP 200

    // The MirDB GitHub repository URL
    const githubUrl = 'https://github.com/yetone/mirdb';

    // Make a HEAD request to check if the link is valid
    const response = await request.head(githubUrl);

    // GitHub should return 200 for valid repositories
    expect(response.status()).toBe(200);
  });

  test('no anchor elements have empty or # only href values', async ({ page }) => {
    // Test Case 3: Check for empty href attributes
    // Expected: No anchor elements have empty or '#' only href values

    // Find all anchor elements
    const allLinks = await page.locator('a').all();

    const invalidLinks: { text: string; href: string | null }[] = [];

    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Check for empty href or '#' only href
      if (href === '' || href === '#') {
        invalidLinks.push({
          text: text?.trim() || '[no text]',
          href: href
        });
      }
    }

    // Report any invalid links found
    if (invalidLinks.length > 0) {
      const errorMessage = invalidLinks
        .map(l => `Link "${l.text}" has invalid href: "${l.href}"`)
        .join('\n');
      expect(invalidLinks.length, `Found invalid links:\n${errorMessage}`).toBe(0);
    }

    expect(invalidLinks).toEqual([]);
  });

  test('all navigation links point to existing sections', async ({ page }) => {
    // Additional validation: Check that navigation links are valid
    const navLinks = await page.locator('.site-nav .nav-link').all();

    expect(navLinks.length).toBeGreaterThan(0);

    for (const link of navLinks) {
      const href = await link.getAttribute('href');

      // All nav links should be internal hash links
      expect(href).toMatch(/^#/);

      if (href) {
        const targetId = href.substring(1);
        const targetElement = page.locator(`#${targetId}`);
        await expect(targetElement, `Navigation link ${href} should point to existing element`).toHaveCount(1);
      }
    }
  });

  test('all links have non-null href attributes', async ({ page }) => {
    // Additional validation: Ensure all anchor elements have href attributes
    const anchorsWithoutHref = await page.locator('a:not([href])').all();

    expect(anchorsWithoutHref.length, 'All anchor elements should have href attributes').toBe(0);
  });
});

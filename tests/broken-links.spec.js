// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Broken Links and Error Handling E2E Tests
 * Verifies the page handles potential link errors gracefully
 * - All internal anchor links resolve to valid section IDs
 * - External links have proper security attributes (target="_blank" rel="noopener noreferrer")
 * - No broken internal links exist
 */

/**
 * Escapes special characters in CSS selectors
 * @param {string} id - The ID to escape
 * @returns {string} - The escaped ID
 */
function escapeCssSelector(id) {
  return id.replace(/([!"#$%&'()*+,.\/:;<=>?@[\\\]^`{|}~])/g, '\\$1');
}

test.describe('Error Handling - Broken Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: All anchor links point to existing sections
   * Input: Check all anchor links point to existing sections
   * Expected: All internal anchor links resolve to valid section IDs
   */
  test('TC1: All internal anchor links resolve to valid section IDs', async ({ page }) => {
    // Get all internal anchor links (hrefs starting with #)
    const anchorLinks = page.locator('a[href^="#"]');
    const count = await anchorLinks.count();

    // Ensure we have anchor links to test
    expect(count).toBeGreaterThan(0);

    // Collect all anchor link hrefs and verify each has a matching element
    const brokenLinks = [];

    for (let i = 0; i < count; i++) {
      const link = anchorLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href !== '#') {
        // Extract the ID from the href (remove the # prefix)
        const targetId = href.substring(1);

        // Check if an element with this ID exists
        const targetElement = page.locator(`#${escapeCssSelector(targetId)}`);
        const exists = await targetElement.count();

        if (exists === 0) {
          const linkText = await link.textContent();
          brokenLinks.push({ href, linkText: linkText?.trim() });
        }
      }
    }

    // All anchor links should have matching target elements
    expect(brokenLinks).toEqual([]);
  });

  /**
   * Test Case 2: External links have security attributes
   * Input: Check external links have security attributes
   * Expected: External links have rel='noopener noreferrer'
   */
  test('TC2: External links have rel="noopener noreferrer" security attributes', async ({ page }) => {
    // Get all external links (links starting with http:// or https://)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    // Ensure we have external links to test
    expect(count).toBeGreaterThan(0);

    // Check each external link for proper security attributes
    const insecureLinks = [];

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');
      const target = await link.getAttribute('target');

      const hasNoopener = rel && rel.includes('noopener');
      const hasNoreferrer = rel && rel.includes('noreferrer');
      const hasBlankTarget = target === '_blank';

      if (!hasNoopener || !hasNoreferrer) {
        const linkText = await link.textContent();
        insecureLinks.push({
          href,
          linkText: linkText?.trim(),
          rel,
          target,
          issues: {
            missingNoopener: !hasNoopener,
            missingNoreferrer: !hasNoreferrer,
            missingBlankTarget: !hasBlankTarget
          }
        });
      }
    }

    // All external links should have proper security attributes
    expect(insecureLinks).toEqual([]);
  });

  /**
   * Test Case 3: Verify no broken internal links
   * Input: Verify no broken internal links
   * Expected: All href values starting with '#' have matching element IDs
   */
  test('TC3: All href values starting with "#" have matching element IDs', async ({ page }) => {
    // Get all links on the page
    const allLinks = page.locator('a');
    const count = await allLinks.count();

    const results = {
      totalLinks: count,
      internalLinks: 0,
      externalLinks: 0,
      brokenInternalLinks: [],
      validInternalLinks: []
    };

    for (let i = 0; i < count; i++) {
      const link = allLinks.nth(i);
      const href = await link.getAttribute('href');

      if (!href) continue;

      if (href.startsWith('#')) {
        results.internalLinks++;

        // Skip empty anchor (#)
        if (href === '#') {
          results.validInternalLinks.push({ href, note: 'Empty anchor (returns to top)' });
          continue;
        }

        // Extract target ID and check if element exists
        const targetId = href.substring(1);
        const targetElement = page.locator(`#${escapeCssSelector(targetId)}`);
        const exists = await targetElement.count();

        if (exists > 0) {
          results.validInternalLinks.push({ href, targetId });
        } else {
          const linkText = await link.textContent();
          results.brokenInternalLinks.push({
            href,
            targetId,
            linkText: linkText?.trim()
          });
        }
      } else if (href.startsWith('http://') || href.startsWith('https://')) {
        results.externalLinks++;
      }
    }

    // Log summary for debugging
    console.log(`Link validation summary:
      - Total links: ${results.totalLinks}
      - Internal links: ${results.internalLinks}
      - External links: ${results.externalLinks}
      - Valid internal links: ${results.validInternalLinks.length}
      - Broken internal links: ${results.brokenInternalLinks.length}`);

    // No broken internal links should exist
    expect(results.brokenInternalLinks).toEqual([]);

    // Ensure we actually tested internal links
    expect(results.internalLinks).toBeGreaterThan(0);
  });

  /**
   * Additional Test: External links open in new tab (target="_blank")
   */
  test('TC2-additional: External links have target="_blank" attribute', async ({ page }) => {
    // Get all external links
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    const linksWithoutBlankTarget = [];

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const target = await link.getAttribute('target');

      if (target !== '_blank') {
        const href = await link.getAttribute('href');
        const linkText = await link.textContent();
        linksWithoutBlankTarget.push({
          href,
          linkText: linkText?.trim(),
          target
        });
      }
    }

    // All external links should open in new tab
    expect(linksWithoutBlankTarget).toEqual([]);
  });

  /**
   * Additional Test: Navigation anchor links scroll to correct sections
   */
  test('TC1-additional: Navigation anchor links scroll to correct sections', async ({ page }) => {
    // Test each navigation anchor link
    const navAnchors = [
      { href: '#features', sectionId: 'features' },
      { href: '#quick-start', sectionId: 'quick-start' },
      { href: '#commands', sectionId: 'commands' },
      { href: '#configuration', sectionId: 'configuration' }
    ];

    for (const anchor of navAnchors) {
      // Find the navigation link
      const navLink = page.locator(`nav a[href="${anchor.href}"]`);

      // Verify link exists and is visible
      await expect(navLink).toBeVisible();

      // Click the link
      await navLink.click();

      // Wait for smooth scroll
      await page.waitForTimeout(500);

      // Verify the target section is in viewport
      const targetSection = page.locator(`#${anchor.sectionId}`);
      await expect(targetSection).toBeInViewport();

      // Scroll back to top for next test
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(200);
    }
  });

  /**
   * Additional Test: Hero CTA anchor link works correctly
   */
  test('TC1-additional2: Hero Get Started link scrolls to Quick Start section', async ({ page }) => {
    // Find the Get Started button in hero section
    const getStartedLink = page.locator('#hero a[href="#quick-start"]');

    // Verify link exists and is visible
    await expect(getStartedLink).toBeVisible();

    // Click the link
    await getStartedLink.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify quick-start section is in viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });
});

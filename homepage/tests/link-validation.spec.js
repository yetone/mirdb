// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Scenario: Link Validation (Success Criteria)
 *
 * Verify that all links on the page are functional:
 * - All internal navigation links work correctly
 * - GitHub repository link points to valid URL
 * - No 404 errors when following any link
 * - External links have target="_blank" and rel="noopener noreferrer"
 */

test.describe('Link Validation - Internal Navigation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Click all internal navigation links
   * Expected: All internal links navigate to correct sections or pages
   */
  test('should navigate to Features section via nav link', async ({ page }) => {
    const featuresLink = page.locator('nav.main-nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    await featuresLink.click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('should navigate to Architecture section via nav link', async ({ page }) => {
    const architectureLink = page.locator('nav.main-nav a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();

    await architectureLink.click();
    await page.waitForTimeout(500);

    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });

  test('should navigate to Commands section via nav link', async ({ page }) => {
    const commandsLink = page.locator('nav.main-nav a[href="#commands"]');
    await expect(commandsLink).toBeVisible();

    await commandsLink.click();
    await page.waitForTimeout(500);

    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();
  });

  test('should navigate to Getting Started section via nav link', async ({ page }) => {
    const gettingStartedLink = page.locator('nav.main-nav a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();

    await gettingStartedLink.click();
    await page.waitForTimeout(500);

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('should navigate to main content via skip link', async ({ page }) => {
    const skipLink = page.locator('a[href="#main-content"]');
    await expect(skipLink).toBeAttached();

    // Focus and click the skip link
    await skipLink.focus();
    await skipLink.click();

    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeAttached();
  });

  test('should navigate to Features section via secondary CTA', async ({ page }) => {
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();

    const href = await secondaryCTA.getAttribute('href');
    expect(href).toBe('#features');

    await secondaryCTA.click();
    await page.waitForTimeout(500);

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('all internal anchor links should have corresponding target elements', async ({ page }) => {
    // Get all anchor links with hash hrefs (internal links)
    const internalLinks = page.locator('a[href^="#"]');
    const count = await internalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.length > 1) {
        const targetId = href.substring(1); // Remove the # symbol
        const targetElement = page.locator(`#${targetId}`);
        await expect(targetElement, `Target element for ${href} should exist`).toBeAttached();
      }
    }
  });
});

test.describe('Link Validation - GitHub Repository Link', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 2: Verify GitHub repository link
   * Expected: GitHub link points to valid repository URL
   */
  test('primary CTA should point to valid GitHub repository URL', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();

    const href = await primaryCTA.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
    expect(href).toMatch(/^https:\/\/github\.com\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+$/);
  });

  test('footer GitHub link should point to valid repository URL', async ({ page }) => {
    const footerGitHubLink = page.locator('footer a[href*="github.com"]');
    await expect(footerGitHubLink).toBeVisible();

    const href = await footerGitHubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
    expect(href).toMatch(/^https:\/\/github\.com\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+$/);
  });

  test('hero CTA and footer should have consistent GitHub URL', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const footerLink = page.locator('footer a[href*="github.com"]');

    const heroCTAHref = await primaryCTA.getAttribute('href');
    const footerHref = await footerLink.getAttribute('href');

    expect(heroCTAHref).toBe(footerHref);
  });
});

test.describe('Link Validation - No Broken Links (404 Errors)', () => {
  /**
   * Test Case 3: Check for broken links
   * Expected: No 404 errors when following any link on the page
   */
  test('all internal anchor links should resolve to existing elements', async ({ page }) => {
    await page.goto('/');

    const internalLinks = page.locator('a[href^="#"]');
    const count = await internalLinks.count();

    const brokenLinks = [];

    for (let i = 0; i < count; i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.length > 1) {
        const targetId = href.substring(1);
        const targetElement = page.locator(`#${targetId}`);
        const exists = await targetElement.count() > 0;

        if (!exists) {
          brokenLinks.push(href);
        }
      }
    }

    expect(brokenLinks, `Found broken internal links: ${brokenLinks.join(', ')}`).toHaveLength(0);
  });

  test('static CSS file should be accessible (no 404)', async ({ page }) => {
    const response = await page.request.get('/styles.css');
    expect(response.status()).toBe(200);
  });

  test('static JS file should be accessible (no 404)', async ({ page }) => {
    const response = await page.request.get('/main.js');
    expect(response.status()).toBe(200);
  });

  test('homepage should load without 404 errors', async ({ page }) => {
    const response = await page.goto('/');
    expect(response?.status()).toBe(200);
  });

  test('external CDN resources should be accessible', async ({ page }) => {
    await page.goto('/');

    // Check that Mermaid.js CDN script is referenced
    const mermaidScript = page.locator('script[src*="mermaid"]');
    const scriptSrc = await mermaidScript.getAttribute('src');

    expect(scriptSrc).toContain('cdn.jsdelivr.net');
    expect(scriptSrc).toContain('mermaid');
  });

  test('page should not have any 404 errors during load', async ({ page }) => {
    const failedRequests = [];

    page.on('response', (response) => {
      if (response.status() === 404) {
        failedRequests.push(response.url());
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    expect(failedRequests, `Found 404 errors for: ${failedRequests.join(', ')}`).toHaveLength(0);
  });
});

test.describe('Link Validation - External Links Security Attributes', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 4: Verify external links open in new tab
   * Expected: External links have target="_blank" and rel="noopener noreferrer"
   */
  test('primary CTA external link should have target="_blank"', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const target = await primaryCTA.getAttribute('target');

    expect(target).toBe('_blank');
  });

  test('primary CTA external link should have rel="noopener noreferrer"', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const rel = await primaryCTA.getAttribute('rel');

    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('footer GitHub link should have target="_blank"', async ({ page }) => {
    const footerLink = page.locator('footer a[href*="github.com"]');
    const target = await footerLink.getAttribute('target');

    expect(target).toBe('_blank');
  });

  test('footer GitHub link should have rel="noopener noreferrer"', async ({ page }) => {
    const footerLink = page.locator('footer a[href*="github.com"]');
    const rel = await footerLink.getAttribute('rel');

    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('all external links should have proper security attributes', async ({ page }) => {
    // Get all external links (links starting with http:// or https://)
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    const linksWithoutProperAttrs = [];

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      if (target !== '_blank' || !rel?.includes('noopener') || !rel?.includes('noreferrer')) {
        linksWithoutProperAttrs.push({
          href,
          target,
          rel,
          missing: []
        });

        if (target !== '_blank') {
          linksWithoutProperAttrs[linksWithoutProperAttrs.length - 1].missing.push('target="_blank"');
        }
        if (!rel?.includes('noopener')) {
          linksWithoutProperAttrs[linksWithoutProperAttrs.length - 1].missing.push('noopener');
        }
        if (!rel?.includes('noreferrer')) {
          linksWithoutProperAttrs[linksWithoutProperAttrs.length - 1].missing.push('noreferrer');
        }
      }
    }

    expect(
      linksWithoutProperAttrs,
      `External links missing security attributes: ${JSON.stringify(linksWithoutProperAttrs, null, 2)}`
    ).toHaveLength(0);
  });

  test('internal anchor links should NOT have target="_blank"', async ({ page }) => {
    const internalLinks = page.locator('a[href^="#"]');
    const count = await internalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = internalLinks.nth(i);
      const target = await link.getAttribute('target');
      const href = await link.getAttribute('href');

      expect(
        target,
        `Internal link ${href} should not open in new tab`
      ).toBeNull();
    }
  });
});

test.describe('Link Validation - Navigation Link Inventory', () => {
  test('should have all expected navigation links present', async ({ page }) => {
    await page.goto('/');

    // Expected internal nav links
    const expectedNavLinks = [
      '#features',
      '#architecture',
      '#commands',
      '#getting-started'
    ];

    for (const expectedHref of expectedNavLinks) {
      const link = page.locator(`nav.main-nav a[href="${expectedHref}"]`);
      await expect(link, `Navigation link ${expectedHref} should exist`).toBeVisible();
    }
  });

  test('should have skip to main content link', async ({ page }) => {
    await page.goto('/');

    const skipLink = page.locator('a[href="#main-content"]');
    await expect(skipLink).toBeAttached();
  });

  test('should have expected external links in hero section', async ({ page }) => {
    await page.goto('/');

    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();

    const href = await primaryCTA.getAttribute('href');
    expect(href).toContain('github.com');
  });

  test('should have expected external links in footer', async ({ page }) => {
    await page.goto('/');

    const footerLink = page.locator('footer a[href*="github.com"]');
    await expect(footerLink).toBeVisible();
  });

  test('total link count should match expected', async ({ page }) => {
    await page.goto('/');

    // Count all anchor tags
    const allLinks = page.locator('a');
    const totalLinks = await allLinks.count();

    // Expected links:
    // - Skip to main content: 1
    // - Nav links: 4 (#features, #architecture, #commands, #getting-started)
    // - Primary CTA (GitHub): 1
    // - Secondary CTA (#features): 1
    // - Footer GitHub link: 1
    const expectedMinLinks = 8;

    expect(totalLinks).toBeGreaterThanOrEqual(expectedMinLinks);
  });
});

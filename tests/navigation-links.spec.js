// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Navigation and GitHub Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero CTA links to GitHub repository', async ({ page }) => {
    // Check that the primary CTA in the hero section links to GitHub
    const heroCta = page.locator('.hero a.btn-primary, .hero .cta-primary, .hero a[href*="github.com/yetone/mirdb"]').first();
    await expect(heroCta).toBeVisible();

    // Verify the href attribute points to the correct GitHub repository
    const href = await heroCta.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('TC2: Footer contains a link to the GitHub repository', async ({ page }) => {
    // Check that the footer exists
    const footer = page.locator('footer, [data-testid="footer"]').first();
    await expect(footer).toBeVisible();

    // Check that there's a GitHub link in the footer
    const footerGithubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGithubLink).toBeVisible();

    // Verify the href attribute points to the correct GitHub repository
    const href = await footerGithubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('TC3: GitHub links open in a new tab (target=_blank)', async ({ page }) => {
    // Get all GitHub links on the page
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const count = await githubLinks.count();

    // Ensure there are GitHub links
    expect(count).toBeGreaterThan(0);

    // Verify each GitHub link has target="_blank" for opening in a new tab
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // Check for target="_blank"
      expect(target).toBe('_blank');

      // Security best practice: external links should have rel="noopener noreferrer"
      expect(rel).toContain('noopener');
    }
  });

  test('TC4: All links on the page are valid (no broken links)', async ({ page, request }) => {
    // Get all links on the page
    const links = page.locator('a[href]');
    const count = await links.count();

    // Collect all hrefs
    const hrefs = [];
    for (let i = 0; i < count; i++) {
      const href = await links.nth(i).getAttribute('href');
      if (href) {
        hrefs.push(href);
      }
    }

    // Verify each external link
    for (const href of hrefs) {
      // Skip anchor links (internal page navigation)
      if (href.startsWith('#')) {
        // Verify anchor target exists on page
        const anchorId = href.substring(1);
        const target = page.locator(`#${anchorId}`);
        await expect(target).toBeAttached();
        continue;
      }

      // Skip mailto and tel links
      if (href.startsWith('mailto:') || href.startsWith('tel:')) {
        continue;
      }

      // For external links, verify they return a valid response (not 404)
      if (href.startsWith('http://') || href.startsWith('https://')) {
        const response = await request.head(href, { timeout: 10000 });
        expect(response.status(), `Link ${href} returned status ${response.status()}`).toBeLessThan(400);
      }
    }
  });
});

import { test, expect, request } from '@playwright/test';

test.describe('External Links Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: GitHub repository link resolves to valid repository page (HTTP 200)', async ({ page }) => {
    // Find all GitHub links on the page
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const count = await githubLinks.count();
    expect(count).toBeGreaterThan(0);

    // Get the GitHub URL
    const githubUrl = await githubLinks.first().getAttribute('href');
    expect(githubUrl).toBeTruthy();

    // Verify the GitHub link resolves to a valid page (HTTP 200)
    const apiContext = await request.newContext();
    const response = await apiContext.head(githubUrl!);
    expect(response.status()).toBe(200);
    await apiContext.dispose();
  });

  test('TC2: External links have target="_blank" attribute to open in new tab', async ({ page }) => {
    // Find all external links (links to external domains)
    const allLinks = await page.locator('a[href^="http"]').all();
    expect(allLinks.length).toBeGreaterThan(0);

    // Check each external link has target="_blank"
    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      const isExternal = href && !href.includes('localhost') && !href.includes('mirdb.io');

      if (isExternal) {
        const target = await link.getAttribute('target');
        expect(target, `External link ${href} should have target="_blank"`).toBe('_blank');
      }
    }
  });

  test('TC3: External links have rel="noopener noreferrer" or rel="noopener" security attributes', async ({ page }) => {
    // Find all external links with target="_blank"
    const externalLinks = await page.locator('a[href^="http"][target="_blank"]').all();
    expect(externalLinks.length).toBeGreaterThan(0);

    // Check each external link has proper security attributes
    for (const link of externalLinks) {
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');

      expect(rel, `External link ${href} should have rel attribute`).toBeTruthy();
      expect(
        rel?.includes('noopener'),
        `External link ${href} should have rel containing "noopener"`
      ).toBe(true);
    }
  });

  test('TC4: Check crates.io link if present', async ({ page }) => {
    // Check if there are any crates.io links on the page
    const cratesLinks = page.locator('a[href*="crates.io"]');
    const count = await cratesLinks.count();

    if (count > 0) {
      // Get the crates.io URL
      const cratesUrl = await cratesLinks.first().getAttribute('href');
      expect(cratesUrl).toBeTruthy();

      // Verify the crates.io link resolves to a valid page
      const apiContext = await request.newContext();
      const response = await apiContext.head(cratesUrl!);
      // crates.io should return 200 for valid packages
      expect(response.status()).toBe(200);
      await apiContext.dispose();
    } else {
      // If no crates.io link is present, the test passes (it's optional)
      test.skip(true, 'No crates.io links found on the page');
    }
  });

  test('All external links are identified correctly', async ({ page }) => {
    // Get all links on the page
    const allLinks = await page.locator('a').all();
    const externalLinks: string[] = [];

    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      if (href && href.startsWith('http') && !href.includes('localhost') && !href.includes('mirdb.io')) {
        externalLinks.push(href);
      }
    }

    // Verify we found external links
    expect(externalLinks.length).toBeGreaterThan(0);

    // Log the external links found for verification
    console.log('External links found:', externalLinks);

    // Verify all expected external links are present
    const hasGithubLink = externalLinks.some(link => link.includes('github.com'));
    expect(hasGithubLink, 'Should have at least one GitHub link').toBe(true);
  });

  test('GitHub links in navigation have proper security attributes', async ({ page }) => {
    // Specifically check the navigation GitHub link
    const navGithubLink = page.locator('[data-testid="nav-links"] a[href*="github"]');
    await expect(navGithubLink).toBeVisible();
    await expect(navGithubLink).toHaveAttribute('target', '_blank');
    await expect(navGithubLink).toHaveAttribute('rel', /noopener/);
  });

  test('GitHub links in hero section have proper security attributes', async ({ page }) => {
    // Check the hero section GitHub CTA
    const heroGithubLink = page.locator('[data-testid="cta-github"]');
    await expect(heroGithubLink).toBeVisible();
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    await expect(heroGithubLink).toHaveAttribute('rel', /noopener/);
    await expect(heroGithubLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
  });

  test('GitHub links in footer have proper security attributes', async ({ page }) => {
    // Check the footer GitHub link
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    await expect(footerGithubLink).toHaveAttribute('rel', /noopener/);
    await expect(footerGithubLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
  });

  test('Configuration documentation link has proper security attributes', async ({ page }) => {
    // Scroll to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Check the configuration documentation link
    const configDocLink = configSection.locator('a[href*="github.com"]');
    const count = await configDocLink.count();

    if (count > 0) {
      await expect(configDocLink.first()).toHaveAttribute('target', '_blank');
      await expect(configDocLink.first()).toHaveAttribute('rel', /noopener/);
    }
  });
});

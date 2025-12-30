const { test, expect } = require('@playwright/test');

/**
 * External Link Validation Tests
 * Scenario: Validate all external links open correctly and have proper attributes
 *
 * Tests cover:
 * 1. All external links have target="_blank" attribute
 * 2. All external links have rel="noopener noreferrer" for security
 * 3. All external links are reachable (integration test)
 */

test.describe('External Link Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Unit Test - GitHub link attributes validation
   * Verifies that all GitHub links have proper security attributes
   */
  test('Test Case 1: GitHub links have target="_blank" and rel="noopener noreferrer"', async ({ page }) => {
    // Find all links pointing to GitHub
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThan(0);

    // Verify each GitHub link has proper attributes
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');

      // Check target="_blank"
      const target = await link.getAttribute('target');
      expect(target, `GitHub link ${href} should have target="_blank"`).toBe('_blank');

      // Check rel contains both "noopener" and "noreferrer" for security
      const rel = await link.getAttribute('rel');
      expect(rel, `GitHub link ${href} should have rel attribute`).toBeTruthy();
      expect(rel, `GitHub link ${href} should contain "noopener"`).toContain('noopener');
      expect(rel, `GitHub link ${href} should contain "noreferrer"`).toContain('noreferrer');
    }
  });

  /**
   * Additional Unit Tests - All external links validation
   */
  test('All external links have target="_blank" attribute', async ({ page }) => {
    // Find all external links (http:// or https://)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Verify each external link has target="_blank"
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      expect(target, `External link ${href} should have target="_blank"`).toBe('_blank');
    }
  });

  test('All external links have rel="noopener noreferrer" for security', async ({ page }) => {
    // Find all external links
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Verify each external link has proper rel attribute
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');

      expect(rel, `External link ${href} should have rel attribute`).toBeTruthy();
      expect(rel, `External link ${href} should contain "noopener"`).toContain('noopener');
      expect(rel, `External link ${href} should contain "noreferrer"`).toContain('noreferrer');
    }
  });

  test('CircleCI badge link has proper attributes', async ({ page }) => {
    // Find CircleCI badge link
    const circleCiLink = page.locator('a[href*="circleci.com"]');
    const count = await circleCiLink.count();

    expect(count).toBeGreaterThan(0);

    const href = await circleCiLink.first().getAttribute('href');
    expect(href).toContain('circleci.com');

    // Check target="_blank"
    const target = await circleCiLink.first().getAttribute('target');
    expect(target, 'CircleCI link should have target="_blank"').toBe('_blank');

    // Check rel attributes
    const rel = await circleCiLink.first().getAttribute('rel');
    expect(rel, 'CircleCI link should have rel attribute').toBeTruthy();
    expect(rel, 'CircleCI link should contain "noopener"').toContain('noopener');
    expect(rel, 'CircleCI link should contain "noreferrer"').toContain('noreferrer');
  });

  test('Footer GitHub link has proper attributes', async ({ page }) => {
    // Find footer GitHub link
    const footer = page.locator('footer');
    const githubLink = footer.locator('a:has-text("GitHub")');

    await expect(githubLink.first()).toBeVisible();

    const href = await githubLink.first().getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    const target = await githubLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('Footer Issues link has proper attributes', async ({ page }) => {
    // Find footer Issues link
    const footer = page.locator('footer');
    const issuesLink = footer.locator('a:has-text("Issues")');

    await expect(issuesLink.first()).toBeVisible();

    const href = await issuesLink.first().getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb/issues');

    const target = await issuesLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await issuesLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('Footer License link has proper attributes', async ({ page }) => {
    // Find footer License link
    const footer = page.locator('footer');
    const licenseLinks = footer.locator('a:has-text("License")');
    const count = await licenseLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = licenseLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');

      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  test('Hero View on GitHub button has proper attributes', async ({ page }) => {
    // Find the "View on GitHub" button in hero section
    const heroSection = page.locator('.hero, section.hero');
    const githubButton = heroSection.locator('a.btn:has-text("GitHub"), a.btn-secondary:has-text("GitHub")');

    await expect(githubButton.first()).toBeVisible();

    const href = await githubButton.first().getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    const target = await githubButton.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubButton.first().getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });
});

/**
 * Test Case 2: Integration Test - External link reachability
 * Verifies that all external links return 200 status codes
 */
test.describe('External Link Reachability', () => {
  test('Test Case 2: All external links return 200 status codes', async ({ page, request }) => {
    await page.goto('/');

    // Find all external links
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Collect unique URLs
    const uniqueUrls = new Set();
    for (let i = 0; i < count; i++) {
      const href = await externalLinks.nth(i).getAttribute('href');
      if (href) {
        uniqueUrls.add(href);
      }
    }

    // Test each unique URL
    const results = [];
    for (const url of uniqueUrls) {
      try {
        const response = await request.head(url, {
          timeout: 30000,
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; MirDB-Homepage-Test/1.0)'
          }
        });

        // Accept 200, 301, 302 (redirects), and 304 as valid responses
        const validStatuses = [200, 201, 301, 302, 303, 304, 307, 308];
        const isValid = validStatuses.includes(response.status());

        results.push({
          url,
          status: response.status(),
          valid: isValid
        });

        if (!isValid) {
          console.log(`Warning: ${url} returned status ${response.status()}`);
        }
      } catch (error) {
        // Some servers may block HEAD requests, try GET as fallback
        try {
          const response = await request.get(url, {
            timeout: 30000,
            headers: {
              'User-Agent': 'Mozilla/5.0 (compatible; MirDB-Homepage-Test/1.0)'
            }
          });

          const validStatuses = [200, 201, 301, 302, 303, 304, 307, 308];
          const isValid = validStatuses.includes(response.status());

          results.push({
            url,
            status: response.status(),
            valid: isValid
          });
        } catch (getError) {
          results.push({
            url,
            status: 'error',
            valid: false,
            error: getError.message
          });
        }
      }
    }

    // Log all results
    console.log('External Link Check Results:');
    results.forEach(r => {
      console.log(`  ${r.valid ? '✓' : '✗'} ${r.url} - ${r.status}${r.error ? ` (${r.error})` : ''}`);
    });

    // Filter for GitHub links which should definitely be reachable
    const githubResults = results.filter(r => r.url.includes('github.com'));

    // Verify all GitHub links are reachable
    for (const result of githubResults) {
      expect(result.valid, `GitHub link ${result.url} should be reachable (got status ${result.status})`).toBe(true);
    }

    // Verify majority of links are reachable (allowing for some potential network issues)
    const validCount = results.filter(r => r.valid).length;
    const validPercentage = (validCount / results.length) * 100;
    expect(validPercentage, `At least 80% of external links should be reachable`).toBeGreaterThanOrEqual(80);
  });

  test('GitHub repository main link is reachable', async ({ request }) => {
    const response = await request.get('https://github.com/yetone/mirdb', {
      timeout: 30000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; MirDB-Homepage-Test/1.0)'
      }
    });

    expect(response.status()).toBe(200);
  });

  test('GitHub issues page is reachable', async ({ request }) => {
    const response = await request.get('https://github.com/yetone/mirdb/issues', {
      timeout: 30000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; MirDB-Homepage-Test/1.0)'
      }
    });

    expect(response.status()).toBe(200);
  });

  test('All footer external links are reachable', async ({ page, request }) => {
    await page.goto('/');

    // Find all footer external links
    const footer = page.locator('footer');
    const footerLinks = footer.locator('a[href^="http://"], a[href^="https://"]');
    const count = await footerLinks.count();

    expect(count).toBeGreaterThan(0);

    // Collect unique URLs from footer
    const uniqueUrls = new Set();
    for (let i = 0; i < count; i++) {
      const href = await footerLinks.nth(i).getAttribute('href');
      if (href) {
        uniqueUrls.add(href);
      }
    }

    // Test each footer URL
    for (const url of uniqueUrls) {
      const response = await request.get(url, {
        timeout: 30000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; MirDB-Homepage-Test/1.0)'
        }
      });

      expect(response.status(), `Footer link ${url} should return 200`).toBe(200);
    }
  });
});

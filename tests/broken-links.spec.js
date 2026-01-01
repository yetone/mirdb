const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Broken Links Tests
 *
 * This test suite verifies all internal and external links are valid and functional.
 * It covers:
 * - Internal anchor links (section navigation)
 * - External links (GitHub, docs, crates.io)
 * - Automated link checking for 4xx/5xx errors
 */

test.describe('Error Handling - Broken Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Internal Anchor Links', () => {
    test('TC1: All internal anchor links navigate to correct sections', async ({ page }) => {
      // Define all internal anchor links and their target sections
      const internalLinks = [
        { href: '#features', sectionId: 'features' },
        { href: '#architecture', sectionId: 'architecture' },
        { href: '#commands', sectionId: 'commands' },
        { href: '#quickstart', sectionId: 'quickstart' },
        { href: '#configuration', sectionId: 'configuration' },
      ];

      for (const link of internalLinks) {
        // Find all anchor links with this href
        const anchors = page.locator(`a[href="${link.href}"]`);
        const count = await anchors.count();

        // Verify at least one link exists for each section
        expect(count).toBeGreaterThan(0);

        // Click the first visible link
        const firstVisibleLink = anchors.first();
        await firstVisibleLink.click();

        // Wait for scroll animation
        await page.waitForTimeout(300);

        // Verify target section exists
        const targetSection = page.locator(`#${link.sectionId}`);
        await expect(targetSection).toBeVisible();

        // Verify section is in viewport after clicking
        await expect(targetSection).toBeInViewport({ ratio: 0.3 });

        // Scroll back to top for next test
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(100);
      }
    });

    test('TC1a: Hero Get Started button links to quickstart section', async ({ page }) => {
      const getStartedBtn = page.locator('.hero-buttons a[href="#quickstart"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveText('Get Started');

      // Click and verify navigation
      await getStartedBtn.click();
      await page.waitForTimeout(300);

      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport({ ratio: 0.3 });
    });

    test('TC1b: Mobile menu internal links work correctly', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.waitForTimeout(100);

      // Open mobile menu
      const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await mobileToggle.click();

      // Verify mobile menu is visible
      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).toHaveClass(/active/);

      // Click on a mobile menu link
      const featuresLink = mobileMenu.locator('a[href="#features"]');
      await featuresLink.click();

      // Verify navigation occurred
      await page.waitForTimeout(300);
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport({ ratio: 0.3 });

      // Verify mobile menu closed after clicking
      await expect(mobileMenu).not.toHaveClass(/active/);
    });
  });

  test.describe('External Links - GitHub', () => {
    test('TC2: GitHub repository link returns 200 status and loads repository', async ({ page, request }) => {
      // Find GitHub link in navigation
      const githubLink = page.locator('[data-testid="nav-link-github"]');
      await expect(githubLink).toBeVisible();

      // Get the href
      const href = await githubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Verify link opens in new tab (for security)
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', /noopener/);

      // Make HTTP request to verify the link returns 200
      const response = await request.get(href);
      expect(response.status()).toBe(200);

      // Verify the response is an HTML page containing expected content
      const contentType = response.headers()['content-type'];
      expect(contentType).toContain('text/html');
    });

    test('TC2a: Hero View on GitHub button is valid', async ({ page, request }) => {
      const heroGithubBtn = page.locator('.hero-buttons a[href="https://github.com/yetone/mirdb"]');
      await expect(heroGithubBtn).toBeVisible();
      await expect(heroGithubBtn).toContainText('GitHub');

      const href = await heroGithubBtn.getAttribute('href');
      const response = await request.get(href);
      expect(response.status()).toBe(200);
    });

    test('TC2b: Footer GitHub link is valid', async ({ page, request }) => {
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();

      const href = await footerGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      const response = await request.get(href);
      expect(response.status()).toBe(200);
    });
  });

  test.describe('External Links - Documentation', () => {
    test('TC3: Documentation link is valid and accessible', async ({ page, request }) => {
      // Find documentation link
      const docsLink = page.locator('[data-testid="nav-link-docs"]');
      await expect(docsLink).toBeVisible();

      const href = await docsLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');

      // Verify link opens in new tab
      await expect(docsLink).toHaveAttribute('target', '_blank');
      await expect(docsLink).toHaveAttribute('rel', /noopener/);

      // Make HTTP request to verify accessibility
      // Note: GitHub redirects #readme to the main page, so we just verify the base URL
      const baseUrl = href.split('#')[0];
      const response = await request.get(baseUrl);
      expect(response.status()).toBe(200);
    });

    test('TC3a: Commands documentation link is valid', async ({ page, request }) => {
      const commandsDocsLink = page.locator('[data-testid="commands-docs-link"]');
      await expect(commandsDocsLink).toBeVisible();

      const href = await commandsDocsLink.getAttribute('href');
      expect(href).toContain('memcached');
      expect(href).toContain('protocol');

      // Verify the memcached protocol doc link
      const response = await request.get(href);
      expect(response.status()).toBe(200);
    });

    test('TC3b: Contributing link is valid', async ({ page, request }) => {
      const contributingLink = page.locator('[data-testid="footer-contributing-link"]');
      await expect(contributingLink).toBeVisible();

      const href = await contributingLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb#contributing');

      // Verify the base repository URL is accessible
      const baseUrl = href.split('#')[0];
      const response = await request.get(baseUrl);
      expect(response.status()).toBe(200);
    });

    test('TC3c: License link is valid', async ({ page, request }) => {
      const licenseLink = page.locator('[data-testid="footer-license-link"]');
      await expect(licenseLink).toBeVisible();

      const href = await licenseLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb#license');

      // Verify the base repository URL is accessible
      const baseUrl = href.split('#')[0];
      const response = await request.get(baseUrl);
      expect(response.status()).toBe(200);
    });
  });

  test.describe('External Links - Crates.io', () => {
    test('TC4: Check crates.io link if present', async ({ page, request }) => {
      // Search for any crates.io links on the page
      const cratesLinks = page.locator('a[href*="crates.io"]');
      const count = await cratesLinks.count();

      if (count > 0) {
        // If crates.io links exist, verify they are valid
        for (let i = 0; i < count; i++) {
          const link = cratesLinks.nth(i);
          const href = await link.getAttribute('href');

          // Verify the link is accessible
          const response = await request.get(href);
          expect(response.status()).toBe(200);
        }
      } else {
        // If no crates.io links, this test passes (link is optional per test case)
        expect(count).toBe(0);
      }
    });
  });

  test.describe('Automated Link Checker', () => {
    test('TC5: Run automated link checker - no broken links (4xx or 5xx responses)', async ({ page, request }) => {
      // Collect all links on the page
      const allLinks = await page.locator('a[href]').all();
      const brokenLinks = [];
      const checkedUrls = new Set();

      for (const link of allLinks) {
        const href = await link.getAttribute('href');

        // Skip if already checked or if it's an internal anchor only
        if (!href || checkedUrls.has(href)) continue;
        checkedUrls.add(href);

        // Handle different link types
        if (href.startsWith('#')) {
          // Internal anchor link - verify target exists
          const targetId = href.substring(1);
          if (targetId) {
            const targetElement = page.locator(`#${targetId}`);
            const exists = await targetElement.count() > 0;
            if (!exists) {
              brokenLinks.push({
                href,
                type: 'internal-anchor',
                error: `Target element #${targetId} not found`,
              });
            }
          }
        } else if (href.startsWith('http://') || href.startsWith('https://')) {
          // External link - verify HTTP response
          try {
            const response = await request.get(href, {
              timeout: 10000,
              ignoreHTTPSErrors: true,
            });
            const status = response.status();

            // Consider 429 (rate limited) as not broken - it's a transient issue
            if (status >= 400 && status !== 429) {
              brokenLinks.push({
                href,
                type: 'external',
                error: `HTTP ${status}`,
              });
            }
          } catch (error) {
            brokenLinks.push({
              href,
              type: 'external',
              error: error.message,
            });
          }
        } else if (href.startsWith('/')) {
          // Relative link - verify it resolves correctly
          try {
            const response = await request.get(href);
            const status = response.status();

            if (status >= 400) {
              brokenLinks.push({
                href,
                type: 'relative',
                error: `HTTP ${status}`,
              });
            }
          } catch (error) {
            brokenLinks.push({
              href,
              type: 'relative',
              error: error.message,
            });
          }
        }
      }

      // Report any broken links found
      if (brokenLinks.length > 0) {
        console.log('Broken links found:');
        brokenLinks.forEach(bl => {
          console.log(`  - ${bl.href} (${bl.type}): ${bl.error}`);
        });
      }

      // Assert no broken links
      expect(brokenLinks).toHaveLength(0);
    });

    test('TC5a: Verify all external links have proper security attributes', async ({ page }) => {
      // Find all external links (links that open in new tabs)
      const externalLinks = page.locator('a[target="_blank"]');
      const count = await externalLinks.count();

      const insecureLinks = [];

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const rel = await link.getAttribute('rel');

        // Verify external links have rel="noopener" for security
        if (!rel || !rel.includes('noopener')) {
          insecureLinks.push({
            href,
            missingAttribute: 'noopener',
          });
        }
      }

      // Report any insecure links
      if (insecureLinks.length > 0) {
        console.log('Links missing security attributes:');
        insecureLinks.forEach(il => {
          console.log(`  - ${il.href}: missing ${il.missingAttribute}`);
        });
      }

      // Assert all external links are secure
      expect(insecureLinks).toHaveLength(0);
    });

    test('TC5b: Verify canonical URL in meta tags is valid', async ({ page, request }) => {
      // Check canonical URL
      const canonicalLink = page.locator('link[rel="canonical"]');
      const canonicalHref = await canonicalLink.getAttribute('href');

      // The canonical URL should be defined
      expect(canonicalHref).toBeTruthy();

      // Note: We don't verify the canonical URL responds since it may be a production domain
      // Just verify the format is correct
      expect(canonicalHref).toMatch(/^https?:\/\//);
    });
  });
});

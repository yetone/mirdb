// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Link Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Get Started CTA navigates to quick start section', async ({ page }) => {
    // Locate the Get Started button in the hero section
    const heroSection = page.locator('.hero');
    const getStartedBtn = heroSection.locator('a.btn').filter({ hasText: /Get Started/i });
    await expect(getStartedBtn).toBeVisible();

    // Verify the href points to the getting-started section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#getting-started');

    // Click the button and verify navigation
    await getStartedBtn.click();

    // Verify the URL hash changed
    await expect(page).toHaveURL(/#getting-started/);

    // Verify the quick start section is visible
    const quickStartSection = page.locator('#getting-started');
    await expect(quickStartSection).toBeVisible();
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC2: View Documentation CTA navigates to documentation', async ({ page }) => {
    // Locate the View Documentation button in the hero section
    const heroSection = page.locator('.hero');
    const docBtn = heroSection.locator('a.btn').filter({ hasText: /View Documentation/i });
    await expect(docBtn).toBeVisible();

    // Verify the href points to GitHub repository or documentation
    const href = await docBtn.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/github\.com.*mirdb|readme|doc/i);

    // Verify the link has proper attributes for external navigation
    const target = await docBtn.getAttribute('target');
    const rel = await docBtn.getAttribute('rel');

    // For external links, should open in new tab with security attributes
    if (href && href.startsWith('http')) {
      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
    }
  });

  test('TC3: GitHub footer link opens repository in new tab', async ({ page }) => {
    // Locate the footer section
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Find the GitHub link in the footer
    const githubLink = footer.locator('a').filter({ hasText: /GitHub/i });
    await expect(githubLink).toBeVisible();

    // Verify the href points to the MirDB GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC4: All links return valid responses (no 404 errors)', async ({ page, request }) => {
    // Get all anchor elements on the page
    const links = await page.locator('a[href]').all();
    expect(links.length).toBeGreaterThan(0);

    const checkedUrls = new Set();
    const errors = [];

    for (const link of links) {
      const href = await link.getAttribute('href');
      if (!href) continue;

      // Skip anchor links (internal page navigation)
      if (href.startsWith('#')) continue;

      // Skip already checked URLs
      if (checkedUrls.has(href)) continue;
      checkedUrls.add(href);

      // For external URLs, make a HEAD request to check if they're valid
      if (href.startsWith('http://') || href.startsWith('https://')) {
        try {
          const response = await request.head(href, {
            timeout: 10000,
            ignoreHTTPSErrors: true,
          });

          // Check for 404 or other client errors
          if (response.status() === 404) {
            errors.push(`Link ${href} returned 404 Not Found`);
          } else if (response.status() >= 400 && response.status() < 500) {
            // Some sites block HEAD requests, try GET
            const getResponse = await request.get(href, {
              timeout: 10000,
              ignoreHTTPSErrors: true,
            });
            if (getResponse.status() === 404) {
              errors.push(`Link ${href} returned 404 Not Found`);
            }
          }
        } catch (e) {
          // Network errors are not 404s, they might be timeouts or blocked requests
          // We only care about 404 errors for this test
          console.log(`Could not verify ${href}: ${e.message}`);
        }
      }
    }

    // Report all 404 errors
    if (errors.length > 0) {
      throw new Error(`Found broken links (404):\n${errors.join('\n')}`);
    }
  });

  test('TC5: External links have target=_blank and rel=noopener', async ({ page }) => {
    // Get all anchor elements with external URLs
    const links = await page.locator('a[href^="http://"], a[href^="https://"]').all();
    expect(links.length).toBeGreaterThan(0);

    const issues = [];

    for (const link of links) {
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');
      const text = await link.textContent();

      // Check for target="_blank"
      if (target !== '_blank') {
        issues.push(`External link "${text?.trim()}" (${href}) is missing target="_blank"`);
      }

      // Check for rel="noopener" (security best practice)
      if (!rel || !rel.includes('noopener')) {
        issues.push(`External link "${text?.trim()}" (${href}) is missing rel="noopener"`);
      }
    }

    // Report all issues
    if (issues.length > 0) {
      throw new Error(`External links missing proper attributes:\n${issues.join('\n')}`);
    }
  });

  test('TC6: All internal anchor links point to existing sections', async ({ page }) => {
    // Get all anchor elements with hash links
    const hashLinks = await page.locator('a[href^="#"]').all();

    for (const link of hashLinks) {
      const href = await link.getAttribute('href');
      if (!href || href === '#') continue;

      const targetId = href.substring(1);
      const targetElement = page.locator(`#${targetId}`);

      // Verify the target element exists
      const count = await targetElement.count();
      expect(count, `Target section "${targetId}" should exist for link ${href}`).toBe(1);
    }
  });

  test('TC7: Documentation footer link is functional', async ({ page }) => {
    // Locate the Documentation link in the footer
    const footer = page.locator('footer.footer');
    const docsLink = footer.locator('a').filter({ hasText: /Documentation/i });
    await expect(docsLink).toBeVisible();

    // Verify the href points to documentation
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/github\.com.*mirdb.*readme/i);

    // Verify it opens in a new tab with security
    const target = await docsLink.getAttribute('target');
    const rel = await docsLink.getAttribute('rel');
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
  });

  test('TC8: License footer link is functional', async ({ page }) => {
    // Locate the License link in the footer
    const footer = page.locator('footer.footer');
    const licenseLink = footer.locator('a').filter({ hasText: /License/i });
    await expect(licenseLink).toBeVisible();

    // Verify the href points to GitHub repository (license info would be there)
    const href = await licenseLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/github\.com.*mirdb/i);

    // Verify it opens in a new tab with security
    const target = await licenseLink.getAttribute('target');
    const rel = await licenseLink.getAttribute('rel');
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
  });
});

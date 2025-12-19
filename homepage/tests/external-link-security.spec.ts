import { test, expect } from '@playwright/test';

/**
 * Security - External Link Handling Tests
 *
 * These tests verify that external links are secured properly to prevent
 * security vulnerabilities like tabnabbing attacks and information disclosure.
 */
test.describe('Security - External Link Handling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  /**
   * Test Case 1: Find all external links on page
   * Verifies that all external links (GitHub, docs, crates.io, etc.) are identified
   */
  test('TC1: Find all external links on page', async ({ page }) => {
    // External links are identified by href starting with http:// or https://
    // and not pointing to the same origin
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    // Collect all external link details
    const linkDetails: { href: string; text: string }[] = [];
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const text = await link.textContent();
      if (href) {
        linkDetails.push({ href, text: text?.trim() || '' });
      }
    }

    // Verify external links are found
    expect(count).toBeGreaterThan(0);

    // Expected external domains that should be present
    const expectedDomains = ['github.com', 'opensource.org', 'crates.io'];
    const foundDomains = linkDetails.map((link) => {
      try {
        return new URL(link.href).hostname;
      } catch {
        return '';
      }
    });

    // Verify all expected domains are represented
    for (const domain of expectedDomains) {
      const hasDomain = foundDomains.some((d) => d.includes(domain));
      expect(hasDomain, `Expected to find link to ${domain}`).toBe(true);
    }

    // Log the found external links for visibility
    console.log(`Found ${count} external links:`);
    linkDetails.forEach((link) => {
      console.log(`  - ${link.text}: ${link.href}`);
    });
  });

  /**
   * Test Case 2: Check GitHub link security attributes
   * Verifies GitHub links have rel='noopener noreferrer' and target='_blank'
   */
  test('TC2: Check GitHub link security attributes', async ({ page }) => {
    // Find all GitHub links
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThan(0);

    // Check each GitHub link for security attributes
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // Verify target="_blank" for opening in new tab
      expect(
        target,
        `GitHub link ${href} should have target="_blank"`
      ).toBe('_blank');

      // Verify rel contains noopener and noreferrer for security
      expect(rel, `GitHub link ${href} should have rel attribute`).toBeTruthy();
      expect(
        rel,
        `GitHub link ${href} should contain "noopener"`
      ).toContain('noopener');
      expect(
        rel,
        `GitHub link ${href} should contain "noreferrer"`
      ).toContain('noreferrer');
    }
  });

  /**
   * Test Case 3: Check documentation link security attributes
   * Verifies external documentation links have appropriate security attributes
   */
  test('TC3: Check documentation link security attributes', async ({ page }) => {
    // Find documentation-related links (GitHub README docs, external docs, etc.)
    // This includes links in footer labeled as "Docs" and any other documentation links
    const docsLinks = page.locator(
      'a[data-testid="footer-docs-link"], a[href*="#readme"], a[href*="docs"], a:has-text("Docs"), a:has-text("Documentation")'
    );
    const count = await docsLinks.count();

    // Filter to only external links (with http/https)
    const externalDocsLinks: { href: string; target: string | null; rel: string | null }[] = [];
    for (let i = 0; i < count; i++) {
      const link = docsLinks.nth(i);
      const href = await link.getAttribute('href');
      if (href && (href.startsWith('http://') || href.startsWith('https://'))) {
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');
        externalDocsLinks.push({ href, target, rel });
      }
    }

    // There should be at least one external documentation link
    expect(externalDocsLinks.length).toBeGreaterThan(0);

    // Check each external docs link for security attributes
    for (const linkInfo of externalDocsLinks) {
      expect(
        linkInfo.target,
        `Docs link ${linkInfo.href} should have target="_blank"`
      ).toBe('_blank');
      expect(
        linkInfo.rel,
        `Docs link ${linkInfo.href} should have rel attribute`
      ).toBeTruthy();
      expect(
        linkInfo.rel,
        `Docs link ${linkInfo.href} should contain "noopener"`
      ).toContain('noopener');
      expect(
        linkInfo.rel,
        `Docs link ${linkInfo.href} should contain "noreferrer"`
      ).toContain('noreferrer');
    }
  });

  /**
   * Test Case 4: Verify no sensitive information in URLs
   * Ensures links do not contain tokens, keys, or sensitive parameters
   */
  test('TC4: Verify no sensitive information in URLs', async ({ page }) => {
    // Get all links on the page
    const allLinks = page.locator('a[href]');
    const count = await allLinks.count();

    // Patterns that indicate sensitive information in URLs
    const sensitivePatterns = [
      /[?&]token=/i,
      /[?&]key=/i,
      /[?&]api[_-]?key=/i,
      /[?&]secret=/i,
      /[?&]password=/i,
      /[?&]passwd=/i,
      /[?&]pwd=/i,
      /[?&]auth=/i,
      /[?&]access[_-]?token=/i,
      /[?&]bearer=/i,
      /[?&]session=/i,
      /[?&]jwt=/i,
      /[?&]credential/i,
      /[?&]private/i,
      // Also check for patterns in the path that look like tokens
      /\/token\/[a-zA-Z0-9]{20,}/,
      /\/key\/[a-zA-Z0-9]{20,}/,
    ];

    const linksWithSensitiveInfo: string[] = [];

    for (let i = 0; i < count; i++) {
      const link = allLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href) {
        for (const pattern of sensitivePatterns) {
          if (pattern.test(href)) {
            linksWithSensitiveInfo.push(`${href} (matches: ${pattern})`);
            break;
          }
        }
      }
    }

    // Assert no sensitive information was found
    expect(
      linksWithSensitiveInfo,
      'Links should not contain sensitive information like tokens, keys, or secrets'
    ).toHaveLength(0);

    if (linksWithSensitiveInfo.length > 0) {
      console.error('Found links with potentially sensitive information:');
      linksWithSensitiveInfo.forEach((link) => console.error(`  - ${link}`));
    }
  });

  /**
   * Additional security test: Verify all external links have security attributes
   * This is a comprehensive check that all external links (target="_blank") have
   * the proper rel="noopener noreferrer" attributes
   */
  test('TC5: All external links with target="_blank" have security attributes', async ({ page }) => {
    // Find all links that open in a new tab
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    const insecureLinks: { href: string; rel: string | null }[] = [];

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');

      // Check if rel is missing or doesn't contain required attributes
      if (!rel || !rel.includes('noopener') || !rel.includes('noreferrer')) {
        insecureLinks.push({ href: href || 'unknown', rel });
      }
    }

    // Report any insecure links found
    if (insecureLinks.length > 0) {
      console.error('Insecure external links found:');
      insecureLinks.forEach((link) => {
        console.error(`  - ${link.href} (rel: ${link.rel || 'missing'})`);
      });
    }

    expect(
      insecureLinks,
      'All external links should have rel="noopener noreferrer"'
    ).toHaveLength(0);
  });

  /**
   * Additional security test: Verify crates.io link security
   */
  test('TC6: Crates.io link has security attributes', async ({ page }) => {
    // Find crates.io link
    const cratesLink = page.locator('a[href*="crates.io"]');
    const count = await cratesLink.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = cratesLink.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      expect(
        target,
        `Crates.io link ${href} should have target="_blank"`
      ).toBe('_blank');
      expect(
        rel,
        `Crates.io link ${href} should have rel attribute`
      ).toBeTruthy();
      expect(
        rel,
        `Crates.io link ${href} should contain "noopener"`
      ).toContain('noopener');
      expect(
        rel,
        `Crates.io link ${href} should contain "noreferrer"`
      ).toContain('noreferrer');
    }
  });

  /**
   * Additional security test: Verify MIT License link security
   */
  test('TC7: MIT License link has security attributes', async ({ page }) => {
    // Find MIT License link
    const licenseLink = page.locator('a[href*="opensource.org"]');
    const count = await licenseLink.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = licenseLink.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      expect(
        target,
        `License link ${href} should have target="_blank"`
      ).toBe('_blank');
      expect(
        rel,
        `License link ${href} should have rel attribute`
      ).toBeTruthy();
      expect(
        rel,
        `License link ${href} should contain "noopener"`
      ).toContain('noopener');
      expect(
        rel,
        `License link ${href} should contain "noreferrer"`
      ).toContain('noreferrer');
    }
  });
});

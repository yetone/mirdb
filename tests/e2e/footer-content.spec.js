/**
 * E2E tests for Footer Content scenario
 * Scenario: Verify footer contains required links and information
 *
 * Test Case 2: Check GitHub link in footer - Link to GitHub repository is present in footer
 * Test Case 3: Check license information - License information or link is present in footer
 */
const { test, expect } = require('@playwright/test');

test.describe('Footer Content - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 2: GitHub Link in Footer', () => {
    test('Footer contains a link to the GitHub repository', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Use .first() to handle multiple GitHub links in footer (main repo + issues)
      const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
      await expect(githubLink).toBeVisible();
    });

    test('GitHub link has correct URL', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();

      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');
    });

    test('GitHub link opens in new tab with security attributes', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();

      const target = await githubLink.getAttribute('target');
      const rel = await githubLink.getAttribute('rel');

      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
    });

    test('GitHub link text is visible and descriptive', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();

      await expect(githubLink).toBeVisible();
      const text = await githubLink.textContent();
      expect(text.toLowerCase()).toMatch(/github|source|repository/i);
    });
  });

  test.describe('Test Case 3: License Information', () => {
    test('Footer contains license information text', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      const footerText = await footer.textContent();
      const hasLicenseInfo =
        footerText.toLowerCase().includes('license') ||
        footerText.toLowerCase().includes('mit') ||
        footerText.toLowerCase().includes('apache') ||
        footerText.toLowerCase().includes('bsd');

      expect(hasLicenseInfo).toBe(true);
    });

    test('License type is specified (MIT License)', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // MirDB uses MIT License as per the PRD and current implementation
      expect(footerText).toContain('MIT License');
    });

    test('Copyright notice is present', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      const hasCopyright =
        footerText.includes('©') ||
        footerText.toLowerCase().includes('copyright');

      expect(hasCopyright).toBe(true);
    });
  });

  test.describe('Footer Accessibility and Usability', () => {
    test('Footer is visible when scrolled to bottom', async ({ page }) => {
      // Scroll to the bottom of the page
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('Footer links are focusable via keyboard', async ({ page }) => {
      const footer = page.locator('footer');
      const links = footer.locator('a');
      const count = await links.count();

      expect(count).toBeGreaterThan(0);

      // Check first link is focusable
      const firstLink = links.first();
      await firstLink.focus();
      await expect(firstLink).toBeFocused();
    });

    test('Footer links have accessible names', async ({ page }) => {
      const footer = page.locator('footer');
      const links = footer.locator('a');
      const count = await links.count();

      for (let i = 0; i < count; i++) {
        const link = links.nth(i);
        const text = await link.textContent();
        const ariaLabel = await link.getAttribute('aria-label');

        // Link should have either visible text or aria-label
        expect(text.trim().length > 0 || ariaLabel).toBeTruthy();
      }
    });
  });
});

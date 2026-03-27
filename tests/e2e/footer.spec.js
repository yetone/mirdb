/**
 * Footer Section E2E Tests
 * Owner: Scenario 16 - Footer Section
 *
 * Tests:
 * - License information is displayed and links to LICENSE file
 * - Community/contribution links are present and functional
 * - Footer is visible and accessible
 */
import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('License Information', () => {
    test('should display license information', async ({ page }) => {
      const footer = page.locator('footer#footer');
      await expect(footer).toBeVisible();

      // Check for MIT License text
      const licenseText = footer.getByText('MIT License', { exact: false });
      await expect(licenseText.first()).toBeVisible();
    });

    test('should have a link to LICENSE file', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const licenseLink = footer.locator('[data-license-link="true"]');

      await expect(licenseLink).toBeVisible();
      const href = await licenseLink.getAttribute('href');
      expect(href).toContain('LICENSE');
      expect(href).toContain('github.com/yetone/mirdb');
    });

    test('license link should open in new tab', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const licenseLink = footer.locator('[data-license-link="true"]');

      await expect(licenseLink).toHaveAttribute('target', '_blank');
      await expect(licenseLink).toHaveAttribute('rel', /noopener/);
    });
  });

  test.describe('Community/Contribution Links', () => {
    test('should have GitHub repository link', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const githubLink = footer.locator('[data-community-link="github"]');

      await expect(githubLink).toBeVisible();
      const href = await githubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('should have issues link for community engagement', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const issuesLink = footer.locator('[data-community-link="issues"]');

      await expect(issuesLink).toBeVisible();
      const href = await issuesLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb/issues');
    });

    test('should have contribute/pull requests link', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const contributeLink = footer.locator('[data-community-link="contributing"]');

      await expect(contributeLink).toBeVisible();
      const href = await contributeLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');
    });

    test('all community links should open in new tab', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const communityLinks = footer.locator('[data-community-link]');

      const count = await communityLinks.count();
      expect(count).toBeGreaterThanOrEqual(2);

      for (let i = 0; i < count; i++) {
        const link = communityLinks.nth(i);
        await expect(link).toHaveAttribute('target', '_blank');
        await expect(link).toHaveAttribute('rel', /noopener/);
      }
    });
  });

  test.describe('Footer Visibility and Navigation', () => {
    test('should be visible when scrolling to bottom', async ({ page }) => {
      const footer = page.locator('footer#footer');

      // Scroll to footer
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('should have proper heading structure', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const headings = footer.locator('h3');

      const count = await headings.count();
      expect(count).toBeGreaterThanOrEqual(2);
    });

    test('should display copyright information', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const copyrightText = footer.getByText(/©.*MirDB/);

      await expect(copyrightText).toBeVisible();
    });
  });

  test.describe('Accessibility', () => {
    test('should have role="contentinfo"', async ({ page }) => {
      const footer = page.locator('footer#footer');
      await expect(footer).toHaveAttribute('role', 'contentinfo');
    });

    test('should be keyboard navigable', async ({ page }) => {
      const footer = page.locator('footer#footer');
      const firstLink = footer.locator('a').first();

      // Tab to footer area
      await firstLink.focus();
      await expect(firstLink).toBeFocused();
    });
  });
});

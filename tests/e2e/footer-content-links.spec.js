/**
 * E2E Tests for Footer Content and Links
 * Scenario: Verify the footer contains required links to documentation, GitHub, license, and project status
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Footer Content and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test.describe('Test Case 1: Footer navigation links', () => {
    test('should have footer element visible at bottom of page', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('should contain Documentation link', async ({ page }) => {
      const footer = page.locator('footer');
      const docLink = footer.locator('a', { hasText: /documentation/i });
      await expect(docLink).toBeVisible();

      const href = await docLink.getAttribute('href');
      expect(href).toBeTruthy();
    });

    test('should contain GitHub link', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a', { hasText: /github/i });
      await expect(githubLink).toBeVisible();

      const href = await githubLink.getAttribute('href');
      expect(href).toBeTruthy();
    });

    test('should contain License link', async ({ page }) => {
      const footer = page.locator('footer');
      const licenseLink = footer.locator('a', { hasText: /license/i });
      await expect(licenseLink).toBeVisible();

      const href = await licenseLink.getAttribute('href');
      expect(href).toBeTruthy();
    });

    test('should have at least 3 navigation links', async ({ page }) => {
      const footerLinks = page.locator('footer .footer-links a');
      const count = await footerLinks.count();
      expect(count).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('Test Case 2: GitHub link destination', () => {
    test('GitHub link should point to valid GitHub URL', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a', { hasText: /github/i });

      const href = await githubLink.getAttribute('href');
      expect(href).toMatch(/^https:\/\/github\.com\/.+/);
    });

    test('GitHub link should point to mirdb repository', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a', { hasText: /github/i });

      const href = await githubLink.getAttribute('href');
      expect(href.toLowerCase()).toContain('mirdb');
    });

    test('GitHub link should open in new tab', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a', { hasText: /github/i });

      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('GitHub link should have security rel attributes', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a', { hasText: /github/i });

      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });

  test.describe('Test Case 3: Project status badge/indicator', () => {
    test('footer should display project status indicator', async ({ page }) => {
      const footer = page.locator('footer');

      // Check for status badge, image, or text indicator
      const statusBadge = footer.locator('.project-status, .status-badge, [class*="status"], [class*="badge"]');
      const statusImage = footer.locator('img[alt*="status"], img[alt*="badge"], img[src*="badge"], img[src*="shields.io"]');

      const footerText = await footer.textContent();
      const hasStatusText =
        footerText.toLowerCase().includes('open source') ||
        footerText.toLowerCase().includes('mit license') ||
        footerText.toLowerCase().includes('active') ||
        footerText.toLowerCase().includes('maintained');

      const hasBadge = await statusBadge.count() > 0;
      const hasImage = await statusImage.count() > 0;

      // Footer should have either a badge element, status image, or status text
      expect(hasBadge || hasImage || hasStatusText).toBe(true);
    });

    test('should indicate licensing or development status', async ({ page }) => {
      const footer = page.locator('footer');
      const footerText = await footer.textContent();

      // Footer should mention licensing, open source status, or development status
      const hasStatusInfo =
        footerText.toLowerCase().includes('mit') ||
        footerText.toLowerCase().includes('license') ||
        footerText.toLowerCase().includes('open source');

      expect(hasStatusInfo).toBe(true);
    });
  });

  test.describe('Test Case 4: Semantic HTML structure', () => {
    test('footer content should be wrapped in footer element', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Footer should exist and contain content
      const content = await footer.textContent();
      expect(content.length).toBeGreaterThan(0);
    });

    test('footer should have aria-label for accessibility', async ({ page }) => {
      const footer = page.locator('footer');
      const ariaLabel = await footer.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });

    test('footer should be at bottom of page', async ({ page }) => {
      const footer = page.locator('footer');
      const box = await footer.boundingBox();

      // Footer should be visible
      expect(box).not.toBeNull();

      // Footer should be at bottom (y position should be significant)
      expect(box.y).toBeGreaterThan(100);
    });
  });

  test.describe('Additional footer accessibility tests', () => {
    test('all footer links should be keyboard accessible', async ({ page }) => {
      const footerLinks = page.locator('footer .footer-links a');
      const count = await footerLinks.count();

      for (let i = 0; i < count; i++) {
        const link = footerLinks.nth(i);
        await expect(link).toBeVisible();

        // Check link has proper href
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();
      }
    });

    test('footer links should have visible text', async ({ page }) => {
      const footerLinks = page.locator('footer .footer-links a');
      const count = await footerLinks.count();

      for (let i = 0; i < count; i++) {
        const link = footerLinks.nth(i);
        const text = await link.textContent();
        expect(text.trim().length).toBeGreaterThan(0);
      }
    });
  });
});

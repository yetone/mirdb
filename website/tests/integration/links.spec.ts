/**
 * Link Validation Integration Tests
 * Owner: Scenario 14 - Error Handling - Broken Links
 *
 * Tests that validate all links on the page point to valid destinations:
 * - Internal anchor links (#section) point to existing elements
 * - External links have proper format and attributes
 * - Documentation links are properly configured
 */
import { test, expect } from '@playwright/test';

test.describe('Error Handling - Broken Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Internal Anchor Links Validation', () => {
    test('All navigation anchor links point to existing elements', async ({ page }) => {
      // Get all internal anchor links from navigation
      const navLinks = await page.locator('.nav__links a[href^="#"], .nav__mobile-menu a[href^="#"]').all();
      const validatedIds = new Set<string>();

      for (const link of navLinks) {
        const href = await link.getAttribute('href');
        if (href && href.startsWith('#')) {
          const targetId = href.substring(1);
          if (!validatedIds.has(targetId)) {
            validatedIds.add(targetId);
            const targetElement = page.locator(`#${targetId}`);
            await expect(targetElement, `Element with id="${targetId}" should exist for link ${href}`).toBeVisible();
          }
        }
      }

      // Verify we found navigation links
      expect(validatedIds.size).toBeGreaterThan(0);
    });

    test('Hero section CTA anchor links point to existing elements', async ({ page }) => {
      // Get the Quick Start CTA link
      const quickStartLink = page.locator('.hero__cta[href^="#"]');
      const href = await quickStartLink.getAttribute('href');

      expect(href).toBeTruthy();
      if (href && href.startsWith('#')) {
        const targetId = href.substring(1);
        const targetElement = page.locator(`#${targetId}`);
        await expect(targetElement, `Element with id="${targetId}" should exist`).toBeVisible();
      }
    });

    test('All internal anchor links on page point to existing elements', async ({ page }) => {
      // Collect all anchor links on the page
      const allAnchorLinks = await page.locator('a[href^="#"]').all();
      const checkedIds = new Set<string>();

      for (const link of allAnchorLinks) {
        const href = await link.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.substring(1);

          // Avoid checking duplicates
          if (!checkedIds.has(targetId)) {
            checkedIds.add(targetId);
            const targetElement = page.locator(`#${targetId}`);
            await expect(targetElement, `Element with id="${targetId}" should exist for anchor link ${href}`).toBeVisible();
          }
        }
      }

      // Ensure we found and validated at least some internal links
      expect(checkedIds.size).toBeGreaterThan(0);
    });
  });

  test.describe('TC2: GitHub Repository Link Validation', () => {
    test('GitHub link in footer has valid format and attributes', async ({ page }) => {
      // Get the GitHub link from footer
      const githubLink = page.locator('[data-testid="github-link"]');
      await expect(githubLink).toBeVisible();

      const href = await githubLink.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https:\/\/github\.com\/.+/);

      // Verify proper link attributes for external link
      await expect(githubLink).toHaveAttribute('target', '_blank');
      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('GitHub link in hero section has valid format and attributes', async ({ page }) => {
      // Get the GitHub CTA link from hero
      const githubCta = page.locator('.hero__cta:has-text("View on GitHub")');
      await expect(githubCta).toBeVisible();

      const href = await githubCta.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https:\/\/github\.com\/.+/);

      // Verify proper link attributes for external link
      await expect(githubCta).toHaveAttribute('target', '_blank');
      const rel = await githubCta.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('All GitHub links are well-formed URLs', async ({ page }) => {
      // Find all links that point to GitHub
      const githubLinks = await page.locator('a[href*="github.com"]').all();
      expect(githubLinks.length).toBeGreaterThan(0);

      for (const link of githubLinks) {
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();

        // Verify URL is well-formed
        const url = new URL(href!);
        expect(url.protocol).toBe('https:');
        expect(url.hostname).toBe('github.com');
        expect(url.pathname.length).toBeGreaterThan(1); // Has a path beyond /
      }
    });
  });

  test.describe('TC3: Documentation Links Validation', () => {
    test('Technical documentation link has valid format', async ({ page }) => {
      // Get the documentation link from features section
      const docsLink = page.locator('.features__docs-link');
      const isVisible = await docsLink.isVisible();

      if (isVisible) {
        const href = await docsLink.getAttribute('href');
        expect(href).toBeTruthy();

        // Check if it's an external link
        if (href!.startsWith('http')) {
          // Verify URL is well-formed
          const url = new URL(href!);
          expect(url.protocol).toBe('https:');

          // Verify external link attributes
          const target = await docsLink.getAttribute('target');
          expect(target).toBe('_blank');
        } else {
          // For relative links, just verify format
          expect(href).toMatch(/^[/.]|^[a-zA-Z]/);
        }
      }
    });

    test('Community link has valid format and attributes', async ({ page }) => {
      // Get the community link from footer
      const communityLink = page.locator('[data-testid="community-link"]');
      await expect(communityLink).toBeVisible();

      const href = await communityLink.getAttribute('href');
      expect(href).toBeTruthy();

      // Verify URL is well-formed
      const url = new URL(href!);
      expect(url.protocol).toBe('https:');

      // Verify proper link attributes for external link
      await expect(communityLink).toHaveAttribute('target', '_blank');
      const rel = await communityLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('License link has valid format and attributes', async ({ page }) => {
      // Get the license link from footer
      const licenseLink = page.locator('[data-testid="license-link"]');
      await expect(licenseLink).toBeVisible();

      const href = await licenseLink.getAttribute('href');
      expect(href).toBeTruthy();

      // Verify URL is well-formed
      const url = new URL(href!);
      expect(url.protocol).toBe('https:');

      // Verify proper link attributes for external link
      await expect(licenseLink).toHaveAttribute('target', '_blank');
      const rel = await licenseLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('All external documentation links open in new tab', async ({ page }) => {
      // Get all external links from footer navigation
      const footerLinks = await page.locator('.footer__links a[href^="http"]').all();
      expect(footerLinks.length).toBeGreaterThan(0);

      for (const link of footerLinks) {
        const target = await link.getAttribute('target');
        expect(target).toBe('_blank');

        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });
  });
});

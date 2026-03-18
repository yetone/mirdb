/**
 * Navigation and Footer E2E Tests.
 * Owner: Scenario 9 - Navigation and Footer
 *
 * Tests:
 * - Sticky navigation
 * - Smooth scroll to sections
 * - Footer content
 * - License and copyright
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation and Footer', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test.describe('Sticky Navigation', () => {
    test('navigation element has position sticky at top', async ({ page }) => {
      // Get the header element
      const header = page.getByTestId('header');
      await expect(header).toBeVisible();

      // Verify it has sticky positioning
      const position = await header.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      expect(position).toBe('sticky');

      // Verify top position
      const top = await header.evaluate((el) => {
        return window.getComputedStyle(el).top;
      });
      expect(top).toBe('0px');
    });

    test('navigation remains visible after scrolling', async ({ page }) => {
      const header = page.getByTestId('header');

      // Scroll down the page
      await page.evaluate(() => {
        window.scrollTo(0, 500);
      });

      // Wait for scroll to complete
      await page.waitForTimeout(100);

      // Header should still be visible
      await expect(header).toBeVisible();
      await expect(header).toBeInViewport();
    });
  });

  test.describe('Smooth Scroll Navigation', () => {
    test('clicking Features link scrolls to features section', async ({ page }) => {
      const featuresLink = page.getByTestId('nav-link-features');
      const featuresSection = page.locator('#features');

      await expect(featuresLink).toBeVisible();
      await expect(featuresSection).toBeVisible();

      // Click the link
      await featuresLink.click();

      // Wait for smooth scroll animation to complete
      await page.waitForTimeout(500);

      // Verify the features section is in the viewport
      await expect(featuresSection).toBeInViewport();
    });

    test('clicking Quick Start link scrolls to quick start section', async ({ page }) => {
      const quickStartLink = page.getByTestId('nav-link-quick-start');
      const quickStartSection = page.locator('#quickstart');

      await expect(quickStartLink).toBeVisible();
      await expect(quickStartSection).toBeVisible();

      // Click the link
      await quickStartLink.click();

      // Wait for smooth scroll animation to complete
      await page.waitForTimeout(500);

      // Verify the quickstart section is in the viewport
      await expect(quickStartSection).toBeInViewport();
    });

    test('clicking Usage link scrolls to usage section', async ({ page }) => {
      const usageLink = page.getByTestId('nav-link-usage');
      const usageSection = page.locator('#usage');

      await expect(usageLink).toBeVisible();
      await expect(usageSection).toBeVisible();

      // Click the link
      await usageLink.click();

      // Wait for smooth scroll animation to complete
      await page.waitForTimeout(500);

      // Verify the usage section is in the viewport
      await expect(usageSection).toBeInViewport();
    });
  });

  test.describe('Footer Content', () => {
    test('footer contains MIT license reference', async ({ page }) => {
      const footer = page.getByTestId('footer');
      await expect(footer).toBeVisible();

      const licenseText = page.getByTestId('footer-license');
      await expect(licenseText).toBeVisible();
      await expect(licenseText).toContainText('MIT');
    });

    test('footer contains Open Source reference', async ({ page }) => {
      const openSourceText = page.getByTestId('footer-open-source');
      await expect(openSourceText).toBeVisible();
      await expect(openSourceText).toContainText('Open Source');
    });

    test('footer contains copyright notice', async ({ page }) => {
      const copyrightText = page.getByTestId('footer-copyright');
      await expect(copyrightText).toBeVisible();
      await expect(copyrightText).toContainText('©');
    });

    test('footer contains author name', async ({ page }) => {
      const copyrightText = page.getByTestId('footer-copyright');
      await expect(copyrightText).toBeVisible();
      // Default author is 'yetone'
      await expect(copyrightText).toContainText('yetone');
    });
  });

  test.describe('Footer Links', () => {
    test('GitHub link in footer is functional', async ({ page }) => {
      const githubLink = page.getByTestId('footer-github-link');
      await expect(githubLink).toBeVisible();

      // Verify the link has the correct href
      const href = await githubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Verify it opens in a new tab
      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');

      // Verify rel attribute for security
      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });

  test.describe('Navigation Component', () => {
    test('navigation renders all nav items with anchor links', async ({ page }) => {
      const navigation = page.getByTestId('navigation');
      await expect(navigation).toBeVisible();

      // Check all nav links are present
      await expect(page.getByTestId('nav-link-features')).toBeVisible();
      await expect(page.getByTestId('nav-link-usage')).toBeVisible();
      await expect(page.getByTestId('nav-link-quick-start')).toBeVisible();
      await expect(page.getByTestId('nav-link-github')).toBeVisible();

      // Verify anchor links have correct href
      const featuresHref = await page.getByTestId('nav-link-features').getAttribute('href');
      expect(featuresHref).toBe('#features');

      const usageHref = await page.getByTestId('nav-link-usage').getAttribute('href');
      expect(usageHref).toBe('#usage');

      const quickstartHref = await page.getByTestId('nav-link-quick-start').getAttribute('href');
      expect(quickstartHref).toBe('#quickstart');
    });

    test('GitHub link opens in new tab', async ({ page }) => {
      const githubNavLink = page.getByTestId('nav-link-github');
      await expect(githubNavLink).toBeVisible();

      const target = await githubNavLink.getAttribute('target');
      expect(target).toBe('_blank');

      const href = await githubNavLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');
    });
  });
});

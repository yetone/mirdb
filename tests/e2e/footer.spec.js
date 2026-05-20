/**
 * Footer E2E Tests
 * Owner: Scenario 7 - Footer
 *
 * Tests:
 * - Footer is visible at bottom of page
 * - Footer contains copyright text with current year
 * - Footer contains license information
 * - Footer links navigate to correct external URLs
 * - Footer has visual separation from main content
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');
const fileUrl = 'file://' + indexPath;

test.describe('Footer E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(fileUrl);
  });

  test('footer is visible on page', async ({ page }) => {
    const footer = page.locator('footer.site-footer');
    await expect(footer).toBeVisible();
  });

  test('footer contains copyright text with current year', async ({ page }) => {
    const footer = page.locator('footer');
    const currentYear = new Date().getFullYear().toString();

    await expect(footer).toContainText(currentYear);
    await expect(footer).toContainText('MirDB Contributors');
  });

  test('footer contains license information', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toContainText('MIT License');
  });

  test('footer contains GitHub link with correct URL', async ({ page }) => {
    const githubLink = page.locator('footer a').filter({ hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('footer contains Documentation link', async ({ page }) => {
    const docsLink = page.locator('footer a').filter({ hasText: 'Documentation' });
    await expect(docsLink).toBeVisible();

    const href = await docsLink.getAttribute('href');
    expect(href).toContain('documentation');
  });

  test('footer external links open in new tab', async ({ page }) => {
    const externalLinks = page.locator('footer a[target="_blank"]');
    const count = await externalLinks.count();
    expect(count).toBeGreaterThanOrEqual(2);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  test('footer has visual separation with border-top', async ({ page }) => {
    const footer = page.locator('footer.site-footer');
    await expect(footer).toBeVisible();

    const hasBorder = await footer.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.borderTopWidth !== '0px' || style.borderTopStyle !== 'none';
    });

    expect(hasBorder).toBe(true);
  });

  test('footer has distinct background color from body', async ({ page }) => {
    const footer = page.locator('footer.site-footer');
    const body = page.locator('body');

    const footerBg = await footer.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.backgroundColor;
    });

    const bodyBg = await body.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.backgroundColor;
    });

    expect(footerBg).not.toBe(bodyBg);
  });

  test('footer links use icon SVGs', async ({ page }) => {
    const footer = page.locator('footer');
    const icons = footer.locator('.footer-link-icon');
    const count = await icons.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test('footer navigation has aria-label', async ({ page }) => {
    const nav = page.locator('footer nav[aria-label="Footer navigation"]');
    await expect(nav).toBeVisible();
  });
});

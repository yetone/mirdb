/**
 * Footer Section E2E Tests
 * Owners: Scenario 7 (Footer), Scenario 20 (GitHub CTA)
 *
 * Test groups:
 * - Footer visibility
 * - Navigation links presence
 * - GitHub link validation
 * - External link attributes (target, rel)
 * - Copyright text
 */

import { test, expect } from '@playwright/test';
import { waitForLoad, scrollToSection } from './utils';

test.describe('Footer Navigation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
    await scrollToSection(page, '#footer');
  });

  test('Test Case 1: Footer contains at least 2 navigation links', async ({ page }) => {
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    const links = footer.locator('a');
    const linkCount = await links.count();

    expect(linkCount).toBeGreaterThanOrEqual(2);
  });

  test('Test Case 2: GitHub link exists and points to correct repository', async ({ page }) => {
    const footer = page.locator('#footer');
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');

    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('Test Case 3: GitHub link has proper external link attributes', async ({ page }) => {
    const footer = page.locator('#footer');
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');

    const target = await githubLink.getAttribute('target');
    const rel = await githubLink.getAttribute('rel');

    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
  });

  test('Test Case 4: Footer contains copyright text with current or recent year', async ({ page }) => {
    const footer = page.locator('#footer');
    const footerText = await footer.textContent();

    expect(footerText).toContain('©');

    const currentYear = new Date().getFullYear();
    const hasCurrentYear = footerText?.includes(currentYear.toString());
    const hasPreviousYear = footerText?.includes((currentYear - 1).toString());

    expect(hasCurrentYear || hasPreviousYear).toBeTruthy();
  });

  test('Footer has additional resource links', async ({ page }) => {
    const footer = page.locator('#footer');
    const links = footer.locator('a');

    const linkTexts: string[] = [];
    const count = await links.count();
    for (let i = 0; i < count; i++) {
      const text = await links.nth(i).textContent();
      if (text) linkTexts.push(text.toLowerCase());
    }

    const hasGitHub = linkTexts.some(text => text.includes('github'));
    expect(hasGitHub).toBeTruthy();
  });

  test('Footer links are accessible with proper focus states', async ({ page }) => {
    const footer = page.locator('#footer');
    const firstLink = footer.locator('a').first();

    await firstLink.focus();
    await expect(firstLink).toBeFocused();
  });

  test('Footer is semantically correct with footer tag', async ({ page }) => {
    const footer = page.locator('footer#footer');
    await expect(footer).toBeVisible();
  });
});

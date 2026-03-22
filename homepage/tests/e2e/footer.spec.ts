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

// BEGIN: Scenario 20 - GitHub CTA Navigation Tests
test.describe('GitHub CTA Navigation (Scenario 20)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('Test Case 1: Primary GitHub CTA button exists in hero section with correct href', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Find the primary GitHub CTA button in hero section
    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeVisible();

    // Verify it's a GitHub link
    const href = await primaryCta.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify button text indicates GitHub
    const text = await primaryCta.textContent();
    expect(text?.toLowerCase()).toContain('github');
  });

  test('Test Case 2: All GitHub links point to correct repository', async ({ page }) => {
    // Get all anchor elements with href containing 'github.com'
    const githubLinks = page.locator('a[href*="github.com"]');
    const linkCount = await githubLinks.count();

    // Ensure we have multiple GitHub links across the page
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Check each GitHub link points to the correct repository
    for (let i = 0; i < linkCount; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');

      // All GitHub links should contain the correct repo path
      expect(href).toMatch(/github\.com\/yetone\/mirdb/);
    }
  });

  test('Test Case 3: GitHub links have target="_blank" and rel="noopener"', async ({ page }) => {
    // Get all anchor elements with href containing 'github.com'
    const githubLinks = page.locator('a[href*="github.com"]');
    const linkCount = await githubLinks.count();

    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Verify each GitHub link has proper external link attributes
    for (let i = 0; i < linkCount; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // External links should open in new tab
      expect(target).toBe('_blank');

      // External links should have noopener for security
      expect(rel).toContain('noopener');
    }
  });

  test('Hero, badges, and footer all contain GitHub links', async ({ page }) => {
    // Check hero section has GitHub CTA
    const heroCta = page.locator('#hero a[href*="github.com"]');
    await expect(heroCta.first()).toBeVisible();

    // Check badges section has GitHub link
    const badgesGithub = page.locator('#badges a[href*="github.com"]');
    const badgesCount = await badgesGithub.count();
    expect(badgesCount).toBeGreaterThanOrEqual(1);

    // Check footer has GitHub link
    const footerGithub = page.locator('#footer a[href*="github.com"]');
    await expect(footerGithub.first()).toBeVisible();
  });

  test('GitHub links are keyboard accessible', async ({ page }) => {
    // Focus on the primary CTA
    const primaryCta = page.locator('#primary-cta');
    await primaryCta.focus();
    await expect(primaryCta).toBeFocused();

    // Tab to find other focusable elements
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });
});
// END: Scenario 20 - GitHub CTA Navigation Tests

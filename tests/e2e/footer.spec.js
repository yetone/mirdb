/**
 * E2E tests for Footer Section.
 * Owner: Scenario 13 - Footer Section
 *
 * Tests:
 * - Footer element exists as semantic <footer>
 * - GitHub link exists in footer with correct URL
 * - Footer contains "MirDB" text
 * - Footer contains copyright and author attribution
 * - Footer contains status badge (CircleCI)
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepagePath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(homepagePath);
  });

  // Test Case 1: footer element exists at the bottom of the page
  test('footer element exists and uses semantic footer tag', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');
  });

  // Test Case 2: GitHub link exists in footer
  test('GitHub link exists in footer pointing to https://github.com/yetone/mirdb', async ({ page }) => {
    const footer = page.locator('footer');
    const githubLink = footer.locator('[data-testid="github-link-footer"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  // Test Case 3: Text 'MirDB' appears in the footer
  test('footer contains text "MirDB"', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const text = await footer.textContent();
    expect(text).toContain('MirDB');
  });

  // Test Case 4: Footer contains copyright text or author attribution
  test('footer contains copyright text with author attribution', async ({ page }) => {
    const footer = page.locator('footer');
    const copyright = footer.locator('.footer-copyright');
    await expect(copyright).toBeVisible();

    const text = await copyright.textContent();
    expect(text.toLowerCase()).toContain('yetone');
  });

  // Additional: Footer contains project status badge
  test('footer contains CircleCI build status badge', async ({ page }) => {
    const footer = page.locator('footer');
    const badges = footer.locator('[data-testid="footer-badges"]');
    await expect(badges).toBeVisible();

    const circleciBadge = badges.locator('[data-testid="circleci-badge"]');
    await expect(circleciBadge).toBeVisible();
  });

  // Additional: Footer contains project description/tagline
  test('footer contains project tagline', async ({ page }) => {
    const footer = page.locator('footer');
    const tagline = footer.locator('.footer-tagline');
    await expect(tagline).toBeVisible();

    const text = await tagline.textContent();
    expect(text.toLowerCase()).toContain('persistent key-value store');
  });
});

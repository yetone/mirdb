// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * GitHub Repository Integration E2E Tests
 * Tests REQ-9 from PRD: Link to GitHub repository for source code access
 *
 * Test Cases:
 * TC1 (e2e): Find GitHub link in hero section
 * TC2 (e2e): Find GitHub link in footer
 */

test.describe('GitHub Repository Integration', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:8080');
  });

  // Test Case 1: Find GitHub link in hero section
  test('TC1: GitHub link exists in hero section', async ({ page }) => {
    // Find the hero section
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find GitHub link in hero section
    const githubLink = heroSection.locator('a').filter({ hasText: /GitHub/i });
    await expect(githubLink).toBeVisible();

    // Verify the link text contains 'GitHub'
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');
  });

  // Test Case 2: Find GitHub link in footer
  test('TC2: GitHub link exists in footer', async ({ page }) => {
    // Find the footer section
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find GitHub link in footer
    const githubLink = footer.locator('a').filter({ hasText: /GitHub/i });
    await expect(githubLink).toBeVisible();

    // Verify the link exists and contains GitHub text
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');

    // Verify it links to github.com
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
  });
});

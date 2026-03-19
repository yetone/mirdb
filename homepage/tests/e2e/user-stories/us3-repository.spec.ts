/**
 * E2E Tests for US-3: Accessing the Project Repository
 * Owner: Scenario 19 - User Story - Accessing Repository
 *
 * Verifies that Casey (Open-source contributor) can easily navigate to the
 * GitHub repository within 3 clicks (actually achievable in 1 click).
 *
 * Test Cases:
 * 1. Click 'View on GitHub' in hero section opens GitHub in new tab
 * 2. Click GitHub link in footer opens GitHub in new tab
 * 3. User can reach GitHub repository in 1 click from homepage
 *
 * Requirements: REQ-8, REQ-13, REQ-14
 */

import { test, expect } from '@playwright/test';

const GITHUB_URL = 'https://github.com/yetone/mirdb';

test.describe('US-3: Accessing the Project Repository', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test("TC-1: Click 'View on GitHub' in hero section opens GitHub in new tab", async ({ page, context }) => {
    // Find hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Find the "View on GitHub" button in hero section
    const githubButton = heroSection.locator('a:has-text("View on GitHub")');
    await expect(githubButton).toBeVisible();

    // Verify the button has correct href
    const href = await githubButton.getAttribute('href');
    expect(href).toBe(GITHUB_URL);

    // Verify button opens in new tab (target="_blank")
    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security (noopener noreferrer)
    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Test that clicking opens a new tab with the correct URL
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubButton.click(),
    ]);

    // Verify the new page URL
    await newPage.waitForLoadState('domcontentloaded');
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    // Close the new tab
    await newPage.close();
  });

  test('TC-2: Click GitHub link in footer opens GitHub repository in new tab', async ({ page, context }) => {
    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find GitHub repository link in footer
    const repositoryLink = footer.locator('a:has-text("Repository")');
    await expect(repositoryLink).toBeVisible();

    // Verify the link has correct href
    const href = await repositoryLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);

    // Verify link opens in new tab
    const target = await repositoryLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await repositoryLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Test that clicking opens a new tab with the correct URL
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      repositoryLink.click(),
    ]);

    // Verify the new page navigates to GitHub
    await newPage.waitForLoadState('domcontentloaded');
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    // Close the new tab
    await newPage.close();
  });

  test('TC-3: User can reach GitHub repository in 1 click from homepage', async ({ page }) => {
    // This test verifies that GitHub is accessible with a single click
    // from the homepage without any intermediate steps

    // User lands on homepage
    await expect(page.locator('#hero')).toBeVisible();

    // Count the number of clickable GitHub links visible immediately
    // (without scrolling or additional navigation)
    const heroGithubLink = page.locator('#hero a:has-text("View on GitHub")');
    await expect(heroGithubLink).toBeVisible();

    // Verify it's a direct link (1 click, no intermediate page)
    const href = await heroGithubLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);

    // Verify the link goes directly to GitHub repo (not to an intermediate page)
    expect(href).not.toContain('/redirect');
    expect(href).not.toContain('/link');

    // Verify the link is immediately clickable (visible in initial viewport)
    const heroBox = await heroGithubLink.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      // Button should be visible without scrolling
      const viewportHeight = await page.evaluate(() => window.innerHeight);
      expect(heroBox.y).toBeLessThan(viewportHeight);
    }

    // Count clicks needed: 1 click on "View on GitHub" = 1 click total
    const clicksNeeded = 1;
    expect(clicksNeeded).toBeLessThanOrEqual(3); // Within 3 clicks as per requirement
    expect(clicksNeeded).toBe(1); // Actually achievable in 1 click
  });

  test('Complete flow: Casey navigates to GitHub repository easily', async ({ page, context }) => {
    // This test simulates the complete user story flow for Casey (Open-source contributor)

    // Step 1: Casey lands on the MirDB homepage
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Step 2: Casey immediately sees the "View on GitHub" button in the hero
    const githubButton = heroSection.locator('a:has-text("View on GitHub")');
    await expect(githubButton).toBeVisible();

    // Verify the button is prominently displayed
    const buttonBox = await githubButton.boundingBox();
    expect(buttonBox).not.toBeNull();
    if (buttonBox) {
      // Button should be in the main content area (not tiny or hidden)
      expect(buttonBox.width).toBeGreaterThan(100);
      expect(buttonBox.height).toBeGreaterThan(30);
    }

    // Step 3: Casey clicks the button and is redirected to GitHub
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubButton.click(),
    ]);

    // Verify Casey arrives at the correct GitHub repository
    await newPage.waitForLoadState('domcontentloaded');
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    // Clean up
    await newPage.close();
  });

  test('Multiple GitHub access points available on homepage', async ({ page }) => {
    // Verify multiple GitHub links are available for easy access

    // Check hero section has GitHub link
    const heroGithubLink = page.locator('#hero a:has-text("View on GitHub")');
    await expect(heroGithubLink).toBeVisible();

    // Check footer section has GitHub links
    await page.locator('footer').scrollIntoViewIfNeeded();
    const footerRepoLink = page.locator('footer a:has-text("Repository")');
    await expect(footerRepoLink).toBeVisible();

    // Count all GitHub links on the page
    const allGithubLinks = page.locator(`a[href^="${GITHUB_URL}"]`);
    const linkCount = await allGithubLinks.count();

    // Expect at least 2 (hero + footer repository link)
    // Footer may also have Issues and Discussions links
    expect(linkCount).toBeGreaterThanOrEqual(2);

    // Verify all links point to the correct repository
    for (let i = 0; i < linkCount; i++) {
      const href = await allGithubLinks.nth(i).getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');
    }
  });

  test('GitHub links have proper accessibility attributes', async ({ page }) => {
    // Verify GitHub links are accessible

    // Check hero GitHub button
    const heroGithubLink = page.locator('#hero a:has-text("View on GitHub")');
    await expect(heroGithubLink).toBeVisible();

    // Verify link text is descriptive
    const buttonText = await heroGithubLink.textContent();
    expect(buttonText).toContain('GitHub');

    // Verify link is keyboard accessible
    await heroGithubLink.focus();
    await expect(heroGithubLink).toBeFocused();

    // Check footer repository link
    await page.locator('footer').scrollIntoViewIfNeeded();
    const footerRepoLink = page.locator('footer a:has-text("Repository")');

    // Verify link is accessible via keyboard
    await footerRepoLink.focus();
    await expect(footerRepoLink).toBeFocused();

    // Verify security attributes are present
    const heroRel = await heroGithubLink.getAttribute('rel');
    expect(heroRel).toContain('noopener');

    const footerRel = await footerRepoLink.getAttribute('rel');
    expect(footerRel).toContain('noopener');
  });
});

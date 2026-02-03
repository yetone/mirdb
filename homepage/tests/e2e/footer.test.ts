/**
 * E2E tests for Footer section and external links.
 * Owner: Scenario 6 - Footer and External Links
 *
 * Tests:
 * - Footer section displays correctly
 * - GitHub repository link works and opens in new tab
 * - License information is displayed
 * - CircleCI badge links to CircleCI project
 */

import { test, expect } from '@playwright/test';

test.describe('Footer and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
  });

  test('footer section is visible at the bottom of the page', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Check footer section exists and is visible
    await expect(footer).toBeVisible();

    // Verify it's a semantic footer element
    const tagName = await footer.evaluate((el) => el.tagName);
    expect(tagName).toBe('FOOTER');
  });

  test('footer displays GitHub link, license info, and author attribution', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Check GitHub link
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub Repository');

    // Check license info
    const licenseInfo = page.locator('[data-testid="license-info"]');
    await expect(licenseInfo).toBeVisible();
    await expect(licenseInfo).toContainText('MIT License');

    // Check author attribution
    const authorAttribution = page.locator('[data-testid="author-attribution"]');
    await expect(authorAttribution).toBeVisible();
    await expect(authorAttribution).toContainText('yetone');
  });

  test('clicking GitHub repository link opens github.com/yetone/mirdb in new tab', async ({ page, context }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Set up listener for new page (popup/new tab)
    const pagePromise = context.waitForEvent('page');

    // Get the GitHub link and verify attributes
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
    await expect(githubLink).toHaveAttribute('rel', /noreferrer/);

    // Click the GitHub link
    await githubLink.click();

    // Wait for the new tab to open
    const newPage = await pagePromise;

    // Verify the new tab URL is the GitHub repository
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
  });

  test('license type is displayed accurately', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify license info is displayed
    const licenseInfo = page.locator('[data-testid="license-info"]');
    await expect(licenseInfo).toBeVisible();
    await expect(licenseInfo).toContainText('MIT License');

    // Verify license link
    const licenseLink = page.locator('[data-testid="license-link"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE');
    await expect(licenseLink).toContainText('MIT License');
  });

  test('CircleCI badge links to circleci.com/gh/yetone/mirdb', async ({ page, context }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Set up listener for new page (popup/new tab)
    const pagePromise = context.waitForEvent('page');

    // Get the CircleCI badge link and verify attributes
    const circleciBadgeLink = page.locator('[data-testid="circleci-badge-link"]');
    await expect(circleciBadgeLink).toBeVisible();
    await expect(circleciBadgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    await expect(circleciBadgeLink).toHaveAttribute('target', '_blank');
    await expect(circleciBadgeLink).toHaveAttribute('rel', /noopener/);
    await expect(circleciBadgeLink).toHaveAttribute('rel', /noreferrer/);

    // Verify the badge image is present
    const badge = page.locator('[data-testid="circleci-badge"]');
    await expect(badge).toBeVisible();
    await expect(badge).toHaveAttribute('alt', 'CircleCI Build Status');

    // Click the CircleCI badge
    await circleciBadgeLink.click();

    // Wait for the new tab to open
    const newPage = await pagePromise;

    // Verify the new tab URL contains CircleCI (may redirect to app.circleci.com/pipelines/github/)
    expect(newPage.url()).toMatch(/circleci\.com\/(gh|pipelines\/github)\/yetone\/mirdb/);
  });

  test('author attribution links to yetone GitHub profile', async ({ page, context }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Set up listener for new page
    const pagePromise = context.waitForEvent('page');

    // Get the author link and verify attributes
    const authorLink = page.locator('[data-testid="author-link"]');
    await expect(authorLink).toBeVisible();
    await expect(authorLink).toHaveAttribute('href', 'https://github.com/yetone');
    await expect(authorLink).toHaveAttribute('target', '_blank');
    await expect(authorLink).toContainText('yetone');

    // Click the author link
    await authorLink.click();

    // Wait for the new tab to open
    const newPage = await pagePromise;

    // Verify the new tab URL is yetone's GitHub profile
    expect(newPage.url()).toContain('github.com/yetone');
  });

  test('footer has three main sections: Project, Resources, and Legal', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Check for three main sections
    const projectSection = page.locator('[data-testid="footer-project"]');
    const resourcesSection = page.locator('[data-testid="footer-resources"]');
    const legalSection = page.locator('[data-testid="footer-legal"]');

    await expect(projectSection).toBeVisible();
    await expect(resourcesSection).toBeVisible();
    await expect(legalSection).toBeVisible();

    // Verify section headings
    await expect(projectSection.locator('h2')).toContainText('MirDB');
    await expect(resourcesSection.locator('h2')).toContainText('Resources');
    await expect(legalSection.locator('h2')).toContainText('Legal');
  });

  test('footer displays correctly on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // All sections should be visible
    await expect(footer).toBeVisible();

    const projectSection = page.locator('[data-testid="footer-project"]');
    const resourcesSection = page.locator('[data-testid="footer-resources"]');
    const legalSection = page.locator('[data-testid="footer-legal"]');

    await expect(projectSection).toBeVisible();
    await expect(resourcesSection).toBeVisible();
    await expect(legalSection).toBeVisible();

    // All key elements should still be visible
    const githubLink = page.locator('[data-testid="github-link"]');
    const licenseInfo = page.locator('[data-testid="license-info"]');
    const authorAttribution = page.locator('[data-testid="author-attribution"]');

    await expect(githubLink).toBeVisible();
    await expect(licenseInfo).toBeVisible();
    await expect(authorAttribution).toBeVisible();
  });

  test('footer displays correctly on desktop viewport', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });

    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    await expect(footer).toBeVisible();

    // All sections should be in a horizontal grid on desktop
    const projectSection = page.locator('[data-testid="footer-project"]');
    const resourcesSection = page.locator('[data-testid="footer-resources"]');
    const legalSection = page.locator('[data-testid="footer-legal"]');

    await expect(projectSection).toBeVisible();
    await expect(resourcesSection).toBeVisible();
    await expect(legalSection).toBeVisible();
  });

  test('copyright notice is displayed', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    const copyright = page.locator('[data-testid="copyright"]');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MirDB');
    await expect(copyright).toContainText('All rights reserved');
  });
});

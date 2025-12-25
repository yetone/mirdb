// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Navigation and Links Test Suite
 * Scenario: Verify all navigation links work correctly and lead to expected destinations
 */

// Test Case 1: GitHub repository link navigation (accessible in 2 or fewer clicks)
test('TC1: GitHub repository link navigates to MirDB repository', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check secondary CTA (GitHub button in hero section) - 1 click access
  const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
  await expect(secondaryCTA).toBeVisible();

  // Verify it links to GitHub MirDB repository
  const href = await secondaryCTA.getAttribute('href');
  expect(href).toContain('github.com');
  expect(href.toLowerCase()).toContain('mirdb');

  // Verify it's accessible in 1 click (direct link from hero)
  const tagName = await secondaryCTA.evaluate(el => el.tagName.toLowerCase());
  expect(tagName).toBe('a');

  // Verify external link attributes
  const target = await secondaryCTA.getAttribute('target');
  const rel = await secondaryCTA.getAttribute('rel');
  expect(target).toBe('_blank');
  expect(rel).toContain('noopener');
  expect(rel).toContain('noreferrer');

  // Also check header navigation GitHub link (another 1-click access option)
  const headerGithubLink = page.locator('.nav-links a[href*="github.com"]');
  await expect(headerGithubLink).toBeVisible();
});

// Test Case 2: Documentation link navigation
test('TC2: Documentation link navigates to documentation page', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check footer documentation link
  const docsLink = page.locator('[data-testid="footer-docs-link"]');
  await expect(docsLink).toBeVisible();

  // Verify it has a valid documentation link
  const href = await docsLink.getAttribute('href');
  expect(href).toBeTruthy();
  expect(href.length).toBeGreaterThan(0);

  // Verify the link text mentions documentation
  const linkText = await docsLink.textContent();
  expect(linkText.toLowerCase()).toContain('documentation');

  // Verify external link attributes
  const target = await docsLink.getAttribute('target');
  const rel = await docsLink.getAttribute('rel');
  expect(target).toBe('_blank');
  expect(rel).toContain('noopener');
  expect(rel).toContain('noreferrer');
});

// Test Case 3: Footer contains required links (GitHub, documentation, license)
test('TC3: Footer contains links to GitHub, documentation, and license', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check footer section exists
  const footer = page.locator('[data-testid="footer"]');
  await expect(footer).toBeVisible();

  // Check footer links container
  const footerLinks = page.locator('[data-testid="footer-links"]');
  await expect(footerLinks).toBeVisible();

  // Verify GitHub link in footer
  const githubLink = page.locator('[data-testid="footer-github-link"]');
  await expect(githubLink).toBeVisible();
  const githubHref = await githubLink.getAttribute('href');
  expect(githubHref).toContain('github.com');
  expect(githubHref.toLowerCase()).toContain('mirdb');

  // Verify Documentation link in footer
  const docsLink = page.locator('[data-testid="footer-docs-link"]');
  await expect(docsLink).toBeVisible();
  const docsHref = await docsLink.getAttribute('href');
  expect(docsHref).toBeTruthy();
  const docsText = await docsLink.textContent();
  expect(docsText.toLowerCase()).toContain('documentation');

  // Verify License link in footer
  const licenseLink = page.locator('[data-testid="footer-license-link"]');
  await expect(licenseLink).toBeVisible();
  const licenseHref = await licenseLink.getAttribute('href');
  expect(licenseHref).toBeTruthy();
  const licenseText = await licenseLink.textContent();
  expect(licenseText.toLowerCase()).toContain('license');
});

// Test Case 4: Project status indicator is displayed in footer
test('TC4: Project status indicator is displayed in footer', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check footer exists
  const footer = page.locator('[data-testid="footer"]');
  await expect(footer).toBeVisible();

  // Check project status indicator exists
  const projectStatus = page.locator('[data-testid="project-status"]');
  await expect(projectStatus).toBeVisible();

  // Verify status contains some text indicating project status
  const statusText = await projectStatus.textContent();
  expect(statusText).toBeTruthy();
  expect(statusText.trim().length).toBeGreaterThan(0);

  // Verify status indicator element is present
  const statusIndicator = projectStatus.locator('.status-indicator');
  await expect(statusIndicator).toBeVisible();
});

// Test Case 5: All external links have proper security attributes
test('TC5: External links have target="_blank" and rel="noopener noreferrer"', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Get all external links (links with http/https href)
  const externalLinks = page.locator('a[href^="http"]');
  const linkCount = await externalLinks.count();

  expect(linkCount).toBeGreaterThan(0);

  // Check each external link has proper attributes
  for (let i = 0; i < linkCount; i++) {
    const link = externalLinks.nth(i);
    const href = await link.getAttribute('href');
    const target = await link.getAttribute('target');
    const rel = await link.getAttribute('rel');

    // External links should open in new tab
    expect(target).toBe('_blank');

    // External links should have noopener noreferrer for security
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  }
});

// Additional test: Verify all navigation is accessible within 2 clicks
test('Navigation: GitHub accessible within 2 clicks', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Hero section GitHub button is directly visible - 1 click access
  const heroGithubCTA = page.locator('[data-testid="secondary-cta"]');
  await expect(heroGithubCTA).toBeVisible();

  // Header navigation GitHub link - also 1 click access
  const headerGithubLink = page.locator('.nav-links a[href*="github.com"]');
  await expect(headerGithubLink).toBeVisible();

  // Footer GitHub link is also directly visible - 1 click access
  const footerGithub = page.locator('[data-testid="footer-github-link"]');
  await expect(footerGithub).toBeVisible();

  // All three provide single-click access to GitHub
  // This satisfies the "2 or fewer clicks" requirement
});

// Additional test: Primary navigation elements are present and visible
test('Navigation: All primary navigation elements are visible', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Header navigation
  const headerNav = page.locator('.nav-links');
  await expect(headerNav).toBeVisible();

  // Hero section CTA buttons
  const primaryCTA = page.locator('[data-testid="primary-cta"]');
  const secondaryCTA = page.locator('[data-testid="secondary-cta"]');

  await expect(primaryCTA).toBeVisible();
  await expect(secondaryCTA).toBeVisible();

  // Footer navigation
  const footer = page.locator('[data-testid="footer"]');
  const footerLinks = page.locator('[data-testid="footer-links"]');

  await expect(footer).toBeVisible();
  await expect(footerLinks).toBeVisible();
});

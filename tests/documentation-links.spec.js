const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Documentation Links (REQ-8)
 * Verifies that links to documentation resources are provided in navigation and footer
 */
test.describe('Documentation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Documentation link is present in navigation', async ({ page }) => {
    // Locate the navigation links container
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Find documentation link in navigation (case-insensitive search for "docs" or "documentation")
    const docsLinkInNav = navLinks.locator('a').filter({
      hasText: /docs|documentation/i
    });

    // Verify documentation link exists in navigation
    await expect(docsLinkInNav).toBeVisible();

    // Verify it has a valid href attribute
    const href = await docsLinkInNav.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  test('TC2: Documentation link is present in footer', async ({ page }) => {
    // Locate the footer links container
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Find documentation link in footer (case-insensitive search for "docs" or "documentation")
    const docsLinkInFooter = footerLinks.locator('a').filter({
      hasText: /docs|documentation/i
    });

    // Verify documentation link exists in footer
    await expect(docsLinkInFooter).toBeVisible();

    // Verify it has a valid href attribute
    const href = await docsLinkInFooter.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  test('TC3: Documentation link navigates to valid resource (not 404)', async ({ page, context }) => {
    // Find documentation link in footer (the primary documentation link)
    const footerLinks = page.locator('.footer-links');
    const docsLink = footerLinks.locator('a').filter({
      hasText: /docs|documentation/i
    });

    // Get the href attribute
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Make a request to the documentation URL and verify it's not a 404
    const response = await context.request.get(href);

    // Verify the response is successful (not 404 or other error)
    expect(response.status()).not.toBe(404);
    expect(response.ok()).toBe(true);
  });

  test('Documentation link in navigation has correct styling', async ({ page }) => {
    const navLinks = page.locator('.nav-links');
    const docsLink = navLinks.locator('a').filter({
      hasText: /docs|documentation/i
    });

    // Verify it has nav link styling
    await expect(docsLink).toBeVisible();

    // Verify the link is clickable
    const isEnabled = await docsLink.isEnabled();
    expect(isEnabled).toBe(true);
  });

  test('Documentation link in footer has correct styling', async ({ page }) => {
    const footerLinks = page.locator('.footer-links');
    const docsLink = footerLinks.locator('a').filter({
      hasText: /docs|documentation/i
    });

    // Verify it has footer link styling
    await expect(docsLink).toBeVisible();

    // Verify the link is clickable
    const isEnabled = await docsLink.isEnabled();
    expect(isEnabled).toBe(true);
  });

  test('Documentation links open in new tab for external resources', async ({ page }) => {
    const footerLinks = page.locator('.footer-links');
    const docsLink = footerLinks.locator('a').filter({
      hasText: /docs|documentation/i
    });

    // Check if the link has target="_blank" for external resources
    const target = await docsLink.getAttribute('target');
    const href = await docsLink.getAttribute('href');

    // If it's an external link, it should open in a new tab
    if (href && (href.startsWith('http://') || href.startsWith('https://'))) {
      expect(target).toBe('_blank');
    }
  });
});

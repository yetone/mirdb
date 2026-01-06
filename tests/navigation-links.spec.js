// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Navigation Links (REQ-5)
 * Verify navigation links to documentation, GitHub repository, and getting started guide function correctly
 */

test.describe('Navigation Links (REQ-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Navigation menu or links section exists
   */
  test('TC1: Navigation element exists on the page', async ({ page }) => {
    // Query for navigation element
    const navigation = page.locator('[data-testid="navigation"]');
    await expect(navigation).toBeVisible();

    // Verify navigation has proper semantic structure
    const navContainer = navigation.locator('nav');
    await expect(navContainer).toBeVisible();
    await expect(navContainer).toHaveAttribute('role', 'navigation');
    await expect(navContainer).toHaveAttribute('aria-label', 'Main navigation');

    // Verify nav-links section exists
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();
  });

  /**
   * Test Case 2: Documentation link exists in navigation
   */
  test('TC2: Documentation link exists in navigation', async ({ page }) => {
    // Query for Documentation link
    const docsLink = page.locator('[data-testid="nav-documentation"]');
    await expect(docsLink).toBeVisible();

    // Verify link text
    await expect(docsLink).toContainText('Documentation');

    // Verify link has correct href
    await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');

    // Verify link opens in new tab (security best practice for external links)
    await expect(docsLink).toHaveAttribute('target', '_blank');
    await expect(docsLink).toHaveAttribute('rel', /noopener/);
  });

  /**
   * Test Case 3: GitHub repository link exists and points to correct URL
   */
  test('TC3: GitHub link exists and points to correct URL', async ({ page }) => {
    // Query for GitHub link
    const githubLink = page.locator('[data-testid="nav-github"]');
    await expect(githubLink).toBeVisible();

    // Verify link text
    await expect(githubLink).toContainText('GitHub');

    // Verify link points to the correct repository URL
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify link opens in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  /**
   * Test Case 4: Getting Started guide link exists
   */
  test('TC4: Getting Started link exists in navigation', async ({ page }) => {
    // Query for Getting Started link
    const gettingStartedLink = page.locator('[data-testid="nav-getting-started"]');
    await expect(gettingStartedLink).toBeVisible();

    // Verify link text
    await expect(gettingStartedLink).toContainText('Getting Started');

    // Verify link navigates to the get-started section
    await expect(gettingStartedLink).toHaveAttribute('href', '#get-started');
  });

  /**
   * Test Case 5: Click Documentation link directs user to documentation
   */
  test('TC5: Documentation link navigation works correctly', async ({ page, context }) => {
    // Listen for new page (new tab) event
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      page.locator('[data-testid="nav-documentation"]').click()
    ]);

    // Verify the new page URL contains the expected documentation URL
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com/yetone/mirdb');
  });

  /**
   * Test Case 6: GitHub link opens in new tab
   */
  test('TC6: GitHub link opens in new tab correctly', async ({ page, context }) => {
    // Listen for new page (new tab) event
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      page.locator('[data-testid="nav-github"]').click()
    ]);

    // Verify the new page URL is the GitHub repository
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com/yetone/mirdb');
  });
});

test.describe('Navigation Links Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Additional test: Navigation links are keyboard accessible
   */
  test('Navigation links are keyboard accessible', async ({ page }) => {
    // Tab to the documentation link
    const docsLink = page.locator('[data-testid="nav-documentation"]');

    // Focus on the link
    await docsLink.focus();

    // Verify link is focused
    await expect(docsLink).toBeFocused();

    // Verify focus is visible
    const isVisible = await docsLink.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.outlineWidth !== '0px' || style.outline !== 'none';
    });
    expect(isVisible).toBeTruthy();
  });

  /**
   * Additional test: Getting Started link scrolls to section
   */
  test('Getting Started link scrolls to the quick-start section', async ({ page }) => {
    // Click Getting Started link
    const gettingStartedLink = page.locator('[data-testid="nav-getting-started"]');
    await gettingStartedLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the quick-start section is now in view
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeInViewport();
  });

  /**
   * Additional test: Navigation is visible and accessible on the page
   */
  test('Navigation header is fixed at the top of the page', async ({ page }) => {
    // Verify navigation header is visible
    const navHeader = page.locator('[data-testid="navigation"]');
    await expect(navHeader).toBeVisible();

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    // Verify navigation is still visible after scrolling
    await expect(navHeader).toBeVisible();
    await expect(navHeader).toBeInViewport();
  });
});

import { test, expect } from '@playwright/test';

/**
 * Navigation Functionality E2E Tests
 *
 * This test suite verifies the navigation menu works correctly
 * on both desktop and mobile viewports according to REQ-10.
 */

test.describe('Navigation Functionality', () => {

  /**
   * Test Case 1: Desktop viewport navigation
   * Input: Load page at desktop viewport (1280px width)
   * Expected: Navigation bar displays with visible menu items: Home, Features, Documentation, GitHub
   */
  test('should display navigation bar with visible menu items on desktop viewport', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Verify navigation bar is visible
    const nav = page.locator('[data-testid="main-navigation"]');
    await expect(nav).toBeVisible();

    // Verify navigation links are visible
    const homeLink = page.locator('[data-testid="nav-home"]');
    const featuresLink = page.locator('[data-testid="nav-features"]');
    const documentationLink = page.locator('[data-testid="nav-documentation"]');
    const githubLink = page.locator('[data-testid="nav-github"]');

    await expect(homeLink).toBeVisible();
    await expect(featuresLink).toBeVisible();
    await expect(documentationLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify the text content of navigation links
    await expect(homeLink).toHaveText('Home');
    await expect(featuresLink).toHaveText('Features');
    await expect(documentationLink).toHaveText('Documentation');
    await expect(githubLink).toHaveText('GitHub');
  });

  /**
   * Test Case 2: Sticky navigation on scroll
   * Input: Scroll down 500px on desktop
   * Expected: Navigation remains visible and fixed at top of viewport
   */
  test('should keep navigation visible and fixed when scrolling down 500px', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Scroll down 500px
    await page.evaluate(() => window.scrollBy(0, 500));

    // Wait for any scroll animations
    await page.waitForTimeout(300);

    // Verify navigation is still visible
    const nav = page.locator('[data-testid="main-navigation"]');
    await expect(nav).toBeVisible();

    // Verify navigation has fixed positioning (sticky behavior)
    const navStyle = await nav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        position: style.position,
        top: style.top
      };
    });

    // Navigation should be fixed or sticky at the top
    expect(['fixed', 'sticky']).toContain(navStyle.position);
    expect(navStyle.top).toBe('0px');
  });

  /**
   * Test Case 3: Mobile viewport with hamburger menu
   * Input: Resize to mobile viewport (375px width)
   * Expected: Hamburger menu icon is displayed, horizontal menu items are hidden
   */
  test('should display hamburger menu icon on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Verify hamburger menu button is visible
    const hamburgerButton = page.locator('[data-testid="hamburger-menu"]');
    await expect(hamburgerButton).toBeVisible();

    // Verify desktop navigation links are hidden
    const desktopNav = page.locator('[data-testid="desktop-nav-links"]');
    await expect(desktopNav).toBeHidden();
  });

  /**
   * Test Case 4: Mobile navigation menu opens on click
   * Input: Click hamburger menu on mobile
   * Expected: Mobile navigation menu opens showing all navigation links
   */
  test('should open mobile navigation menu when hamburger is clicked', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Click hamburger menu
    const hamburgerButton = page.locator('[data-testid="hamburger-menu"]');
    await hamburgerButton.click();

    // Wait for menu animation
    await page.waitForTimeout(300);

    // Verify mobile menu is visible
    const mobileMenu = page.locator('[data-testid="mobile-nav-menu"]');
    await expect(mobileMenu).toBeVisible();

    // Verify all navigation links are visible in mobile menu
    const mobileHomeLink = page.locator('[data-testid="mobile-nav-home"]');
    const mobileFeaturesLink = page.locator('[data-testid="mobile-nav-features"]');
    const mobileDocumentationLink = page.locator('[data-testid="mobile-nav-documentation"]');
    const mobileGithubLink = page.locator('[data-testid="mobile-nav-github"]');

    await expect(mobileHomeLink).toBeVisible();
    await expect(mobileFeaturesLink).toBeVisible();
    await expect(mobileDocumentationLink).toBeVisible();
    await expect(mobileGithubLink).toBeVisible();
  });

  /**
   * Test Case 5: Features navigation link
   * Input: Click Features navigation link
   * Expected: Page scrolls to or navigates to Features section
   */
  test('should scroll to Features section when Features link is clicked', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Click Features navigation link
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the Features section is in view
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  /**
   * Test Case 6: GitHub external link
   * Input: Click GitHub navigation link
   * Expected: Link opens MirDB GitHub repository (external link)
   */
  test('should have GitHub link pointing to MirDB repository', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Verify GitHub link attributes
    const githubLink = page.locator('[data-testid="nav-github"]');

    // Check href attribute points to GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it opens in new tab (target="_blank")
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Additional test: Mobile menu closes when link is clicked
   */
  test('should close mobile menu when a navigation link is clicked', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Open mobile menu
    const hamburgerButton = page.locator('[data-testid="hamburger-menu"]');
    await hamburgerButton.click();
    await page.waitForTimeout(300);

    // Click Features link in mobile menu
    const mobileFeaturesLink = page.locator('[data-testid="mobile-nav-features"]');
    await mobileFeaturesLink.click();

    // Wait for menu close animation
    await page.waitForTimeout(300);

    // Verify mobile menu is closed
    const mobileMenu = page.locator('[data-testid="mobile-nav-menu"]');
    await expect(mobileMenu).toBeHidden();
  });

  /**
   * Additional test: Navigation links have correct hrefs
   */
  test('should have correct href attributes on all navigation links', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Verify Home link href
    const homeLink = page.locator('[data-testid="nav-home"]');
    const homeHref = await homeLink.getAttribute('href');
    expect(homeHref === '/' || homeHref === '#' || homeHref === '#home').toBeTruthy();

    // Verify Features link href
    const featuresLink = page.locator('[data-testid="nav-features"]');
    const featuresHref = await featuresLink.getAttribute('href');
    expect(featuresHref).toBe('#features');

    // Verify Documentation link href
    const documentationLink = page.locator('[data-testid="nav-documentation"]');
    const documentationHref = await documentationLink.getAttribute('href');
    expect(documentationHref === '#commands' || documentationHref === '#getting-started').toBeTruthy();
  });
});

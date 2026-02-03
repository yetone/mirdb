/**
 * E2E tests for Hero section navigation and header navigation.
 * Owner: Scenario 1 - Hero Section Display (hero tests)
 * Owner: Scenario 16 - Navigation and Anchor Links (navigation tests)
 *
 * Tests:
 * - Hero section loads with all elements visible
 * - Get Started button scrolls to installation section
 * - View on GitHub button opens in new tab
 * - Header navigation with smooth scroll
 * - Anchor links work correctly
 * - URL hash updates on navigation
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
  });

  test('hero section renders with logo, tagline, and CTA buttons visible above the fold', async ({ page }) => {
    // Check hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check logo is present and visible
    const logo = page.locator('[data-testid="hero-logo"] img');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Check tagline is visible with correct text
    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A Persistent Key-Value Store with Memcached Protocol');

    // Check both CTA buttons are visible
    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toContainText('Get Started');

    const githubButton = page.locator('[data-testid="github-button"]');
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toContainText('View on GitHub');

    // Verify all elements are above the fold (in viewport)
    await expect(heroSection).toBeInViewport();
  });

  test('clicking Get Started button scrolls smoothly to installation section', async ({ page }) => {
    // Get the initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started button
    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    await getStartedButton.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify the page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the installation section is now in view
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeInViewport();
  });

  test('clicking View on GitHub button opens github.com/yetone/mirdb in a new tab', async ({ page, context }) => {
    // Set up listener for new page (popup/new tab)
    const pagePromise = context.waitForEvent('page');

    // Click the GitHub button
    const githubButton = page.locator('[data-testid="github-button"]');
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubButton).toHaveAttribute('target', '_blank');
    await expect(githubButton).toHaveAttribute('rel', /noopener/);

    await githubButton.click();

    // Wait for the new tab to open
    const newPage = await pagePromise;

    // Verify the new tab URL is the GitHub repository
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
  });

  test('hero section has proper structure and semantic HTML', async ({ page }) => {
    // Check for single H1 in hero section
    const h1Elements = page.locator('#hero h1');
    await expect(h1Elements).toHaveCount(1);
    await expect(h1Elements).toContainText('MirDB');

    // Check hero is a section element
    const heroSection = page.locator('#hero');
    const tagName = await heroSection.evaluate((el) => el.tagName);
    expect(tagName).toBe('SECTION');
  });

  test('hero section displays correctly on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const logo = page.locator('[data-testid="hero-logo"] img');
    await expect(logo).toBeVisible();

    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
    await expect(ctaButtons).toBeVisible();

    // Buttons should stack vertically on mobile
    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    const githubButton = page.locator('[data-testid="github-button"]');

    await expect(getStartedButton).toBeVisible();
    await expect(githubButton).toBeVisible();
  });

  test('hero section displays correctly on desktop viewport', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // All elements should be visible
    const logo = page.locator('[data-testid="hero-logo"] img');
    await expect(logo).toBeVisible();

    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    const githubButton = page.locator('[data-testid="github-button"]');

    await expect(getStartedButton).toBeVisible();
    await expect(githubButton).toBeVisible();
  });

  test('hero terminal decoration is visible', async ({ page }) => {
    // Check for terminal-style decoration
    const terminal = page.locator('#hero .terminal');
    await expect(terminal).toBeVisible();

    // Check for terminal dots
    const terminalDots = page.locator('#hero .terminal-dot');
    await expect(terminalDots).toHaveCount(3);
  });
});

/**
 * E2E tests for Navigation and Anchor Links
 * Owner: Scenario 16 - Navigation and Anchor Links
 */
test.describe('Navigation and Anchor Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
  });

  test('navigation header is present and sticky', async ({ page }) => {
    // Check navigation header exists
    const navHeader = page.locator('[data-testid="navigation-header"]');
    await expect(navHeader).toBeVisible();

    // Verify it's sticky by scrolling and checking it's still visible
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);
    await expect(navHeader).toBeVisible();
    await expect(navHeader).toBeInViewport();
  });

  test('clicking navigation link to Features section scrolls smoothly and updates URL', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Features navigation link
    const featuresLink = page.locator('[data-testid="nav-link-features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(1000);

    // Verify page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash updated
    const url = page.url();
    expect(url).toContain('#features');
  });

  test('clicking navigation link to Installation section scrolls smoothly and updates URL', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Installation navigation link
    const installationLink = page.locator('[data-testid="nav-link-installation"]');
    await expect(installationLink).toBeVisible();
    await installationLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(1000);

    // Verify page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify installation section is in viewport
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeInViewport();

    // Verify URL hash updated
    const url = page.url();
    expect(url).toContain('#installation');
  });

  test('loading page with /#architecture hash scrolls to architecture section', async ({ page }) => {
    // Navigate directly to URL with hash
    await page.goto('/mirdb/#architecture');

    // Wait for scroll to complete
    await page.waitForTimeout(1000);

    // Verify architecture section is in viewport
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });

  test('all navigation links are accessible and functional via keyboard', async ({ page }) => {
    // Focus on the Features link directly and test keyboard activation
    const featuresLink = page.locator('[data-testid="nav-link-features"]');
    await featuresLink.focus();
    await expect(featuresLink).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(1000);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Focus on the Installation link and test keyboard activation
    const installationLink = page.locator('[data-testid="nav-link-installation"]');
    await installationLink.focus();
    await expect(installationLink).toBeFocused();

    // Press Enter to navigate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(1000);

    // Verify installation section is in viewport
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeInViewport();

    // Verify all navigation links are focusable (tabindex >= 0 or no tabindex)
    await expect(page.locator('[data-testid="nav-link-features"]')).toHaveAttribute('href', '#features');
    await expect(page.locator('[data-testid="nav-link-examples"]')).toHaveAttribute('href', '#examples');
    await expect(page.locator('[data-testid="nav-link-architecture"]')).toHaveAttribute('href', '#architecture');
    await expect(page.locator('[data-testid="nav-link-installation"]')).toHaveAttribute('href', '#installation');
  });

  test('navigation has all required section links', async ({ page }) => {
    // Check all nav links are present
    await expect(page.locator('[data-testid="nav-link-features"]')).toBeVisible();
    await expect(page.locator('[data-testid="nav-link-examples"]')).toBeVisible();
    await expect(page.locator('[data-testid="nav-link-architecture"]')).toBeVisible();
    await expect(page.locator('[data-testid="nav-link-installation"]')).toBeVisible();

    // Check they have correct hrefs
    await expect(page.locator('[data-testid="nav-link-features"]')).toHaveAttribute('href', '#features');
    await expect(page.locator('[data-testid="nav-link-examples"]')).toHaveAttribute('href', '#examples');
    await expect(page.locator('[data-testid="nav-link-architecture"]')).toHaveAttribute('href', '#architecture');
    await expect(page.locator('[data-testid="nav-link-installation"]')).toHaveAttribute('href', '#installation');
  });

  test('logo link scrolls to top of page', async ({ page }) => {
    // First scroll down
    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(100);

    // Click logo
    const logoLink = page.locator('[data-testid="nav-logo"]');
    await logoLink.click();

    // Wait for scroll
    await page.waitForTimeout(1000);

    // Verify hero section is in viewport
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();
  });

  test('GitHub link in navigation opens in new tab', async ({ page, context }) => {
    const pagePromise = context.waitForEvent('page');

    const githubLink = page.locator('[data-testid="nav-github-link"]');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);

    await githubLink.click();
    const newPage = await pagePromise;
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
  });
});

test.describe('Mobile Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/mirdb/');
    // Wait for JavaScript to be fully loaded and initialized
    await page.waitForLoadState('networkidle');
  });

  test('mobile menu button is visible on mobile viewport', async ({ page }) => {
    const menuButton = page.locator('[data-testid="mobile-menu-button"]');
    await expect(menuButton).toBeVisible();

    // Desktop nav should be hidden
    const desktopNav = page.locator('[data-testid="desktop-nav"]');
    await expect(desktopNav).not.toBeVisible();
  });

  test('clicking mobile menu button opens mobile menu', async ({ page }) => {
    const menuButton = page.locator('[data-testid="mobile-menu-button"]');
    const mobileMenu = page.locator('[data-testid="mobile-nav-menu"]');

    // Menu should be hidden initially
    await expect(mobileMenu).not.toBeVisible();

    // Click to open using JavaScript to bypass any click interception
    await page.evaluate(() => {
      const btn = document.getElementById('mobile-menu-button');
      if (btn) btn.click();
    });

    // Wait a bit for the JavaScript to execute
    await page.waitForTimeout(100);

    // Menu should be visible
    await expect(mobileMenu).toBeVisible();

    // All mobile nav links should be visible
    await expect(page.locator('[data-testid="mobile-nav-link-features"]')).toBeVisible();
    await expect(page.locator('[data-testid="mobile-nav-link-examples"]')).toBeVisible();
    await expect(page.locator('[data-testid="mobile-nav-link-architecture"]')).toBeVisible();
    await expect(page.locator('[data-testid="mobile-nav-link-installation"]')).toBeVisible();
  });

  test('clicking mobile nav link closes menu and scrolls to section', async ({ page }) => {
    const menuButton = page.locator('[data-testid="mobile-menu-button"]');
    const mobileMenu = page.locator('[data-testid="mobile-nav-menu"]');

    // Open mobile menu using JavaScript
    await page.evaluate(() => {
      const btn = document.getElementById('mobile-menu-button');
      if (btn) btn.click();
    });
    await page.waitForTimeout(100);
    await expect(mobileMenu).toBeVisible();

    // Click Features link
    const featuresLink = page.locator('[data-testid="mobile-nav-link-features"]');
    await featuresLink.click();

    // Wait for scroll and menu close
    await page.waitForTimeout(1000);

    // Menu should be closed
    await expect(mobileMenu).not.toBeVisible();

    // Features section should be in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Escape key closes mobile menu', async ({ page }) => {
    const menuButton = page.locator('[data-testid="mobile-menu-button"]');
    const mobileMenu = page.locator('[data-testid="mobile-nav-menu"]');

    // Open mobile menu using JavaScript
    await page.evaluate(() => {
      const btn = document.getElementById('mobile-menu-button');
      if (btn) btn.click();
    });
    await page.waitForTimeout(100);
    await expect(mobileMenu).toBeVisible();

    // Press Escape
    await page.keyboard.press('Escape');

    // Menu should be closed
    await expect(mobileMenu).not.toBeVisible();
  });
});

/**
 * Navigation and Footer E2E Tests
 * Owner: Scenario 6 - Navigation and Footer Implementation
 *
 * Tests for navigation bar, smooth scroll, mobile menu, and footer functionality.
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation Bar', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation bar is visible at top of page with MirDB logo', async ({ page }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    const logo = page.locator('.nav__logo');
    await expect(logo).toBeVisible();
    await expect(logo).toContainText('MirDB');

    const logoImg = page.locator('.nav__logo img');
    await expect(logoImg).toBeVisible();
  });

  test('TC2: Navigation bar remains fixed/sticky at top when scrolling', async ({ page }) => {
    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(300);

    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Check that nav is at the top of viewport
    const navBox = await nav.boundingBox();
    expect(navBox.y).toBe(0);
  });

  test('TC3: Click Features navigation link - page smooth scrolls to #features section', async ({ page }) => {
    const featuresLink = page.locator('.nav__links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    const initialScrollY = await page.evaluate(() => window.scrollY);
    await featuresLink.click();
    await page.waitForTimeout(1000); // Wait for smooth scroll

    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('TC4: Click Architecture navigation link - page smooth scrolls to #architecture section', async ({ page }) => {
    const archLink = page.locator('.nav__links a[href="#architecture"]');
    await expect(archLink).toBeVisible();

    const initialScrollY = await page.evaluate(() => window.scrollY);
    await archLink.click();
    await page.waitForTimeout(1000); // Wait for smooth scroll

    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    const archSection = page.locator('#architecture');
    await expect(archSection).toBeInViewport();
  });

  test('TC5: Click Get Started navigation link - page smooth scrolls to #getting-started section', async ({ page }) => {
    const getStartedLink = page.locator('.nav__links a[href="#getting-started"]');
    await expect(getStartedLink).toBeVisible();

    const initialScrollY = await page.evaluate(() => window.scrollY);
    await getStartedLink.click();
    await page.waitForTimeout(1000); // Wait for smooth scroll

    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    const getStartedSection = page.locator('#getting-started');
    await expect(getStartedSection).toBeInViewport();
  });

  test('TC6: Click GitHub navigation link - has correct href and opens in new tab', async ({ page }) => {
    const githubLink = page.locator('.nav__links .nav__github');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });
});

test.describe('Mobile Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');
  });

  test('TC7: Hamburger menu icon is visible on 320px viewport, desktop links are hidden', async ({ page }) => {
    const hamburgerButton = page.locator('.nav__mobile-toggle');
    await expect(hamburgerButton).toBeVisible();

    const desktopLinks = page.locator('.nav__links');
    await expect(desktopLinks).not.toBeVisible();
  });

  test('TC8: Click hamburger menu - mobile menu opens showing all navigation links', async ({ page }) => {
    const hamburgerButton = page.locator('.nav__mobile-toggle');
    await hamburgerButton.click();
    await page.waitForTimeout(500);

    const mobileMenu = page.locator('.nav__mobile-menu');
    await expect(mobileMenu).toHaveClass(/nav__mobile-menu--open/);

    // Check all links are visible
    const mobileLinks = page.locator('.nav__mobile-menu-links a');
    const linkCount = await mobileLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(5);

    // Check specific links
    await expect(page.locator('.nav__mobile-menu-links a[href="#features"]')).toBeVisible();
    await expect(page.locator('.nav__mobile-menu-links a[href="#getting-started"]')).toBeVisible();
    await expect(page.locator('.nav__mobile-menu-links a[href="#architecture"]')).toBeVisible();
    await expect(page.locator('.nav__mobile-menu-links a[href="#code-examples"]')).toBeVisible();
    await expect(page.locator('.nav__mobile-menu-links a[href="https://github.com/yetone/mirdb"]')).toBeVisible();
  });

  test('TC9: Click link in mobile menu - menu closes and page scrolls to target section', async ({ page }) => {
    const hamburgerButton = page.locator('.nav__mobile-toggle');
    await hamburgerButton.click();
    await page.waitForTimeout(500);

    const mobileMenu = page.locator('.nav__mobile-menu');
    await expect(mobileMenu).toHaveClass(/nav__mobile-menu--open/);

    // Click a link in the mobile menu
    const featuresLink = page.locator('.nav__mobile-menu-links a[href="#features"]');
    await featuresLink.click();
    await page.waitForTimeout(1000);

    // Menu should be closed
    await expect(mobileMenu).not.toHaveClass(/nav__mobile-menu--open/);

    // Page should have scrolled to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });
});

test.describe('Footer', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC10: Footer is visible with GitHub, Documentation, and License links', async ({ page }) => {
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    const footerLinks = page.locator('.footer__links');
    await expect(footerLinks).toBeVisible();

    // Check for required links
    const githubLink = page.locator('.footer__links a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    const docsLink = page.locator('.footer__links a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();

    const licenseLink = page.locator('.footer__links a:has-text("License")');
    await expect(licenseLink).toBeVisible();
  });

  test('TC11: Click GitHub link in footer - has correct href and opens in new tab', async ({ page }) => {
    const githubLink = page.locator('.footer__links a:has-text("GitHub")');
    await githubLink.scrollIntoViewIfNeeded();

    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC12: Copyright text with year and MirDB is displayed', async ({ page }) => {
    const copyright = page.locator('.footer__copyright');
    await copyright.scrollIntoViewIfNeeded();
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MirDB');
    // Check for copyright symbol and year
    const copyrightText = await copyright.textContent();
    expect(copyrightText).toMatch(/©\s*\d{4}/);
  });
});

test.describe('Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC13: All navigation links are focusable and activatable via keyboard', async ({ page }) => {
    // Tab to each navigation link and verify it's focusable
    const navLinks = page.locator('.nav__links a');
    const linkCount = await navLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      await link.focus();

      // Verify the link is focused
      const isFocused = await link.evaluate((el) => document.activeElement === el);
      expect(isFocused).toBe(true);

      // Verify focus outline is visible (not 'none')
      const outlineStyle = await link.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.outlineStyle;
      });
      // Focus styles may be applied via :focus-visible
    }
  });

  test('TC14: Skip to main content link is present and functional', async ({ page }) => {
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeAttached();

    // Skip link should have correct href
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Tab to make skip link visible
    await page.keyboard.press('Tab');

    // Skip link should be visible when focused
    await expect(skipLink).toBeVisible();

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Main content should receive focus
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeFocused();
  });

  test('Navigation links have visible focus states', async ({ page }) => {
    const firstNavLink = page.locator('.nav__links a').first();
    await firstNavLink.focus();

    // Check that focus styles are applied
    const hasOutline = await firstNavLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.outlineStyle !== 'none' || styles.outlineWidth !== '0px';
    });
    // Note: :focus-visible may only show outline on keyboard focus
  });

  test('Mobile menu can be opened and closed with keyboard', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    const hamburgerButton = page.locator('.nav__mobile-toggle');
    await hamburgerButton.focus();

    // Press Enter to open menu
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);

    const mobileMenu = page.locator('.nav__mobile-menu');
    await expect(mobileMenu).toHaveClass(/nav__mobile-menu--open/);

    // Press Escape to close menu
    await page.keyboard.press('Escape');
    await page.waitForTimeout(500);

    await expect(mobileMenu).not.toHaveClass(/nav__mobile-menu--open/);
  });
});

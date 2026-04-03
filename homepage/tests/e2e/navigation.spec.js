/**
 * Navigation E2E Tests
 * Owner: Scenario 1 - Header and Navigation, Scenario 10 - Footer
 *
 * Tests:
 * - Header elements present (logo, nav links, theme toggle)
 * - Navigation links scroll to correct sections
 * - Sticky header remains visible on scroll
 * - Theme toggle works
 */

const { test, expect } = require('@playwright/test');

test.describe('Header and Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Header contains MirDB logo and project name', async ({ page }) => {
    // Check header exists
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Check logo is present
    const logo = page.locator('.header__logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('src', '../assets/logo.gif');
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Check project name is displayed
    const title = page.locator('.header__title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('MirDB');
  });

  test('TC2: Navigation contains anchor links to all major sections', async ({ page }) => {
    // Get all nav links
    const navList = page.locator('.nav__list');
    await expect(navList).toBeVisible();

    // Check for required navigation links
    const expectedLinks = [
      { text: 'Features', href: '#features' },
      { text: 'Quick Start', href: '#quick-start' },
      { text: 'Architecture', href: '#architecture' },
      { text: 'API Reference', href: '#api-reference' },
      { text: 'Configuration', href: '#configuration' },
      { text: 'Contributing', href: '#contributing' }
    ];

    for (const link of expectedLinks) {
      const navLink = page.locator(`.nav__link[href="${link.href}"]`);
      await expect(navLink).toBeVisible();
      await expect(navLink).toHaveText(link.text);
    }

    // Verify count of navigation links
    const navLinks = page.locator('.nav__link');
    await expect(navLinks).toHaveCount(6);
  });

  test('TC3: Click Features navigation link scrolls to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Click the Features link
    const featuresLink = page.locator('.nav__link[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Check that the Features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC4: Header remains fixed/sticky when scrolling down 500px', async ({ page }) => {
    // First verify header is visible at top
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Check initial position
    const initialPosition = await header.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.position;
    });
    expect(initialPosition).toBe('fixed');

    // Scroll down 500px
    await page.evaluate(() => window.scrollBy(0, 500));
    await page.waitForTimeout(100);

    // Verify header is still visible and fixed
    await expect(header).toBeVisible();
    const scrolledPosition = await header.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.position;
    });
    expect(scrolledPosition).toBe('fixed');

    // Verify header has scrolled class for shadow effect
    await expect(header).toHaveClass(/scrolled/);

    // Verify header is at top of viewport
    const headerBounds = await header.boundingBox();
    expect(headerBounds.y).toBe(0);
  });

  test('TC5: Theme toggle button is present with accessible label', async ({ page }) => {
    // Check theme toggle exists
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Check for accessible label
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark mode');

    // Verify toggle is a button
    const tagName = await themeToggle.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('button');

    // Verify it has an icon
    const icon = themeToggle.locator('.theme-toggle__icon');
    await expect(icon.first()).toBeVisible();
  });
});

// Additional navigation tests for completeness
test.describe('Navigation Interactions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Navigation links have hover and active states', async ({ page }) => {
    const featuresLink = page.locator('.nav__link[href="#features"]');

    // Hover should work
    await featuresLink.hover();
    // Just verify no error occurs during hover

    // Click and verify active state
    await featuresLink.click();
    await page.waitForTimeout(500);
    await expect(featuresLink).toHaveClass(/active/);
  });

  test('Smooth scroll navigation works for all sections', async ({ page }) => {
    const sectionsToTest = ['#quick-start', '#architecture', '#api-reference'];

    for (const sectionId of sectionsToTest) {
      const navLink = page.locator(`.nav__link[href="${sectionId}"]`);
      await navLink.click();
      await page.waitForTimeout(500);

      const section = page.locator(sectionId);
      await expect(section).toBeInViewport();
    }
  });
});

/**
 * Navigation and Smooth Scrolling Tests
 * Scenario 7: Navigation & Smooth Scrolling
 *
 * Tests:
 * 1. Sticky nav header contains links to all sections
 * 2. Click nav link → smooth scroll + URL hash update
 * 3. Mobile viewport → hamburger visible, links hidden
 * 4. Click hamburger → menu toggles; click link → menu closes
 * 5. Tab key → first focusable is skip link; Enter skips to main
 * 6. Scroll down → nav remains sticky, active section highlighted
 */

const { test, expect } = require('@playwright/test');

const SITE_URL = 'http://localhost:8080/index.html';

// ── Test 1: Sticky nav header with correct links ──
test('sticky nav header contains links to all sections', async ({ page }) => {
  await page.goto(SITE_URL);

  const header = page.locator('.site-nav');
  await expect(header).toBeVisible();

  // Verify sticky positioning via computed style
  const position = await header.evaluate((el) => getComputedStyle(el).position);
  expect(position).toBe('sticky');

  // Verify all section links exist
  const expectedLinks = {
    '#features': 'Features',
    '#quickstart': 'Quick Start',
    '#architecture': 'Architecture',
    '#performance': 'Performance',
    '#documentation': 'Documentation',
  };
  for (const [href, text] of Object.entries(expectedLinks)) {
    const link = page.locator(`.nav-links a[href="${href}"]`);
    await expect(link).toBeVisible();
    await expect(link).toHaveText(text);
  }

  // Verify skip link is first focusable element
  const skipLink = page.locator('.skip-nav');
  await expect(skipLink).toHaveAttribute('href', '#main-content');
  await expect(skipLink).toHaveText('Skip to main content');
});

// ── Test 2: Smooth scrolling to sections ──
test('click nav link smoothly scrolls to section and updates URL', async ({ page }) => {
  await page.goto(SITE_URL);

  const featuresLink = page.locator('.nav-links a[href="#features"]');
  await featuresLink.click();

  // Wait for scroll to settle
  await page.waitForTimeout(600);

  // URL should have hash
  const url = page.url();
  expect(url).toContain('#features');

  // The features section should be in view (near top of viewport)
  const sectionTop = await page.locator('#features').evaluate((el) => {
    const rect = el.getBoundingClientRect();
    return rect.top;
  });
  expect(sectionTop).toBeLessThan(100);
});

// ── Test 3: Mobile hamburger menu visibility ──
test('at mobile viewport, hamburger is visible and nav links are hidden', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 667 });
  await page.goto(SITE_URL);

  const hamburger = page.locator('.hamburger');
  await expect(hamburger).toBeVisible();

  // Nav links should be hidden by default on mobile
  const navLinks = page.locator('.nav-links');
  const isHidden = await navLinks.evaluate((el) => {
    const style = getComputedStyle(el);
    return style.display === 'none';
  });
  expect(isHidden).toBe(true);
});

// ── Test 4: Mobile menu toggle ──
test('clicking hamburger toggles menu; clicking link closes it', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 667 });
  await page.goto(SITE_URL);

  const hamburger = page.locator('.hamburger');
  const navLinks = page.locator('.nav-links');

  // Click hamburger to open
  await hamburger.click();
  await expect(navLinks).toHaveClass(/is-open/);

  // Click hamburger again to close
  await hamburger.click();
  await expect(navLinks).not.toHaveClass(/is-open/);

  // Re-open and click a link to close
  await hamburger.click();
  await expect(navLinks).toHaveClass(/is-open/);

  const firstLink = page.locator('.nav-links a').first();
  await firstLink.click();
  await expect(navLinks).not.toHaveClass(/is-open/);
});

// ── Test 5: Skip navigation link ──
test('first focusable element is skip link; Enter skips to main content', async ({ page }) => {
  await page.goto(SITE_URL);

  // Press Tab to focus first element
  await page.keyboard.press('Tab');

  const activeElement = await page.evaluate(() => document.activeElement?.className);
  expect(activeElement).toContain('skip-nav');

  // Press Enter to activate skip link
  await page.keyboard.press('Enter');

  // Main content should be focused
  const focusedId = await page.evaluate(() => document.activeElement?.id);
  expect(focusedId).toBe('main-content');
});

// ── Test 6: Sticky header and active section highlighting ──
test('nav header remains sticky and active section is highlighted on scroll', async ({ page }) => {
  await page.goto(SITE_URL);

  // Verify header is sticky
  const header = page.locator('.site-nav');
  const isSticky = await header.evaluate((el) => getComputedStyle(el).position === 'sticky');
  expect(isSticky).toBe(true);

  // Scroll to features section
  await page.locator('#features').scrollIntoViewIfNeeded();
  await page.waitForTimeout(300);

  // Header should still be visible at top
  const headerRect = await header.evaluate((el) => el.getBoundingClientRect());
  expect(headerRect.top).toBe(0);
  expect(headerRect.height).toBeGreaterThan(0);

  // Active section should be highlighted in nav
  const activeLink = page.locator('.nav-links a.active');
  const activeHref = await activeLink.getAttribute('href');
  expect(activeHref).toBe('#features');
});

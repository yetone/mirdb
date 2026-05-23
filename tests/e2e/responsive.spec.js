/**
 * E2E responsive design tests.
 * Owner: Scenario 9 - Responsive Design
 *
 * Tests:
 * - Viewport 375px (mobile): no horizontal scroll, readable text
 * - Viewport 768px (tablet): adjusted layout, 2-column grid
 * - Viewport 1440px (desktop): full layout, 3-4 column grid
 * - Mobile menu functionality
 * - Touch target sizes on mobile
 * - Font sizes at all breakpoints
 * - Cross-browser rendering (Chromium, Firefox, WebKit)
 */

const { test, expect } = require('@playwright/test');

const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1440, height: 900 },
};

async function hasHorizontalOverflow(page) {
  return page.evaluate(() => {
    return document.documentElement.scrollWidth > window.innerWidth;
  });
}

async function getElementSize(page, selector) {
  return page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const rect = el.getBoundingClientRect();
    return { width: rect.width, height: rect.height };
  }, selector);
}

async function getComputedStyleValue(page, selector, property) {
  return page.evaluate(({ sel, prop }) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    return window.getComputedStyle(el).getPropertyValue(prop);
  }, { sel: selector, prop: property });
}

async function getAllTouchTargets(page) {
  return page.evaluate(() => {
    const interactiveSelectors = [
      '.nav-link',
      '.cta-button',
      '.github-button',
      '.copy-button',
      '.mobile-menu-toggle',
      '.footer-links a',
      '.footer-github-link'
    ];
    const elements = [];
    const seen = new Set();
    interactiveSelectors.forEach((sel) => {
      document.querySelectorAll(sel).forEach((el) => {
        // Skip elements inside other interactive elements (child links inside buttons etc.)
        const parentInteractive = el.closest('.nav-link, .cta-button, .github-button, .copy-button, .mobile-menu-toggle, .footer-links a');
        if (parentInteractive && parentInteractive !== el) return;

        const rect = el.getBoundingClientRect();
        // Skip hidden or zero-sized elements
        if (rect.width === 0 || rect.height === 0) return;
        // Skip elements that are off-screen
        if (rect.top < 0 || rect.left < 0) return;

        const key = `${sel}-${Math.round(rect.top)}-${Math.round(rect.left)}`;
        if (!seen.has(key)) {
          seen.add(key);
          elements.push({
            tag: el.tagName,
            class: el.className,
            width: rect.width,
            height: rect.height,
          });
        }
      });
    });
    return elements;
  });
}

async function getFontSizePx(page, selector) {
  const size = await getComputedStyleValue(page, selector, 'font-size');
  if (!size) return null;
  return parseFloat(size);
}

async function getGridColumnCount(page, selector) {
  const template = await page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const style = window.getComputedStyle(el);
    return style.gridTemplateColumns;
  }, selector);
  if (!template) return 0;
  // Browser resolves repeat(N, 1fr) to N space-separated track sizes
  return template.trim().split(/\s+/).length;
}

// Use file:// protocol to serve the local HTML
const BASE_URL = `file://${process.cwd()}/index.html`;

// ============================================================
// TEST CASE 1: Mobile viewport - no horizontal overflow
// ============================================================
test('mobile viewport has no horizontal overflow', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.mobile);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  const overflow = await hasHorizontalOverflow(page);
  expect(overflow).toBe(false);
});

// ============================================================
// TEST CASE 2: Mobile viewport - hamburger menu functionality
// ============================================================
test('mobile viewport shows hamburger menu and hides nav links', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.mobile);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  // Hamburger button should be visible
  const toggle = page.locator('.mobile-menu-toggle');
  await expect(toggle).toBeVisible();

  // Nav menu should be hidden initially
  const menu = page.locator('#nav-menu');
  await expect(menu).not.toBeVisible();

  // Click hamburger to open menu
  await toggle.click();
  await expect(menu).toBeVisible();

  // aria-expanded should be true
  await expect(toggle).toHaveAttribute('aria-expanded', 'true');

  // Click hamburger again to close
  await toggle.click();
  await expect(menu).not.toBeVisible();
  await expect(toggle).toHaveAttribute('aria-expanded', 'false');
});

// ============================================================
// TEST CASE 3: Tablet viewport - layout and 2-column grid
// ============================================================
test('tablet viewport has no overflow and features in 2-column grid', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.tablet);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  // No horizontal overflow
  const overflow = await hasHorizontalOverflow(page);
  expect(overflow).toBe(false);

  // Features should be in 2-column grid
  const gridColumnCount = await getGridColumnCount(page, '.features-grid');
  expect(gridColumnCount).toBe(2);

  // Text should be readable (body text >= 16px equivalent)
  const bodyFontSize = await getFontSizePx(page, 'body');
  expect(bodyFontSize).toBeGreaterThanOrEqual(16);
});

// ============================================================
// TEST CASE 4: Desktop viewport - max-width container and 3-4 column grid
// ============================================================
test('desktop viewport has max-width container and features in 4-column grid', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.desktop);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  // Container should have max-width and be centered
  const container = page.locator('.container').first();
  await expect(container).toBeVisible();

  const containerStyle = await getComputedStyleValue(page, '.container', 'max-width');
  expect(containerStyle).toBe('1200px');

  // Verify centering by checking the element's position
  const containerBox = await container.boundingBox();
  expect(containerBox.x).toBeGreaterThan(0);
  // Container should be roughly centered: (1440 - 1200) / 2 = 120px
  expect(containerBox.x).toBeGreaterThanOrEqual(100);
  expect(containerBox.x).toBeLessThanOrEqual(140);

  // Features should be in 4-column grid
  const gridColumnCount = await getGridColumnCount(page, '.features-grid');
  expect(gridColumnCount).toBe(4);

  // Adequate whitespace (padding on body or sections)
  const bodyFontSize = await getFontSizePx(page, 'body');
  expect(bodyFontSize).toBeGreaterThanOrEqual(16);
});

// ============================================================
// TEST CASE 6: Cross-browser rendering (implicit via Playwright projects)
// This test runs automatically across all configured browsers.
// ============================================================
test('page renders consistently across browsers with no broken layout', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.desktop);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  // All main sections should be visible
  await expect(page.locator('#hero')).toBeVisible();
  await expect(page.locator('#about')).toBeVisible();
  await expect(page.locator('#features')).toBeVisible();
  await expect(page.locator('#status')).toBeVisible();
  await expect(page.locator('#quick-start')).toBeVisible();
  await expect(page.locator('#configuration')).toBeVisible();

  // No horizontal overflow
  const overflow = await hasHorizontalOverflow(page);
  expect(overflow).toBe(false);

  // Feature cards should have positive dimensions
  const cards = page.locator('.feature-card');
  const count = await cards.count();
  expect(count).toBe(4);
  for (let i = 0; i < count; i++) {
    const box = await cards.nth(i).boundingBox();
    expect(box.width).toBeGreaterThan(0);
    expect(box.height).toBeGreaterThan(0);
  }
});

// ============================================================
// TEST CASE 7: Touch target sizes on mobile (>= 44x44px)
// ============================================================
test('mobile touch targets are at least 44x44px', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.mobile);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  // Open mobile menu to expose nav links
  const toggle = page.locator('.mobile-menu-toggle');
  await toggle.click();
  await page.waitForTimeout(100);

  const targets = await getAllTouchTargets(page);

  // Filter out zero-sized elements (hidden or not rendered)
  const visibleTargets = targets.filter((t) => t.width > 0 && t.height > 0);
  expect(visibleTargets.length).toBeGreaterThan(0);

  for (const target of visibleTargets) {
    expect(target.width).toBeGreaterThanOrEqual(44);
    expect(target.height).toBeGreaterThanOrEqual(44);
  }
});

// ============================================================
// TEST CASE 8: Font sizes readable at all breakpoints
// ============================================================
test('font sizes are readable at mobile breakpoint', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.mobile);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  const bodyFontSize = await getFontSizePx(page, 'body');
  expect(bodyFontSize).toBeGreaterThanOrEqual(16);

  const h1FontSize = await getFontSizePx(page, '#hero h1');
  expect(h1FontSize).toBeGreaterThanOrEqual(16);

  const pFontSize = await getFontSizePx(page, '#about p');
  expect(pFontSize).toBeGreaterThanOrEqual(16);
});

test('font sizes are readable at tablet breakpoint', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.tablet);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  const bodyFontSize = await getFontSizePx(page, 'body');
  expect(bodyFontSize).toBeGreaterThanOrEqual(16);

  const h1FontSize = await getFontSizePx(page, '#hero h1');
  expect(h1FontSize).toBeGreaterThanOrEqual(16);
});

test('font sizes are readable at desktop breakpoint', async ({ page }) => {
  await page.setViewportSize(VIEWPORTS.desktop);
  await page.goto(BASE_URL);
  await page.waitForLoadState('networkidle');

  const bodyFontSize = await getFontSizePx(page, 'body');
  expect(bodyFontSize).toBeGreaterThanOrEqual(16);

  const h1FontSize = await getFontSizePx(page, '#hero h1');
  expect(h1FontSize).toBeGreaterThanOrEqual(16);

  // Headings should scale up on desktop
  expect(h1FontSize).toBeGreaterThanOrEqual(bodyFontSize);
});

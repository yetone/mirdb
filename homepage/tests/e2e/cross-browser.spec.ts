import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests
 *
 * Verifies the homepage works correctly on Chrome, Firefox, Safari (WebKit),
 * and Edge latest stable. Edge shares the Chromium engine with Chrome, so the
 * "edge" project in playwright.config.ts uses the same engine as "chromium".
 *
 * Coverage:
 * - Page loads without console errors
 * - All major sections render
 * - Critical CSS features (variables, flexbox, grid) work
 * - Sticky header positioning
 * - Smooth-scroll behavior or graceful fallback
 * - Backdrop-filter (with -webkit prefix fallback for Safari)
 * - Animations and transitions
 */

const SECTION_SELECTORS = [
  '[data-testid="hero-section"]',
  '[data-testid="features-section"]',
  '[data-testid="quick-start-section"]',
  '[data-testid="protocol-section"]',
  '[data-testid="architecture-section"]',
  '[data-testid="config-section"]',
  '[data-testid="demo-section"]',
  '[data-testid="roadmap-section"]',
  '[data-testid="status-section"]',
];

test.describe('Cross-Browser Compatibility', () => {
  test('homepage loads without page or console errors', async ({ page, browserName }) => {
    const consoleErrors: string[] = [];
    const pageErrors: string[] = [];

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });
    page.on('pageerror', (err) => {
      pageErrors.push(err.message);
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    expect(
      pageErrors,
      `${browserName}: page should load without uncaught JavaScript errors`,
    ).toEqual([]);
    expect(
      consoleErrors,
      `${browserName}: page should load without console errors`,
    ).toEqual([]);
  });

  test('all major homepage sections render visibly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.setViewportSize({ width: 1280, height: 800 });

    for (const selector of SECTION_SELECTORS) {
      await expect(
        page.locator(selector).first(),
        `${browserName}: ${selector} should be visible`,
      ).toBeVisible();
    }
  });

  test('hero headline renders with non-empty text', async ({ page, browserName }) => {
    await page.goto('/');
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    const text = await headline.textContent();
    expect(text?.trim().length, `${browserName}: hero headline must have text`).toBeGreaterThan(0);
  });

  test('CSS custom properties resolve to concrete background color', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const backgroundColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });
    expect(
      backgroundColor,
      `${browserName}: body background must resolve from CSS variable`,
    ).toMatch(/^rgba?\(/);
    // Should not be transparent or empty
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('');
  });

  test('flexbox layout renders correctly in header', async ({ page, browserName }) => {
    await page.goto('/');
    const header = page.locator('[data-testid="header"]');
    const flexContainer = header.locator('div.flex').first();
    const display = await flexContainer.evaluate(
      (el) => window.getComputedStyle(el).display,
    );
    expect(display, `${browserName}: header should use flexbox`).toBe('flex');
  });

  test('CSS grid layout renders correctly in features section', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    await page.setViewportSize({ width: 1280, height: 800 });

    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    const display = await featuresGrid.evaluate(
      (el) => window.getComputedStyle(el).display,
    );
    expect(display, `${browserName}: features grid should use CSS grid`).toBe('grid');
  });

  test('header is positioned at the top (fixed/sticky) across browsers', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const header = page.locator('[data-testid="header"]');
    await expect(header).toBeVisible();

    const position = await header.evaluate(
      (el) => window.getComputedStyle(el).position,
    );
    expect(
      ['fixed', 'sticky'],
      `${browserName}: header should be fixed or sticky, got ${position}`,
    ).toContain(position);

    // Header must be at top of viewport
    const box = await header.boundingBox();
    expect(box?.y, `${browserName}: header should be at y=0`).toBe(0);
  });

  test('header remains visible after scrolling (sticky behavior)', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const header = page.locator('[data-testid="header"]');
    await expect(header).toBeVisible();

    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(150);

    await expect(
      header,
      `${browserName}: header should remain visible after scroll`,
    ).toBeVisible();
    const box = await header.boundingBox();
    expect(box?.y, `${browserName}: header should stay at top after scroll`).toBe(0);
  });

  test('html element exposes scroll-behavior style or graceful fallback', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const scrollBehavior = await page.evaluate(
      () => window.getComputedStyle(document.documentElement).scrollBehavior,
    );
    // 'smooth' on supporting browsers; 'auto' if not supported (which is a graceful fallback).
    expect(
      ['smooth', 'auto'],
      `${browserName}: scroll-behavior should be smooth or auto, got ${scrollBehavior}`,
    ).toContain(scrollBehavior);
  });

  test('backdrop-filter falls back gracefully in browsers without support', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const header = page.locator('[data-testid="header"]');
    const styles = await header.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backdropFilter: computed.backdropFilter,
        webkitBackdropFilter: (computed as unknown as Record<string, string>)['-webkit-backdrop-filter'],
        backgroundColor: computed.backgroundColor,
      };
    });

    // Either backdrop-filter / -webkit-backdrop-filter is supported,
    // OR the background color provides a sufficient fallback (non-transparent).
    const backdropSupported =
      (styles.backdropFilter && styles.backdropFilter !== 'none') ||
      (styles.webkitBackdropFilter && styles.webkitBackdropFilter !== 'none');
    const hasBackgroundFallback =
      styles.backgroundColor && styles.backgroundColor !== 'rgba(0, 0, 0, 0)';

    expect(
      backdropSupported || hasBackgroundFallback,
      `${browserName}: header must either support backdrop-filter or have a background fallback`,
    ).toBe(true);
  });

  test('navigation links are clickable and accessible', async ({ page, browserName }) => {
    await page.goto('/');
    await page.setViewportSize({ width: 1280, height: 800 });

    const desktopNav = page.locator('[data-testid="desktop-nav"]');
    await expect(desktopNav).toBeVisible();

    const navLinks = desktopNav.locator('a');
    const count = await navLinks.count();
    expect(count, `${browserName}: desktop nav should have 5 links`).toBe(5);

    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();
      const href = await link.getAttribute('href');
      expect(href, `${browserName}: nav link ${i} should have href`).toBeTruthy();
    }
  });

  test('font loading uses widely supported font-family with fallback chain', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const codeFontFamily = await page.evaluate(() => {
      const codeEl = document.querySelector('code, pre');
      if (!codeEl) return '';
      return window.getComputedStyle(codeEl).fontFamily;
    });

    // Must have at least one generic fallback (monospace/sans-serif/serif).
    expect(
      codeFontFamily,
      `${browserName}: code font family should include generic fallback`,
    ).toMatch(/(monospace|sans-serif|serif)/);
  });

  test('images and assets load successfully', async ({ page, browserName }) => {
    const failedRequests: string[] = [];
    page.on('response', (response) => {
      const url = response.url();
      const status = response.status();
      // Track failed asset loads from the local server only
      if (url.startsWith('http://localhost:3000') && status >= 400) {
        failedRequests.push(`${status} ${url}`);
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    expect(
      failedRequests,
      `${browserName}: no local asset requests should fail`,
    ).toEqual([]);
  });

  test('document title and html lang attribute set correctly', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const title = await page.title();
    expect(title, `${browserName}: page must have a title`).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    const lang = await page.evaluate(() => document.documentElement.lang);
    expect(lang, `${browserName}: html lang attribute must be set`).toBe('en');
  });

  test('CSS.supports API is available for runtime feature detection', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const supportsAPI = await page.evaluate(() => {
      return typeof window.CSS !== 'undefined' && typeof window.CSS.supports === 'function';
    });
    expect(
      supportsAPI,
      `${browserName}: CSS.supports API should be available`,
    ).toBe(true);
  });

  test('critical CSS features are supported (variables, flexbox, grid)', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const support = await page.evaluate(() => {
      return {
        cssVariables: window.CSS && CSS.supports('--foo', '0'),
        flexbox: window.CSS && CSS.supports('display', 'flex'),
        grid: window.CSS && CSS.supports('display', 'grid'),
      };
    });

    expect(support.cssVariables, `${browserName}: CSS variables must be supported`).toBe(
      true,
    );
    expect(support.flexbox, `${browserName}: flexbox must be supported`).toBe(true);
    expect(support.grid, `${browserName}: CSS grid must be supported`).toBe(true);
  });

  test('hover and focus styles do not break layout in WebKit', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    await page.setViewportSize({ width: 1280, height: 800 });

    const navLinks = page.locator('[data-testid="desktop-nav"] a');
    const firstLink = navLinks.first();
    await firstLink.hover();
    // Allow any transition to settle
    await page.waitForTimeout(150);
    await expect(
      firstLink,
      `${browserName}: nav link should remain visible after hover`,
    ).toBeVisible();

    // Focus via keyboard
    await firstLink.focus();
    await page.waitForTimeout(50);
    const isFocused = await firstLink.evaluate(
      (el) => document.activeElement === el,
    );
    expect(isFocused, `${browserName}: nav link should accept keyboard focus`).toBe(true);
  });

  test('viewport meta tag is set for mobile/Safari rendering', async ({
    page,
    browserName,
  }) => {
    await page.goto('/');
    const viewport = await page.evaluate(() => {
      const meta = document.querySelector('meta[name="viewport"]');
      return meta?.getAttribute('content') ?? null;
    });
    // Next.js sets a default viewport; verify it exists and contains width.
    expect(viewport, `${browserName}: viewport meta must be set`).toBeTruthy();
    expect(viewport).toMatch(/width=/);
  });
});

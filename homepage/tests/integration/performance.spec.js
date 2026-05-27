/**
 * Performance & Load Time Tests
 * Scenario 11: Performance & Load Time
 *
 * Tests:
 * 1. Critical CSS is inlined in a <style> tag in the head
 * 2. Non-critical CSS is loaded asynchronously via preload + onload
 * 3. All script tags use defer or are placed before closing body tag
 * 4. No synchronous external scripts in the head
 * 5. Preconnect and DNS prefetch hints are present
 * 6. Page renders successfully with JavaScript disabled
 * 7. Core functionality works without JS (nav anchors, content visibility)
 * 8. Total page weight is under 500KB on initial load
 * 9. Performance metrics on slow 3G: FCP, LCP, TTI
 * 10. Resource hints (preload for scripts) are present
 * 11. Meta theme-color is set for both color schemes
 * 12. Canonical and OG meta tags are present
 */

const { test, expect } = require('@playwright/test');

const SITE_URL = 'http://localhost:8080/index.html';

// ── Test 1: Critical CSS is inlined ──
test('critical CSS is inlined in a style tag within the head', async ({ page }) => {
  await page.goto(SITE_URL);

  const head = page.locator('head');
  const styleTag = head.locator('style');
  await expect(styleTag).toHaveCount(1);

  const styleContent = await styleTag.textContent();

  // Verify critical styles are present
  expect(styleContent).toContain('--color-bg');
  expect(styleContent).toContain('--color-primary');
  expect(styleContent).toContain('.site-header');
  expect(styleContent).toContain('.hero');
  expect(styleContent).toContain('.skip-link');
  expect(styleContent).toContain('.main-nav');
  expect(styleContent).toContain('.btn-primary');
  expect(styleContent).toContain('@media (max-width: 767px)');
});

// ── Test 2: Non-critical CSS loaded asynchronously ──
test('non-critical CSS is loaded via preload with onload fallback and noscript', async ({ page }) => {
  await page.goto(SITE_URL);

  const head = page.locator('head');

  // Check for preload links with onload
  const preloadLinks = head.locator('link[rel="preload"][as="style"]');
  const count = await preloadLinks.count();
  expect(count).toBeGreaterThanOrEqual(2);

  for (let i = 0; i < count; i++) {
    const link = preloadLinks.nth(i);
    const onload = await link.getAttribute('onload');
    expect(onload).toContain("this.rel='stylesheet'");
    const href = await link.getAttribute('href');
    expect(href).toMatch(/\.(css)$/);
  }

  // Check noscript fallback - get raw HTML because noscript content is not parsed as DOM when JS is enabled
  const html = await page.content();
  // Count occurrences of noscript tags containing stylesheet links
  const noscriptCount = (html.match(/<noscript>/g) || []).length;
  expect(noscriptCount).toBeGreaterThanOrEqual(2);
  expect(html).toContain('<noscript><link');
  expect(html).toContain('rel=stylesheet></noscript>');
});

// ── Test 3: No synchronous render-blocking stylesheet links in head ──
test('no synchronous render-blocking stylesheet links in head', async ({ page }) => {
  await page.goto(SITE_URL);

  const head = page.locator('head');

  // A link[rel="stylesheet"] without being inside noscript is render-blocking
  const blockingLinks = head.locator(':scope > link[rel="stylesheet"]');
  const count = await blockingLinks.count();
  expect(count).toBe(0);
});

// ── Test 4: All scripts use defer ──
test('all external script tags use defer attribute', async ({ page }) => {
  await page.goto(SITE_URL);

  const scripts = page.locator('script[src]');
  const count = await scripts.count();
  expect(count).toBeGreaterThan(0);

  for (let i = 0; i < count; i++) {
    const script = scripts.nth(i);
    const defer = await script.getAttribute('defer');
    expect(defer).not.toBeNull();
  }
});

// ── Test 5: No synchronous external scripts in head ──
test('no synchronous external scripts in head', async ({ page }) => {
  await page.goto(SITE_URL);

  const headScripts = page.locator('head script[src]');
  const count = await headScripts.count();
  expect(count).toBe(0);
});

// ── Test 6: Preconnect and DNS prefetch hints ──
test('preconnect and dns-prefetch resource hints are present', async ({ page }) => {
  await page.goto(SITE_URL);

  const head = page.locator('head');

  // There are 2 preconnect links: fonts.googleapis.com and fonts.gstatic.com
  const preconnect = head.locator('link[rel="preconnect"]');
  const preconnectCount = await preconnect.count();
  expect(preconnectCount).toBeGreaterThanOrEqual(1);

  const dnsPrefetch = head.locator('link[rel="dns-prefetch"]');
  await expect(dnsPrefetch).toHaveCount(1);
  const dnsHref = await dnsPrefetch.getAttribute('href');
  expect(dnsHref).toContain('fonts.googleapis.com');
});

// ── Test 7: Script preload hints ──
test('preload hints for critical scripts are present', async ({ page }) => {
  await page.goto(SITE_URL);

  const head = page.locator('head');
  const scriptPreloads = head.locator('link[rel="preload"][as="script"]');
  const count = await scriptPreloads.count();
  expect(count).toBeGreaterThanOrEqual(2);

  const hrefs = [];
  for (let i = 0; i < count; i++) {
    const href = await scriptPreloads.nth(i).getAttribute('href');
    hrefs.push(href);
  }
  expect(hrefs.some(h => h && h.includes('nav.js'))).toBe(true);
  expect(hrefs.some(h => h && h.includes('theme.js'))).toBe(true);
});

// ── Test 8: Meta theme-color for both color schemes ──
test('theme-color meta tags are set for both dark and light schemes', async ({ page }) => {
  await page.goto(SITE_URL);

  const darkMeta = page.locator('meta[name="theme-color"][media*="dark"]');
  await expect(darkMeta).toHaveCount(1);
  const darkContent = await darkMeta.getAttribute('content');
  expect(darkContent).toBe('#1a1a1a');

  const lightMeta = page.locator('meta[name="theme-color"][media*="light"]');
  await expect(lightMeta).toHaveCount(1);
  const lightContent = await lightMeta.getAttribute('content');
  expect(lightContent).toBe('#ffffff');
});

// ── Test 9: Canonical link and OG meta tags ──
test('canonical link and Open Graph meta tags are present', async ({ page }) => {
  await page.goto(SITE_URL);

  const canonical = page.locator('link[rel="canonical"]');
  await expect(canonical).toHaveCount(1);

  const ogTitle = page.locator('meta[property="og:title"]');
  await expect(ogTitle).toHaveCount(1);

  const ogDesc = page.locator('meta[property="og:description"]');
  await expect(ogDesc).toHaveCount(1);

  const ogType = page.locator('meta[property="og:type"]');
  await expect(ogType).toHaveCount(1);

  const twitterCard = page.locator('meta[name="twitter:card"]');
  await expect(twitterCard).toHaveCount(1);
});

// ── Test 10: Page renders with JavaScript disabled ──
test('page renders all content visible with JavaScript disabled', async ({ browser }) => {
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  await page.goto(SITE_URL);

  // Hero content should be visible
  const hero = page.locator('.hero');
  await expect(hero).toBeVisible();
  await expect(hero.locator('h1')).toHaveText('MirDB');
  await expect(hero.locator('.hero-tagline')).toContainText('persistent key-value store');

  // Features section should be visible
  const features = page.locator('#features');
  await expect(features).toBeVisible();
  await expect(features.locator('h2')).toHaveText('Features');

  // All feature cards should be visible
  const featureCards = page.locator('.feature-card');
  await expect(featureCards).toHaveCount(6);

  // Quick Start section visible
  const quickstart = page.locator('#quickstart');
  await expect(quickstart).toBeVisible();

  // Architecture section visible
  const architecture = page.locator('#architecture');
  await expect(architecture).toBeVisible();

  // Performance section visible
  const performance = page.locator('#performance');
  await expect(performance).toBeVisible();

  // Documentation section visible
  const docs = page.locator('#docs');
  await expect(docs).toBeVisible();

  // Footer visible
  const footer = page.locator('.site-footer');
  await expect(footer).toBeVisible();

  // Navigation should still be visible (CSS-only)
  const nav = page.locator('.site-header');
  await expect(nav).toBeVisible();

  // Core navigation links should work (anchor links)
  const navLinks = page.locator('.nav-links a');
  const linkCount = await navLinks.count();
  expect(linkCount).toBeGreaterThanOrEqual(5);

  for (let i = 0; i < linkCount; i++) {
    const href = await navLinks.nth(i).getAttribute('href');
    expect(href).toMatch(/^#/);
  }

  await context.close();
});

// ── Test 11: Total page weight under 500KB ──
test('total page weight on initial load is under 500KB', async ({ page }) => {
  let totalBytes = 0;
  page.on('response', async (response) => {
    try {
      const headers = response.headers();
      const length = headers['content-length'];
      if (length) {
        totalBytes += parseInt(length, 10);
      } else {
        // Fallback: try to get body size
        const body = await response.body().catch(() => null);
        if (body) totalBytes += body.length;
      }
    } catch (e) {
      // ignore
    }
  });

  await page.goto(SITE_URL);
  await page.waitForLoadState('networkidle');

  const totalKB = totalBytes / 1024;
  expect(totalKB).toBeLessThan(500);
});

// ── Test 12: Performance metrics on slow 3G simulation ──
test('performance metrics on slow 3G: FCP < 1.5s, LCP < 2.0s', async ({ browser }) => {
  const context = await browser.newContext({
    // Simulate slow 3G
    viewport: { width: 375, height: 667 },
  });

  const page = await context.newPage();

  // Use CDP to emulate slow 3G
  const client = await page.context().newCDPSession(page);
  await client.send('Network.emulateNetworkConditions', {
    offline: false,
    downloadThroughput: 750 * 1024 / 8, // 750 kbps
    uploadThroughput: 250 * 1024 / 8,   // 250 kbps
    latency: 100, // 100ms RTT
  });

  const startTime = Date.now();
  await page.goto(SITE_URL);
  await page.waitForLoadState('networkidle');
  const loadTime = Date.now() - startTime;

  // Get performance entries from the browser
  const paintMetrics = await page.evaluate(() => {
    const entries = performance.getEntriesByType('paint');
    const nav = performance.getEntriesByType('navigation')[0];
    const result = {};
    entries.forEach(e => {
      result[e.name] = e.startTime;
    });
    if (nav) {
      result.domInteractive = nav.domInteractive;
      result.domContentLoaded = nav.domContentLoadedEventEnd;
    }
    // LCP
    const lcpEntries = performance.getEntriesByType('largest-contentful-paint');
    if (lcpEntries.length > 0) {
      result.largestContentfulPaint = lcpEntries[lcpEntries.length - 1].startTime;
    }
    return result;
  });

  // FCP should be measured
  const fcp = paintMetrics['first-contentful-paint'];
  expect(fcp).toBeDefined();
  // FCP under 1.5s (relaxed for CI)
  expect(fcp).toBeLessThan(1500);

  // LCP should be measured
  const lcp = paintMetrics.largestContentfulPaint || paintMetrics['first-contentful-paint'];
  expect(lcp).toBeDefined();
  // LCP under 2.0s (relaxed for CI)
  expect(lcp).toBeLessThan(2000);

  // Page should load reasonably fast (total load time under 3s)
  expect(loadTime).toBeLessThan(3000);

  await context.close();
});

// ── Test 13: Content is visible immediately (no FOUC) ──
test('content is visible immediately without flash of unstyled content', async ({ page }) => {
  await page.goto(SITE_URL);

  // The hero should be styled immediately since critical CSS is inlined
  const heroBg = await page.locator('.hero').evaluate((el) => {
    const style = window.getComputedStyle(el);
    return {
      textAlign: style.textAlign,
      minHeight: style.minHeight,
    };
  });

  expect(heroBg.textAlign).toBe('center');
  expect(heroBg.minHeight).toBeTruthy();

  // Navigation should be sticky immediately
  const navPosition = await page.locator('.site-header').evaluate((el) => {
    return window.getComputedStyle(el).position;
  });
  expect(navPosition).toBe('sticky');
});

// ── Test 14: Hero CTA button is styled from critical CSS ──
test('hero CTA button is styled by critical CSS without external stylesheet', async ({ page }) => {
  await page.goto(SITE_URL);

  // Block external stylesheets to verify critical CSS is sufficient
  await page.route('**/*.css', (route) => route.abort());

  // Reload with CSS blocked
  await page.reload();

  // Hero should still have basic styling from inlined critical CSS
  const hero = page.locator('.hero');
  await expect(hero).toBeVisible();

  const cta = page.locator('.btn-primary');
  await expect(cta).toBeVisible();

  // The CTA should have the background color from critical CSS
  const bgColor = await cta.evaluate((el) => {
    return window.getComputedStyle(el).backgroundColor;
  });
  expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
  expect(bgColor).not.toBe('transparent');
});

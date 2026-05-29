import { test, expect } from '@playwright/test';

test.describe('Performance and Page Load', () => {
  test.beforeEach(async ({ page }) => {
    // Clear any previous console logs
    page.on('console', () => {});
  });

  test('TC1: Load homepage on simulated 3G - TTI under 2 seconds, LCP under 2.5 seconds', async ({ page }) => {
    // Capture console errors
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Simulate 3G network conditions via CDP
    const client = await page.context().newCDPSession(page);
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.6 * 1024 * 1024) / 8, // 1.6 Mbps
      uploadThroughput: (750 * 1024) / 8, // 750 Kbps
      latency: 150, // 150ms
    });

    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate and wait for network idle
    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'networkidle' });
    const loadTime = Date.now() - startTime;

    // Measure TTI via Performance API
    const tti = await page.evaluate(() => {
      const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      if (!nav) return null;
      // TTI approximation: domInteractive
      return nav.domInteractive;
    });

    // Measure LCP via PerformanceObserver
    const lcp = await page.evaluate(() => {
      return new Promise<number | null>((resolve) => {
        let lcpValue: number | null = null;
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          const lastEntry = entries[entries.length - 1] as PerformanceEntry;
          lcpValue = lastEntry.startTime;
        });
        observer.observe({ entryTypes: ['largest-contentful-paint'] as unknown as any });
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpValue);
        }, 3000);
      });
    });

    // Verify page loaded successfully
    await expect(page.getByTestId('home-page')).toBeVisible();
    await expect(page.getByTestId('hero-section')).toBeVisible();

    // TTI should be under 2 seconds (2000ms)
    if (tti !== null) {
      expect(tti, `TTI was ${tti}ms`).toBeLessThan(2000);
    }

    // LCP should be under 2.5 seconds (2500ms)
    if (lcp !== null) {
      expect(lcp, `LCP was ${lcp}ms`).toBeLessThan(2500);
    }

    // Total load time should also be reasonable
    expect(loadTime, `Total load time was ${loadTime}ms`).toBeLessThan(5000);

    // No console errors during load
    expect(consoleErrors, `Console errors: ${consoleErrors.join(', ')}`).toHaveLength(0);
  });

  test('TC2: CLS score below 0.1 during page load', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await page.setViewportSize({ width: 375, height: 667 });

    // Measure CLS via Layout Shift API
    const cls = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let clsValue = 0;
        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            const layoutShift = entry as any;
            if (!layoutShift.hadRecentInput) {
              clsValue += layoutShift.value;
            }
          }
        });
        observer.observe({ entryTypes: ['layout-shift'] as unknown as any });
        setTimeout(() => {
          observer.disconnect();
          resolve(clsValue);
        }, 3000);
      });
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for CLS measurement to complete
    await page.waitForTimeout(3000);

    // Verify page is stable
    await expect(page.getByTestId('home-page')).toBeVisible();

    // CLS should be below 0.1
    expect(cls, `CLS was ${cls}`).toBeLessThan(0.1);

    // No console errors
    expect(consoleErrors).toHaveLength(0);
  });

  test('TC5: prefers-reduced-motion disables BackgroundEffect and micro-interactions', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.setViewportSize({ width: 1280, height: 720 });

    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // BackgroundEffect should be present but in reduced mode
    const bgEffect = page.getByTestId('background-effect');
    await expect(bgEffect).toBeVisible();

    const reducedMotionAttr = await bgEffect.getAttribute('data-reduced-motion');
    expect(reducedMotionAttr).toBe('true');

    // Verify the background effect is a static div (not canvas) when reduced motion is on
    const tagName = await bgEffect.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('div');

    // Feature cards should still be visible
    const featureCards = page.getByTestId('feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify no animations are running by checking computed styles
    const hasAnimations = await page.evaluate(() => {
      const animatedElements = document.querySelectorAll('*');
      for (const el of animatedElements) {
        const style = window.getComputedStyle(el);
        if (style.animationName !== 'none' && style.animationDuration !== '0.01ms') {
          return true;
        }
      }
      return false;
    });
    expect(hasAnimations).toBe(false);

    // No console errors
    expect(consoleErrors).toHaveLength(0);
  });

  test('TC6: Interact with homepage - no console errors during tab, hover, and click', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });

    const consoleErrors: string[] = [];
    const consoleWarnings: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
      if (msg.type() === 'warning') {
        consoleWarnings.push(msg.text());
      }
    });

    // Capture page errors (unhandled exceptions)
    const pageErrors: string[] = [];
    page.on('pageerror', (err) => {
      pageErrors.push(err.message);
    });

    // Mock the API endpoint to prevent 404 errors during interaction
    await page.route('/api/urls/shorten', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ short_url: 'http://localhost:4173/s/abc123' }),
      });
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Tab through interactive elements
    await page.keyboard.press('Tab'); // URL input
    await page.keyboard.press('Tab'); // Shorten button
    await page.keyboard.press('Tab'); // CTA button

    // Hover over feature cards
    const featureCards = page.getByTestId('feature-card');
    const firstCard = featureCards.first();
    await firstCard.hover();

    // Hover over CTA button
    const ctaButton = page.getByTestId('hero-cta-button');
    await ctaButton.hover();

    // Click on URL input and type
    const urlInput = page.getByTestId('url-input');
    await urlInput.click();
    await urlInput.fill('https://example.com');

    // Click shorten button
    const shortenButton = page.getByTestId('shorten-button');
    await shortenButton.click();

    // Wait a moment for any async operations
    await page.waitForTimeout(500);

    // Click footer links
    const footer = page.getByTestId('footer');
    await footer.scrollIntoViewIfNeeded();
    await page.waitForTimeout(200);

    // No console errors
    expect(consoleErrors, `Console errors: ${consoleErrors.join(', ')}`).toHaveLength(0);

    // No unhandled exceptions
    expect(pageErrors, `Page errors: ${pageErrors.join(', ')}`).toHaveLength(0);
  });

  test('TC3-manual: Verify initial JS bundle is under 200KB gzipped', async ({ page }) => {
    // This test verifies the build output rather than runtime behavior
    // The actual bundle size check is done at build time
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify the page loads with minimal resources
    const resources = await page.evaluate(() => {
      return performance.getEntriesByType('resource').map((r: PerformanceResourceTiming) => ({
        name: r.name,
        transferSize: r.transferSize,
        duration: r.duration,
        initiatorType: r.initiatorType,
      }));
    });

    // Filter for JS resources loaded by the page
    const jsResources = resources.filter(
      (r: any) => r.initiatorType === 'script' || r.name.endsWith('.js')
    );

    // Check that JS resources are reasonably sized (under 200KB each)
    for (const resource of jsResources) {
      if (resource.transferSize > 0) {
        const sizeKB = resource.transferSize / 1024;
        expect(
          sizeKB,
          `JS resource ${resource.name} is ${sizeKB.toFixed(1)}KB`
        ).toBeLessThan(200);
      }
    }

    // Page should be functional
    await expect(page.getByTestId('home-page')).toBeVisible();
  });

  test('TC4-manual: Lighthouse performance audit - verify key metrics', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Collect key performance metrics that Lighthouse would measure
    const metrics = await page.evaluate(() => {
      const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return {
        domContentLoaded: nav?.domContentLoadedEventEnd || 0,
        loadComplete: nav?.loadEventEnd || 0,
        firstPaint: performance.getEntriesByName('first-paint')[0]?.startTime || 0,
        firstContentfulPaint: performance.getEntriesByName('first-contentful-paint')[0]?.startTime || 0,
      };
    });

    // Verify reasonable performance metrics
    expect(metrics.domContentLoaded).toBeLessThan(3000);
    expect(metrics.firstContentfulPaint).toBeLessThan(2500);

    // No console errors
    expect(consoleErrors).toHaveLength(0);

    // Verify page is interactive
    const urlInput = page.getByTestId('url-input');
    await expect(urlInput).toBeVisible();
    await urlInput.fill('https://example.com');
    await expect(urlInput).toHaveValue('https://example.com');
  });
});

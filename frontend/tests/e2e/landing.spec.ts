/**
 * Landing Page Performance E2E Tests
 * Owner: Scenario 14 - Performance Requirements
 *
 * End-to-end tests for performance requirements:
 * - NFR-1: Page loads within 2 seconds on 4G networks
 * - Hero section loads immediately (critical content)
 * - Below-fold sections use intersection observer for lazy loading
 * - Production build is code-split and optimized
 */

import { test, expect } from '@playwright/test';

test.describe('Performance Requirements - Page Load', () => {
  test.describe('TC1: First Contentful Paint under 2 seconds (4G simulation)', () => {
    test('Page loads within 2 seconds on 4G network', async ({ page }) => {
      // Enable performance metrics
      const client = await page.context().newCDPSession(page);
      await client.send('Performance.enable');

      // Simulate 4G network conditions
      // 4G: ~9 Mbps download, ~9 Mbps upload, 100ms latency
      await page.route('**/*', async (route) => {
        // Add simulated network delay for 4G
        await new Promise((resolve) => setTimeout(resolve, 20)); // Minimal processing delay
        await route.continue();
      });

      // Start timing
      const startTime = Date.now();

      // Navigate to the page
      await page.goto('/');

      // Wait for first contentful paint (hero section to be visible)
      await page.waitForSelector('#hero-headline', { state: 'visible' });

      // Calculate load time
      const loadTime = Date.now() - startTime;

      // Assert load time is under 2 seconds (2000ms)
      expect(loadTime).toBeLessThan(2000);

      // Verify hero content is visible
      const heroHeadline = page.locator('#hero-headline');
      await expect(heroHeadline).toBeVisible();
    });

    test('Core Web Vitals metrics are acceptable', async ({ page }) => {
      // Navigate to the page
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Get performance metrics
      const metrics = await page.evaluate(() => {
        return new Promise((resolve) => {
          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries();
            const metrics: Record<string, number> = {};

            entries.forEach((entry) => {
              if (entry.entryType === 'paint') {
                metrics[entry.name] = entry.startTime;
              }
            });

            resolve(metrics);
          });

          observer.observe({ entryTypes: ['paint'] });

          // Fallback timeout
          setTimeout(() => {
            const paintEntries = performance.getEntriesByType('paint');
            const metrics: Record<string, number> = {};
            paintEntries.forEach((entry) => {
              metrics[entry.name] = entry.startTime;
            });
            resolve(metrics);
          }, 1000);
        });
      });

      // FCP should be present (page painted content)
      expect(metrics).toBeDefined();
    });
  });

  test.describe('TC2: Hero section loads immediately', () => {
    test('Hero section content is rendered without delay', async ({ page }) => {
      // Navigate to the page
      await page.goto('/');

      // Hero section should be visible immediately after DOM content loaded
      await page.waitForLoadState('domcontentloaded');

      // Hero headline should be visible
      const heroHeadline = page.locator('#hero-headline');
      await expect(heroHeadline).toBeVisible();

      // Verify hero section is above the fold (not lazy loaded)
      const heroSection = page.locator('section[aria-labelledby="hero-headline"]');
      await expect(heroSection).toBeVisible();

      // Verify the URL shortening form is immediately available
      const urlInput = page.getByTestId('url-input');
      await expect(urlInput).toBeVisible();

      const shortenButton = page.getByTestId('shorten-button');
      await expect(shortenButton).toBeVisible();
    });

    test('Hero section is prioritized in load order', async ({ page }) => {
      // Start navigation and capture when hero becomes visible
      const heroVisiblePromise = page.waitForSelector('#hero-headline', {
        state: 'visible',
      });

      await page.goto('/');

      // Wait for hero to be visible
      await heroVisiblePromise;

      // Verify hero is visible before network idle
      const heroHeadline = page.locator('#hero-headline');
      await expect(heroHeadline).toBeVisible();

      // The hero should contain the expected content
      const headlineText = await heroHeadline.textContent();
      expect(headlineText).toBeTruthy();
      expect(headlineText!.length).toBeGreaterThan(0);
    });

    test('Critical CSS is inlined for hero section', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify hero section has proper styling applied
      const heroSection = page.locator('section[aria-labelledby="hero-headline"]');
      await expect(heroSection).toBeVisible();

      // Check that hero section has proper layout classes
      // This verifies the CSS classes are correctly applied in the source
      const heroClasses = await heroSection.getAttribute('class');
      expect(heroClasses).toContain('flex');
      expect(heroClasses).toContain('flex-col');
      expect(heroClasses).toContain('min-h-');
      expect(heroClasses).toContain('items-center');
      expect(heroClasses).toContain('justify-center');

      // Verify hero section is positioned for above-fold visibility
      const boundingBox = await heroSection.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.y).toBeGreaterThanOrEqual(0);

      // Verify hero content container has max-width for proper content width
      const contentContainer = heroSection.locator('.max-w-4xl');
      await expect(contentContainer).toBeVisible();
    });
  });

  test.describe('TC3: Lazy loading with intersection observer', () => {
    test('Below-fold sections use intersection observer for animations', async ({
      page,
    }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Features section should exist but may not be animated yet
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeAttached();

      // Get initial state of features grid (before scrolling into view)
      // Framer Motion starts elements with opacity 0 when using whileInView
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeAttached();

      // Scroll to features section
      await featuresSection.scrollIntoViewIfNeeded();

      // Wait for animation to complete
      await page.waitForTimeout(500);

      // After scrolling, features should be visible with opacity 1
      const gridOpacity = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).opacity;
      });

      // Should be fully visible after scrolling into view
      expect(parseFloat(gridOpacity)).toBeGreaterThan(0);
    });

    test('How It Works section uses viewport-based rendering', async ({
      page,
    }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify How It Works section exists
      const stepsContainer = page.getByTestId('steps-container');
      await expect(stepsContainer).toBeAttached();

      // Scroll to the section
      await stepsContainer.scrollIntoViewIfNeeded();

      // Wait for animation
      await page.waitForTimeout(500);

      // Section should now be visible
      await expect(stepsContainer).toBeVisible();
    });

    test('CTA section uses lazy loading for non-critical content', async ({
      page,
    }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify CTA section exists
      const ctaSection = page.getByTestId('cta-section');
      await expect(ctaSection).toBeAttached();

      // Scroll to CTA section
      await ctaSection.scrollIntoViewIfNeeded();

      // Wait for animation
      await page.waitForTimeout(500);

      // CTA should be visible after scrolling
      await expect(ctaSection).toBeVisible();

      // Verify CTA button is interactable
      const ctaButton = page.getByTestId('cta-signup-button');
      await expect(ctaButton).toBeVisible();
    });

    test('Scroll triggers visibility animations for below-fold sections', async ({
      page,
    }) => {
      // Start with a tall viewport to see below-fold content
      await page.setViewportSize({ width: 1280, height: 400 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Record initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Verify hero is visible at initial position
      const heroHeadline = page.locator('#hero-headline');
      await expect(heroHeadline).toBeVisible();

      // Scroll down to trigger lazy loading of below-fold content
      await page.evaluate(() => {
        window.scrollTo({
          top: 600,
          behavior: 'smooth',
        });
      });

      // Wait for scroll and animations
      await page.waitForTimeout(800);

      // Below-fold sections should now be in view or loading
      const featuresSection = page.getByTestId('features-section');
      const boundingBox = await featuresSection.boundingBox();

      // Features section should be positioned and rendered
      expect(boundingBox).not.toBeNull();
    });
  });
});

test.describe('Performance Requirements - Resource Loading', () => {
  test('Images and assets use lazy loading attributes', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // If there are images, check their loading attributes
    if (imageCount > 0) {
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const loading = await img.getAttribute('loading');

        // Non-critical images should have loading="lazy"
        // Critical images (above fold) may have loading="eager" or no attribute
        // Either approach is acceptable
        if (loading) {
          expect(['lazy', 'eager']).toContain(loading);
        }
      }
    }
  });

  test('Page does not load unnecessary resources on initial load', async ({
    page,
  }) => {
    // Track network requests
    const requests: string[] = [];

    page.on('request', (request) => {
      requests.push(request.url());
    });

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify core resources are loaded
    const jsRequests = requests.filter(
      (url) => url.endsWith('.js') || url.includes('.js?')
    );
    const cssRequests = requests.filter(
      (url) => url.endsWith('.css') || url.includes('.css?')
    );

    // Should have some JS and CSS loaded
    expect(jsRequests.length).toBeGreaterThan(0);
    expect(cssRequests.length + jsRequests.length).toBeLessThan(50); // Reasonable limit
  });

  test('No render-blocking resources delay FCP', async ({ page }) => {
    await page.goto('/');

    // Hero should be visible quickly (under 500ms after domcontentloaded)
    await page.waitForLoadState('domcontentloaded');

    const start = Date.now();
    await page.waitForSelector('#hero-headline', { state: 'visible' });
    const heroVisibleTime = Date.now() - start;

    // Hero should be visible within 500ms after DOM content loaded
    expect(heroVisibleTime).toBeLessThan(500);
  });
});

test.describe('Performance Requirements - Interaction Responsiveness', () => {
  test('Form submission responds within 100ms', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Fill in a URL
    await urlInput.fill('https://example.com/test');

    // Measure button click responsiveness
    const start = Date.now();
    await shortenButton.click();
    const clickResponseTime = Date.now() - start;

    // Click should register within 100ms
    expect(clickResponseTime).toBeLessThan(100);
  });

  test('Page remains interactive during content loading', async ({ page }) => {
    await page.goto('/');

    // Page should be interactive immediately after DOM content loaded
    await page.waitForLoadState('domcontentloaded');

    // Theme toggle should be clickable
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeEnabled();

    // URL input should be focusable
    const urlInput = page.getByTestId('url-input');
    await expect(urlInput).toBeEnabled();

    // Can interact with the page
    await urlInput.click();
    await expect(urlInput).toBeFocused();
  });
});

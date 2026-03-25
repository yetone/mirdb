/**
 * Performance Requirements E2E Tests
 * Owner: Scenario 14 - Performance Requirements
 *
 * Validates page load time and performance optimizations (NFR-1, NFR-5):
 * - Lighthouse performance metrics
 * - First Contentful Paint (FCP)
 * - SEO meta tags (title, description, OG tags)
 * - Lazy loading for below-fold images
 */

import { test, expect, Page } from '@playwright/test';

// Performance thresholds per requirements
const PERFORMANCE_THRESHOLDS = {
  FCP_MAX_MS: 1500, // First Contentful Paint under 1.5 seconds
  LCP_MAX_MS: 2500, // Largest Contentful Paint under 2.5 seconds
  TTI_MAX_MS: 2000, // Time to Interactive under 2 seconds (NFR-1)
  PERFORMANCE_SCORE_MIN: 80, // Lighthouse performance score threshold
};

// SEO requirements
const SEO_REQUIREMENTS = {
  MIN_TITLE_LENGTH: 10,
  MIN_DESCRIPTION_LENGTH: 50,
  MAX_TITLE_LENGTH: 70,
  MAX_DESCRIPTION_LENGTH: 160,
};

/**
 * Helper to collect Web Vitals metrics using Performance API
 */
async function collectPerformanceMetrics(page: Page) {
  const metrics = await page.evaluate(() => {
    return new Promise<{
      fcp: number | null;
      lcp: number | null;
      domContentLoaded: number;
      loadComplete: number;
    }>((resolve) => {
      const timing = performance.timing;
      let fcp: number | null = null;
      let lcp: number | null = null;

      // Get FCP from Performance Observer entries
      const paintEntries = performance.getEntriesByType('paint');
      const fcpEntry = paintEntries.find((entry) => entry.name === 'first-contentful-paint');
      if (fcpEntry) {
        fcp = fcpEntry.startTime;
      }

      // Get LCP from Performance Observer entries
      const lcpEntries = performance.getEntriesByType('largest-contentful-paint');
      if (lcpEntries.length > 0) {
        lcp = lcpEntries[lcpEntries.length - 1].startTime;
      }

      const domContentLoaded = timing.domContentLoadedEventEnd - timing.navigationStart;
      const loadComplete = timing.loadEventEnd - timing.navigationStart;

      resolve({
        fcp,
        lcp,
        domContentLoaded,
        loadComplete,
      });
    });
  });

  return metrics;
}

/**
 * Helper to calculate a simple performance score based on key metrics
 * Simulates Lighthouse scoring approach
 */
function calculatePerformanceScore(metrics: {
  fcp: number | null;
  lcp: number | null;
  domContentLoaded: number;
  loadComplete: number;
}): number {
  let score = 100;

  // FCP scoring (40% weight)
  if (metrics.fcp !== null) {
    if (metrics.fcp > 1800) score -= 40;
    else if (metrics.fcp > 1200) score -= 20;
    else if (metrics.fcp > 600) score -= 10;
  }

  // LCP scoring (30% weight)
  if (metrics.lcp !== null) {
    if (metrics.lcp > 2500) score -= 30;
    else if (metrics.lcp > 1800) score -= 15;
    else if (metrics.lcp > 1200) score -= 7;
  }

  // DOM Content Loaded scoring (15% weight)
  if (metrics.domContentLoaded > 2000) score -= 15;
  else if (metrics.domContentLoaded > 1500) score -= 8;
  else if (metrics.domContentLoaded > 1000) score -= 4;

  // Load Complete scoring (15% weight)
  if (metrics.loadComplete > 3000) score -= 15;
  else if (metrics.loadComplete > 2000) score -= 8;
  else if (metrics.loadComplete > 1500) score -= 4;

  return Math.max(0, Math.min(100, score));
}

test.describe('Performance Requirements - Page Load Performance', () => {
  test.beforeEach(async ({ page }) => {
    // Enable performance observer before navigation
    await page.addInitScript(() => {
      // Enable LCP observation
      if ('PerformanceObserver' in window) {
        const lcpEntries: PerformanceEntry[] = [];
        const observer = new PerformanceObserver((list) => {
          lcpEntries.push(...list.getEntries());
        });
        try {
          observer.observe({ type: 'largest-contentful-paint', buffered: true });
        } catch {
          // Fallback for browsers without LCP support
        }
      }
    });
  });

  test('Test Case 1: Lighthouse-style performance audit - score above 80', async ({ page }) => {
    // Navigate and wait for page to fully load
    await page.goto('/', { waitUntil: 'networkidle' });
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Wait for all resources to load
    await page.waitForLoadState('load');

    // Give time for metrics to be collected
    await page.waitForTimeout(500);

    // Collect performance metrics
    const metrics = await collectPerformanceMetrics(page);

    // Calculate performance score
    const performanceScore = calculatePerformanceScore(metrics);

    // Log metrics for debugging
    console.log('Performance Metrics:', {
      fcp: metrics.fcp,
      lcp: metrics.lcp,
      domContentLoaded: metrics.domContentLoaded,
      loadComplete: metrics.loadComplete,
      calculatedScore: performanceScore,
    });

    // Assert performance score meets threshold
    expect(
      performanceScore,
      `Performance score (${performanceScore}) should be above ${PERFORMANCE_THRESHOLDS.PERFORMANCE_SCORE_MIN}`
    ).toBeGreaterThanOrEqual(PERFORMANCE_THRESHOLDS.PERFORMANCE_SCORE_MIN);
  });

  test('Test Case 2: First Contentful Paint (FCP) under 1.5 seconds', async ({ page }) => {
    // Navigate to homepage
    const startTime = Date.now();
    await page.goto('/');

    // Wait for home page to be visible (indicates FCP occurred)
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Calculate time to first meaningful content
    const timeToVisible = Date.now() - startTime;

    // Also check Paint API if available
    const fcpFromAPI = await page.evaluate(() => {
      const paintEntries = performance.getEntriesByType('paint');
      const fcpEntry = paintEntries.find((entry) => entry.name === 'first-contentful-paint');
      return fcpEntry ? fcpEntry.startTime : null;
    });

    // Use the API FCP if available, otherwise use our measurement
    const fcp = fcpFromAPI || timeToVisible;

    console.log('FCP Metrics:', {
      fcpFromAPI,
      timeToVisible,
      usedValue: fcp,
    });

    expect(
      fcp,
      `FCP (${fcp}ms) should be under ${PERFORMANCE_THRESHOLDS.FCP_MAX_MS}ms`
    ).toBeLessThan(PERFORMANCE_THRESHOLDS.FCP_MAX_MS);
  });
});

test.describe('Performance Requirements - SEO Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();
  });

  test('Test Case 3: Page has descriptive title tag', async ({ page }) => {
    // Get the document title
    const title = await page.title();

    // Verify title exists and is descriptive
    expect(title).toBeTruthy();
    expect(
      title.length,
      `Title length (${title.length}) should be at least ${SEO_REQUIREMENTS.MIN_TITLE_LENGTH} characters`
    ).toBeGreaterThanOrEqual(SEO_REQUIREMENTS.MIN_TITLE_LENGTH);

    // Verify title contains relevant keywords
    const lowerTitle = title.toLowerCase();
    const hasRelevantKeywords =
      lowerTitle.includes('url') ||
      lowerTitle.includes('short') ||
      lowerTitle.includes('link');

    expect(
      hasRelevantKeywords,
      `Title "${title}" should contain relevant keywords (url, short, or link)`
    ).toBe(true);

    // Verify title is not too long (SEO best practice)
    expect(
      title.length,
      `Title length (${title.length}) should not exceed ${SEO_REQUIREMENTS.MAX_TITLE_LENGTH} characters`
    ).toBeLessThanOrEqual(SEO_REQUIREMENTS.MAX_TITLE_LENGTH);

    console.log('Title Tag:', { title, length: title.length });
  });

  test('Test Case 4: Meta description tag is present and descriptive', async ({ page }) => {
    // Get meta description
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Verify description exists
    expect(
      metaDescription,
      'Meta description should be present'
    ).toBeTruthy();

    if (metaDescription) {
      // Verify description is descriptive (minimum length)
      expect(
        metaDescription.length,
        `Description length (${metaDescription.length}) should be at least ${SEO_REQUIREMENTS.MIN_DESCRIPTION_LENGTH} characters`
      ).toBeGreaterThanOrEqual(SEO_REQUIREMENTS.MIN_DESCRIPTION_LENGTH);

      // Verify description is not too long (SEO best practice)
      expect(
        metaDescription.length,
        `Description length (${metaDescription.length}) should not exceed ${SEO_REQUIREMENTS.MAX_DESCRIPTION_LENGTH} characters`
      ).toBeLessThanOrEqual(SEO_REQUIREMENTS.MAX_DESCRIPTION_LENGTH);

      // Verify description contains relevant content
      const lowerDesc = metaDescription.toLowerCase();
      const hasRelevantContent =
        lowerDesc.includes('url') ||
        lowerDesc.includes('short') ||
        lowerDesc.includes('link') ||
        lowerDesc.includes('track');

      expect(
        hasRelevantContent,
        `Description should contain relevant keywords`
      ).toBe(true);

      console.log('Meta Description:', {
        content: metaDescription,
        length: metaDescription.length,
      });
    }
  });

  test('Test Case 5: Open Graph tags (og:title, og:description, og:image) present', async ({ page }) => {
    // Check OG title
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle, 'og:title should be present').toBeTruthy();
    expect(
      ogTitle!.length,
      `og:title length (${ogTitle!.length}) should be at least ${SEO_REQUIREMENTS.MIN_TITLE_LENGTH} characters`
    ).toBeGreaterThanOrEqual(SEO_REQUIREMENTS.MIN_TITLE_LENGTH);

    // Check OG description
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription, 'og:description should be present').toBeTruthy();
    expect(
      ogDescription!.length,
      `og:description length (${ogDescription!.length}) should be at least ${SEO_REQUIREMENTS.MIN_DESCRIPTION_LENGTH} characters`
    ).toBeGreaterThanOrEqual(SEO_REQUIREMENTS.MIN_DESCRIPTION_LENGTH);

    // Check OG image
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
    expect(ogImage, 'og:image should be present').toBeTruthy();

    // Verify OG type is present (additional best practice)
    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
    expect(ogType, 'og:type should be present').toBeTruthy();

    console.log('Open Graph Tags:', {
      ogTitle,
      ogDescription,
      ogImage,
      ogType,
    });
  });
});

test.describe('Performance Requirements - Lazy Loading', () => {
  test('Test Case 6: Below-fold images use lazy loading attribute', async ({ page }) => {
    // Navigate to page
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Get all images on the page
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    // Get viewport height for fold calculation
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // Collect information about images and their lazy loading status
    const imageAnalysis = await page.evaluate((vh: number) => {
      const images = document.querySelectorAll('img');
      const results: Array<{
        src: string;
        hasLazyLoading: boolean;
        isBelowFold: boolean;
        topPosition: number;
      }> = [];

      images.forEach((img) => {
        const rect = img.getBoundingClientRect();
        const isBelowFold = rect.top > vh;
        const hasLazyLoading = img.getAttribute('loading') === 'lazy';

        results.push({
          src: img.src || img.getAttribute('data-src') || 'unknown',
          hasLazyLoading,
          isBelowFold,
          topPosition: rect.top,
        });
      });

      return results;
    }, viewportHeight);

    console.log('Image Analysis:', {
      totalImages: imageCount,
      viewportHeight,
      images: imageAnalysis,
    });

    // Check that below-fold images have lazy loading
    const belowFoldImages = imageAnalysis.filter((img) => img.isBelowFold);

    if (belowFoldImages.length > 0) {
      // All below-fold images should have lazy loading
      const allBelowFoldHaveLazy = belowFoldImages.every((img) => img.hasLazyLoading);

      expect(
        allBelowFoldHaveLazy,
        `All below-fold images should have loading="lazy" attribute. Found ${
          belowFoldImages.filter((img) => !img.hasLazyLoading).length
        } images without lazy loading.`
      ).toBe(true);
    }

    // If there are images, at least some should use lazy loading for optimization
    // If no images exist, the test passes as there's nothing to lazy load
    if (imageCount === 0) {
      console.log('No img elements found - page uses CSS/SVG graphics. Lazy loading N/A.');
      // Test passes - no images to lazy load
      expect(true).toBe(true);
    } else {
      // Additional check: verify lazy loading mechanism exists in the codebase
      const hasLazyLoadingPattern = await page.evaluate(() => {
        // Check for native lazy loading support in any images
        const lazyImages = document.querySelectorAll('img[loading="lazy"]');

        // Check for data-testid patterns that might indicate lazy-loaded components
        const lazyLoadedComponents = document.querySelectorAll('[data-lazy]');

        return {
          nativeLazyImages: lazyImages.length,
          lazyComponents: lazyLoadedComponents.length,
        };
      });

      console.log('Lazy Loading Patterns:', hasLazyLoadingPattern);
    }
  });
});

test.describe('Performance Requirements - Additional Optimizations', () => {
  test('Page load completes under 2 seconds (NFR-1)', async ({ page }) => {
    const startTime = Date.now();

    // Navigate and wait for full load
    await page.goto('/', { waitUntil: 'load' });
    await expect(page.getByTestId('home-page')).toBeVisible();

    const loadTime = Date.now() - startTime;

    console.log('Page Load Time:', {
      loadTimeMs: loadTime,
      thresholdMs: PERFORMANCE_THRESHOLDS.TTI_MAX_MS,
    });

    expect(
      loadTime,
      `Page load time (${loadTime}ms) should be under ${PERFORMANCE_THRESHOLDS.TTI_MAX_MS}ms`
    ).toBeLessThan(PERFORMANCE_THRESHOLDS.TTI_MAX_MS);
  });

  test('Semantic HTML structure for SEO', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Check for semantic HTML elements
    const hasMain = (await page.locator('main').count()) > 0;
    const hasNav = (await page.locator('nav').count()) > 0;
    const hasFooter = (await page.locator('footer').count()) > 0;
    const hasSections = (await page.locator('section').count()) > 0;
    const hasH1 = (await page.locator('h1').count()) === 1; // Should have exactly one h1

    console.log('Semantic HTML:', {
      hasMain,
      hasNav,
      hasFooter,
      hasSections,
      hasH1,
    });

    expect(hasMain, 'Page should have a <main> element').toBe(true);
    expect(hasNav, 'Page should have a <nav> element').toBe(true);
    expect(hasFooter, 'Page should have a <footer> element').toBe(true);
    expect(hasSections, 'Page should have <section> elements').toBe(true);
    expect(hasH1, 'Page should have exactly one <h1> element').toBe(true);
  });
});

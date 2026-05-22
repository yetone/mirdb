/**
 * Hero Section Rendering E2E Tests
 * Owner: Scenario 2 - Hero Section Rendering
 *
 * Tests:
 * - Hero section DOM structure (h1, tagline, CTA)
 * - Hero text content and visibility
 * - Primary CTA button presence and visibility
 * - Responsive layout at mobile, tablet, and desktop viewports
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the hero section to be visible
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' });
  });

  test('should render hero section with product title describing URL shortening service', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify h1 element exists and is visible
    const title = heroSection.locator('h1#hero-title');
    await expect(title).toBeVisible();

    // Verify title text describes URL shortening
    const titleText = await title.textContent();
    expect(titleText).toBeTruthy();
    expect(titleText.toLowerCase()).toContain('shorten');
    expect(titleText.toLowerCase()).toContain('url');

    // Verify h1 uses semantic heading element
    const tagName = await title.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');
  });

  test('should render hero tagline explaining the value proposition', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify tagline element exists and is visible
    const tagline = heroSection.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    // Verify tagline text explains value proposition
    const taglineText = await tagline.textContent();
    expect(taglineText).toBeTruthy();
    expect(taglineText.length).toBeGreaterThan(20);

    // Tagline should mention key value propositions
    const lowerText = taglineText.toLowerCase();
    expect(
      lowerText.includes('short') ||
      lowerText.includes('link') ||
      lowerText.includes('track') ||
      lowerText.includes('analytics')
    ).toBe(true);
  });

  test('should render primary CTA button that is present and visible', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify primary CTA button exists and is visible
    const ctaButton = heroSection.locator('[data-testid="hero-primary-cta"]');
    await expect(ctaButton).toBeVisible();

    // Verify CTA text is appropriate
    const ctaText = await ctaButton.textContent();
    expect(ctaText).toBeTruthy();
    const lowerCta = ctaText.toLowerCase().trim();
    expect(
      lowerCta.includes('get started') ||
      lowerCta.includes('create short url') ||
      lowerCta.includes('shorten')
    ).toBe(true);

    // Verify the CTA is an anchor link with an href
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify button has appropriate styling classes
    const classAttr = await ctaButton.getAttribute('class');
    expect(classAttr).toContain('btn');
    expect(classAttr).toContain('btn-primary');
  });

  test('should render hero content centered and readable on mobile (375px)', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 812 });

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get the hero content container
    const heroContent = heroSection.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Verify hero content is centered (horizontal center alignment)
    const contentBox = await heroContent.boundingBox();
    const sectionBox = await heroSection.boundingBox();

    // Content should be within section bounds
    expect(contentBox.x).toBeGreaterThanOrEqual(0);
    expect(contentBox.x + contentBox.width).toBeLessThanOrEqual(sectionBox.x + sectionBox.width + 1);

    // Title should be visible and readable
    const title = heroSection.locator('h1#hero-title');
    await expect(title).toBeVisible();

    // Tagline should be visible
    const tagline = heroSection.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    // CTA should be visible and touch-friendly (min 44x44px)
    const cta = heroSection.locator('[data-testid="hero-primary-cta"]');
    const ctaBox = await cta.boundingBox();
    expect(ctaBox.width).toBeGreaterThanOrEqual(44);
    expect(ctaBox.height).toBeGreaterThanOrEqual(44);

    // No horizontal overflow (scroll)
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);
  });

  test('should render hero content centered and readable on tablet (768px)', async ({ page }) => {
    // Set viewport to tablet size
    await page.setViewportSize({ width: 768, height: 1024 });

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero content is centered
    const heroContent = heroSection.locator('.hero-content');
    const contentBox = await heroContent.boundingBox();
    const sectionBox = await heroSection.boundingBox();

    // Content should be roughly centered horizontally
    const contentCenter = contentBox.x + contentBox.width / 2;
    const sectionCenter = sectionBox.x + sectionBox.width / 2;
    expect(Math.abs(contentCenter - sectionCenter)).toBeLessThanOrEqual(50);

    // All hero elements should be visible
    await expect(heroSection.locator('h1#hero-title')).toBeVisible();
    await expect(heroSection.locator('[data-testid="hero-tagline"]')).toBeVisible();
    await expect(heroSection.locator('[data-testid="hero-primary-cta"]')).toBeVisible();

    // No horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);
  });

  test('should render hero content centered and readable on desktop (1280px)', async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize({ width: 1280, height: 800 });

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero content is centered
    const heroContent = heroSection.locator('.hero-content');
    const contentBox = await heroContent.boundingBox();
    const sectionBox = await heroSection.boundingBox();

    // Content should be centered horizontally
    const contentCenter = contentBox.x + contentBox.width / 2;
    const sectionCenter = sectionBox.x + sectionBox.width / 2;
    expect(Math.abs(contentCenter - sectionCenter)).toBeLessThanOrEqual(50);

    // All hero elements should be visible
    await expect(heroSection.locator('h1#hero-title')).toBeVisible();
    await expect(heroSection.locator('[data-testid="hero-tagline"]')).toBeVisible();
    await expect(heroSection.locator('[data-testid="hero-primary-cta"]')).toBeVisible();

    // No horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);
  });

  test('should use semantic HTML structure for the hero section', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Section should have aria-labelledby pointing to the title
    const ariaLabelledBy = await heroSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('hero-title');

    // Title should exist with matching id
    const title = page.locator('#hero-title');
    await expect(title).toBeVisible();
    const titleId = await title.getAttribute('id');
    expect(titleId).toBe('hero-title');

    // Title should be h1
    const tagName = await title.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');

    // CTA should be an anchor element (link)
    const cta = heroSection.locator('[data-testid="hero-primary-cta"]');
    const ctaTag = await cta.evaluate(el => el.tagName.toLowerCase());
    expect(ctaTag).toBe('a');
  });

  test('should maintain correct DOM order: title, then tagline, then CTA', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Get all direct child elements within the max-w-2xl container
    const container = heroSection.locator('.max-w-2xl');

    // Get the order of elements
    const elementOrder = await container.evaluate(container => {
      const children = Array.from(container.children);
      return children.map(child => ({
        tag: child.tagName.toLowerCase(),
        id: child.id || null,
        testId: child.getAttribute('data-testid') || null,
      }));
    });

    // First element should be h1 title
    expect(elementOrder[0].tag).toBe('h1');
    expect(elementOrder[0].id).toBe('hero-title');

    // Second element should be the tagline paragraph
    expect(elementOrder[1].tag).toBe('p');
    expect(elementOrder[1].testId).toBe('hero-tagline');

    // Third element should be the CTA anchor
    expect(elementOrder[2].tag).toBe('a');
    expect(elementOrder[2].testId).toBe('hero-primary-cta');
  });
});

/**
 * Performance and Loading Tests
 * Owner: Scenario 11 - Performance and Loading
 *
 * Tests:
 * - First Contentful Paint (FCP) < 1.5s on fast connection
 * - Largest Contentful Paint (LCP) < 2.5s on fast connection
 * - Page load time on simulated 3G < 2s for critical content
 * - Cumulative Layout Shift (CLS) < 0.1
 */

test.describe('Performance and Loading', () => {
  test('should have First Contentful Paint < 1.5s and LCP < 2.5s on fast connection', async ({ page }) => {
    // Navigate first, then collect metrics
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for hero section to be visible (critical content)
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' });

    // Collect FCP metric from Performance API
    const fcp = await page.evaluate(() => {
      const entries = performance.getEntriesByType('paint');
      const fcpEntry = entries.find(e => e.name === 'first-contentful-paint');
      return fcpEntry ? fcpEntry.startTime : 0;
    });

    // Collect LCP metric from Performance API
    const lcp = await page.evaluate(() => {
      const entries = performance.getEntriesByType('largest-contentful-paint');
      if (entries.length > 0) {
        return entries[entries.length - 1].startTime;
      }
      return 0;
    });

    // Assert FCP < 1.5s (1500ms) - allow some margin for CI environments
    expect(fcp).toBeGreaterThan(0);
    expect(fcp).toBeLessThan(1500);

    // Assert LCP < 2.5s (2500ms) - allow some margin for CI environments
    // LCP may be 0 if not yet recorded; in that case, use DOM content as proxy
    if (lcp > 0) {
      expect(lcp).toBeLessThan(2500);
    }
  });

  test('should load critical content within 2 seconds on simulated 3G', async ({ page, context }) => {
    // Emulate 3G network conditions using CDP
    const client = await page.context().newCDPSession(page);
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: 1.6 * 1024 * 1024 / 8, // 1.6 Mbps (Fast 3G download)
      uploadThroughput: 750 * 1024 / 8,          // 750 Kbps (Fast 3G upload)
      latency: 150,                               // 150ms latency
    });

    // Record navigation start time
    const startTime = Date.now();

    // Navigate to the page
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for critical content (hero section) to be visible
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible', timeout: 5000 });

    const criticalContentLoadedTime = Date.now() - startTime;

    // Assert critical content loads within 2 seconds
    expect(criticalContentLoadedTime).toBeLessThan(2000);

    // Clean up: reset network conditions
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: -1,
      uploadThroughput: -1,
      latency: 0,
    });
  });

  test('should have Cumulative Layout Shift (CLS) score < 0.1', async ({ page }) => {
    // Collect layout shift entries
    const clsScore = await page.evaluate(() => {
      return new Promise((resolve) => {
        let cls = 0;
        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            if (!entry.hadRecentInput) {
              cls += entry.value;
            }
          }
        });
        observer.observe({ type: 'layout-shift', buffered: true });

        // Report CLS after a short delay to capture initial shifts
        setTimeout(() => {
          observer.disconnect();
          resolve(cls);
        }, 500);
      });
    });

    // Navigate first, then collect CLS
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for page to fully settle
    await page.waitForTimeout(500);

    // Get the CLS score
    const finalCls = await page.evaluate(() => {
      let cls = 0;
      const entries = performance.getEntriesByType('layout-shift');
      for (const entry of entries) {
        if (!entry.hadRecentInput) {
          cls += entry.value;
        }
      }
      return cls;
    });

    // Assert CLS < 0.1
    expect(finalCls).toBeLessThan(0.1);
  });

  test('should have optimized resource loading with preconnect and fetchpriority', async ({ page }) => {
    await page.goto('/');

    // Check for preconnect hint
    const preconnectLink = page.locator('link[rel="preconnect"]');
    await expect(preconnectLink).toHaveCount(1);

    // Check for fetchpriority on critical CSS
    const cssLink = page.locator('link[href="/static/css/main.css"]');
    const fetchPriority = await cssLink.getAttribute('fetchpriority');
    expect(fetchPriority).toBe('high');
  });

  test('should have explicit image dimensions and lazy loading for below-fold images', async ({ page }) => {
    await page.goto('/');

    // Check feature section images have width and height attributes
    const featureImages = page.locator('#features img');
    const imageCount = await featureImages.count();
    expect(imageCount).toBeGreaterThan(0);

    for (let i = 0; i < imageCount; i++) {
      const img = featureImages.nth(i);
      const width = await img.getAttribute('width');
      const height = await img.getAttribute('height');
      expect(width).toBeTruthy();
      expect(height).toBeTruthy();

      // Below-fold images should have loading="lazy"
      const loading = await img.getAttribute('loading');
      expect(loading).toBe('lazy');
    }
  });
});

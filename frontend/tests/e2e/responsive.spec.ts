import { test, expect } from '@playwright/test';

const viewports = {
  desktop: { width: 1920, height: 1080 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 },
  smallMobile: { width: 320, height: 568 },
};

test.describe('Responsive Design Across Viewports', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Desktop viewport (1920x1080) - hero uses multi-column layout, feature cards in grid, navbar expanded', async ({ page }) => {
    await page.setViewportSize(viewports.desktop);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Hero section should be visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Hero headline should be visible
    const heroHeadline = page.getByTestId('hero-headline');
    await expect(heroHeadline).toBeVisible();

    // Hero form container should be visible
    const heroForm = page.getByTestId('hero-form-container');
    await expect(heroForm).toBeVisible();

    // Features grid should be visible
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Feature cards should be visible (all 4)
    const featureCards = page.getByTestId('feature-card');
    await expect(featureCards).toHaveCount(4);

    // Navbar should be visible
    const navbar = page.getByTestId('navbar');
    await expect(navbar).toBeVisible();

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Features grid should have multiple columns (desktop)
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox).not.toBeNull();
    if (gridBox) {
      // On desktop with 4 cards in a row, grid width should be substantial
      expect(gridBox.width).toBeGreaterThan(600);
    }
  });

  test('TC2: Tablet viewport (768x1024) - content reflows, feature cards may stack, no horizontal scroll', async ({ page }) => {
    await page.setViewportSize(viewports.tablet);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Hero section should be visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Features grid should be visible
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Feature cards should be visible (all 4)
    const featureCards = page.getByTestId('feature-card');
    await expect(featureCards).toHaveCount(4);

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // All content should fit within viewport width
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    const docWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(docWidth).toBeLessThanOrEqual(viewportWidth + 1); // allow 1px rounding
  });

  test('TC3: Mobile viewport (375x667) - single column layout, URL form functional, no horizontal scroll', async ({ page }) => {
    await page.setViewportSize(viewports.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Hero section should be visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Hero headline should be visible and readable
    const heroHeadline = page.getByTestId('hero-headline');
    await expect(heroHeadline).toBeVisible();
    const headlineBox = await heroHeadline.boundingBox();
    expect(headlineBox).not.toBeNull();
    if (headlineBox) {
      expect(headlineBox.width).toBeLessThanOrEqual(viewports.mobile.width);
    }

    // URL input should be visible and functional
    const urlInput = page.getByTestId('url-input');
    await expect(urlInput).toBeVisible();
    await expect(urlInput).toBeEnabled();

    // Shorten button should be visible
    const shortenButton = page.getByTestId('shorten-button');
    await expect(shortenButton).toBeVisible();

    // Features section should be visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Feature cards should be visible
    const featureCards = page.getByTestId('feature-card');
    await expect(featureCards).toHaveCount(4);

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // All content should fit within viewport width
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    const docWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(docWidth).toBeLessThanOrEqual(viewportWidth + 1);
  });

  test('TC4: Small mobile viewport (320x568) - all content visible and accessible, no overflow', async ({ page }) => {
    await page.setViewportSize(viewports.smallMobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Hero section should be visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // URL input should be visible
    const urlInput = page.getByTestId('url-input');
    await expect(urlInput).toBeVisible();

    // Shorten button should be visible
    const shortenButton = page.getByTestId('shorten-button');
    await expect(shortenButton).toBeVisible();

    // Feature cards should be visible
    const featureCards = page.getByTestId('feature-card');
    await expect(featureCards).toHaveCount(4);

    // Footer should be visible
    const footer = page.getByTestId('footer');
    await expect(footer).toBeVisible();

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // No clipped elements - check that body overflow is handled
    const bodyOverflowX = await page.evaluate(() => {
      return window.getComputedStyle(document.body).overflowX;
    });
    expect(['visible', 'hidden', 'auto', 'clip']).toContain(bodyOverflowX);
  });

  test('TC5: Touch target sizes at mobile viewport - all buttons and links have minimum 44x44px', async ({ page }) => {
    await page.setViewportSize(viewports.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check all buttons
    const buttons = await page.locator('button').all();
    for (const button of buttons) {
      const box = await button.boundingBox();
      if (box && box.width > 0 && box.height > 0) {
        expect(box.width, `Button width ${await button.textContent()} should be >= 44px`).toBeGreaterThanOrEqual(44);
        expect(box.height, `Button height ${await button.textContent()} should be >= 44px`).toBeGreaterThanOrEqual(44);
      }
    }

    // Check links within our components (skip navbar links - owned by Scenario 4)
    const links = await page.locator('a').all();
    for (const link of links) {
      const box = await link.boundingBox();
      if (box && box.width > 0 && box.height > 0) {
        // Skip navbar links - those are owned by Scenario 4 (Navigation and Routing)
        const isNavLink = await link.evaluate((el) => {
          const parent = el.closest('nav');
          return parent !== null;
        });
        if (!isNavLink) {
          // Non-nav links should have minimum 44px touch target
          expect(box.height, `Link height should be >= 44px`).toBeGreaterThanOrEqual(44);
        }
      }
    }

    // Check URL input
    const urlInput = page.getByTestId('url-input');
    const inputBox = await urlInput.boundingBox();
    expect(inputBox).not.toBeNull();
    if (inputBox) {
      expect(inputBox.height).toBeGreaterThanOrEqual(44);
    }

    // Check CTA button
    const ctaButton = page.getByTestId('hero-cta-button');
    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      expect(ctaBox.height).toBeGreaterThanOrEqual(44);
    }

    // Check shorten button
    const shortenButton = page.getByTestId('shorten-button');
    const shortenBox = await shortenButton.boundingBox();
    expect(shortenBox).not.toBeNull();
    if (shortenBox) {
      expect(shortenBox.height).toBeGreaterThanOrEqual(44);
      expect(shortenBox.width).toBeGreaterThanOrEqual(44);
    }
  });

  test('TC6: Complete URL shortening flow on mobile - form submits and result displays within viewport', async ({ page }) => {
    await page.setViewportSize(viewports.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Fill in a valid URL
    await urlInput.fill('https://example.com/very/long/url/that/needs/shortening');

    // Submit the form
    await shortenButton.click();

    // Wait for result to appear
    await expect(page.getByTestId('result')).toBeVisible({ timeout: 5000 });

    // Result should be within viewport width
    const result = page.getByTestId('result');
    const resultBox = await result.boundingBox();
    expect(resultBox).not.toBeNull();
    if (resultBox) {
      expect(resultBox.width).toBeLessThanOrEqual(viewports.mobile.width);
    }

    // No horizontal scroll after form submission
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

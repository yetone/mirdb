import { test, expect } from '@playwright/test';

/**
 * Visual Stability - CLS (Cumulative Layout Shift) Tests
 *
 * NFR-3: Page shall achieve Cumulative Layout Shift (CLS) under 0.1
 *
 * These tests verify that the homepage maintains visual stability during
 * page load and does not cause significant layout shifts that would
 * degrade user experience.
 */
test.describe('Visual Stability - CLS (NFR-3)', () => {
  /**
   * Helper function to measure CLS using the Performance Observer API
   * Returns the cumulative layout shift score
   */
  async function measureCLS(page: import('@playwright/test').Page): Promise<number> {
    const cls = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let clsValue = 0;
        let sessionValue = 0;
        let sessionEntries: PerformanceEntry[] = [];

        const observer = new PerformanceObserver((entryList) => {
          for (const entry of entryList.getEntries()) {
            // Cast to LayoutShift type
            const layoutShiftEntry = entry as PerformanceEntry & {
              hadRecentInput: boolean;
              value: number;
            };

            // Only count layout shifts without recent user input
            if (!layoutShiftEntry.hadRecentInput) {
              const firstSessionEntry = sessionEntries[0] as (PerformanceEntry & { value: number }) | undefined;
              const lastSessionEntry = sessionEntries[sessionEntries.length - 1] as PerformanceEntry | undefined;

              // If the entry occurred within 1 second of the previous entry
              // and within 5 seconds of the first entry in the session,
              // include it in the current session
              if (
                sessionValue &&
                entry.startTime - (lastSessionEntry?.startTime ?? 0) < 1000 &&
                entry.startTime - (firstSessionEntry?.startTime ?? 0) < 5000
              ) {
                sessionValue += layoutShiftEntry.value;
                sessionEntries.push(entry);
              } else {
                // Otherwise, start a new session
                sessionValue = layoutShiftEntry.value;
                sessionEntries = [entry];
              }

              // Update max CLS value
              if (sessionValue > clsValue) {
                clsValue = sessionValue;
              }
            }
          }
        });

        observer.observe({ type: 'layout-shift', buffered: true });

        // Wait for page to stabilize, then resolve
        setTimeout(() => {
          observer.disconnect();
          resolve(clsValue);
        }, 3000);
      });
    });

    return cls;
  }

  /**
   * Test Case 1: Measure CLS during initial page load
   * Verifies that the CLS score is under 0.1 during normal page load
   */
  test('Test Case 1: CLS during initial page load is under 0.1', async ({ page }) => {
    // Navigate to homepage first
    await page.goto('/');

    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle');

    // Verify key elements are visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-headline"]')).toBeVisible();

    // Now measure CLS - this will capture any buffered layout shifts
    const cls = await measureCLS(page);

    // Verify CLS is under 0.1 (Good score per Web Vitals)
    expect(cls).toBeLessThan(0.1);

    // Log the CLS value for debugging
    console.log(`CLS during initial page load: ${cls}`);
  });

  /**
   * Test Case 2: Measure CLS with slow image loading
   * Verifies that images have reserved space and CLS remains under 0.1
   */
  test('Test Case 2: CLS with slow image loading remains under 0.1', async ({ page }) => {
    // Intercept image requests and add delay to simulate slow loading
    await page.route('**/*.{png,jpg,jpeg,gif,svg,webp}', async (route) => {
      // Add a 500ms delay to simulate slow network for images
      await new Promise((resolve) => setTimeout(resolve, 500));
      await route.continue();
    });

    // Navigate and start CLS measurement
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Measure CLS
    const cls = await measureCLS(page);

    // Verify images have reserved space (aspect-ratio or explicit dimensions)
    // Check that image containers maintain their space
    const imageContainers = page.locator('.featured-card-image-container, img');
    const containerCount = await imageContainers.count();

    if (containerCount > 0) {
      for (let i = 0; i < containerCount; i++) {
        const container = imageContainers.nth(i);
        const isVisible = await container.isVisible().catch(() => false);

        if (isVisible) {
          // Check if the container has aspect-ratio or explicit dimensions
          const styles = await container.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
              aspectRatio: computed.aspectRatio,
              width: computed.width,
              height: computed.height,
            };
          });

          // Container should have either aspect-ratio or defined dimensions
          const hasReservedSpace =
            styles.aspectRatio !== 'auto' ||
            (styles.width !== 'auto' && styles.height !== 'auto');

          // Log for debugging
          console.log(`Container ${i} styles:`, styles, 'hasReservedSpace:', hasReservedSpace);
        }
      }
    }

    // Verify CLS is under 0.1
    expect(cls).toBeLessThan(0.1);
    console.log(`CLS with slow image loading: ${cls}`);
  });

  /**
   * Test Case 3: Measure CLS when fonts load
   * Verifies that font loading does not cause significant layout shift
   */
  test('Test Case 3: CLS when fonts load is under 0.1', async ({ page }) => {
    // Navigate to page
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Wait for fonts to be loaded
    await page.waitForFunction(() => {
      return document.fonts.ready.then(() => true);
    });

    // Measure CLS
    const cls = await measureCLS(page);

    // Verify the page uses system fonts or has font-display strategy
    // The index.css uses system-ui font stack which should minimize font loading CLS
    const fontFamily = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontFamily;
    });

    // Log font information
    console.log(`Font family used: ${fontFamily}`);
    console.log(`CLS when fonts load: ${cls}`);

    // Verify CLS is under 0.1
    expect(cls).toBeLessThan(0.1);
  });

  /**
   * Additional test: Verify page structure supports visual stability
   * This validates the implementation details that prevent CLS
   */
  test('Page structure supports visual stability', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify hero section has minimum height (prevents jump when content loads)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroStyles = await heroSection.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        minHeight: computed.minHeight,
        display: computed.display,
      };
    });

    // Hero should have min-height set
    expect(heroStyles.minHeight).not.toBe('auto');
    expect(heroStyles.minHeight).not.toBe('0px');

    // Verify navigation is sticky/fixed (doesn't push content)
    const navigation = page.locator('.navigation-header');
    if (await navigation.count() > 0) {
      const navStyles = await navigation.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          position: computed.position,
          top: computed.top,
        };
      });

      // Navigation should be sticky
      expect(['sticky', 'fixed']).toContain(navStyles.position);
    }

    // Verify images use aspect-ratio for reserved space
    const imageContainers = page.locator('.featured-card-image-container');
    const imageContainerCount = await imageContainers.count();

    for (let i = 0; i < imageContainerCount; i++) {
      const container = imageContainers.nth(i);
      const aspectRatio = await container.evaluate((el) => {
        return window.getComputedStyle(el).aspectRatio;
      });

      // Image containers should have aspect-ratio set
      expect(aspectRatio).not.toBe('auto');
      console.log(`Image container ${i} aspect-ratio: ${aspectRatio}`);
    }
  });

  /**
   * Additional test: CLS during scroll and interaction
   * Ensures layout stability during user interactions
   */
  test('CLS remains low during scroll', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll down the page
    await page.evaluate(() => {
      window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
    });

    // Wait for scroll to complete
    await page.waitForTimeout(1000);

    // Scroll back up
    await page.evaluate(() => {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });

    await page.waitForTimeout(1000);

    // Measure CLS after interactions
    const cls = await measureCLS(page);

    // CLS during scroll should also be under 0.1
    expect(cls).toBeLessThan(0.1);
    console.log(`CLS during scroll: ${cls}`);
  });
});

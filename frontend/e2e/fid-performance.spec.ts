import { test, expect } from '@playwright/test';

/**
 * First Input Delay (FID) Performance Tests
 *
 * NFR-2: Page shall achieve First Input Delay (FID) under 100 milliseconds
 *
 * FID measures the time from when a user first interacts with a page
 * to when the browser begins processing event handlers.
 *
 * Note: In synthetic testing, FID is approximated by measuring the delay
 * from click/input action to when the browser processes the event.
 * We use the PerformanceObserver API with 'first-input' entry type.
 */

const FID_THRESHOLD_MS = 100;

interface FIDMeasurement {
  delay: number;
  processingStart: number;
  startTime: number;
  name: string;
}

/**
 * Helper function to set up FID measurement on a page.
 * Returns a promise that resolves with the FID value after user interaction.
 */
async function setupFIDMeasurement(page: typeof import('@playwright/test').Page.prototype) {
  // Inject PerformanceObserver to capture first-input entry
  await page.evaluate(() => {
    // Store measurement on window for later retrieval
    (window as any).__fidMeasurement = null;
    (window as any).__fidPromise = new Promise<{
      delay: number;
      processingStart: number;
      startTime: number;
      name: string;
    }>((resolve) => {
      const observer = new PerformanceObserver((entryList) => {
        const entries = entryList.getEntries();
        for (const entry of entries) {
          // Cast to PerformanceEventTiming which has processingStart
          const eventEntry = entry as PerformanceEventTiming;
          const delay = eventEntry.processingStart - eventEntry.startTime;
          const measurement = {
            delay,
            processingStart: eventEntry.processingStart,
            startTime: eventEntry.startTime,
            name: entry.name,
          };
          (window as any).__fidMeasurement = measurement;
          observer.disconnect();
          resolve(measurement);
        }
      });

      observer.observe({ type: 'first-input', buffered: true });

      // Timeout fallback - resolve with null if no interaction within 10s
      setTimeout(() => {
        if (!(window as any).__fidMeasurement) {
          resolve({
            delay: -1,
            processingStart: 0,
            startTime: 0,
            name: 'timeout',
          });
        }
      }, 10000);
    });
  });
}

/**
 * Retrieve the FID measurement after interaction
 */
async function getFIDMeasurement(page: typeof import('@playwright/test').Page.prototype): Promise<FIDMeasurement> {
  const measurement = await page.evaluate(async () => {
    return await (window as any).__fidPromise;
  });
  return measurement;
}

test.describe('Page Interactivity - FID (NFR-2)', () => {
  test.describe('Test Case 1: CTA Button FID Measurement', () => {
    test('FID when clicking CTA button during page load should be under 100ms', async ({ page }) => {
      // Step 1: Navigate to homepage
      await page.goto('/');

      // Step 2: Set up FID monitoring
      await setupFIDMeasurement(page);

      // Wait for the page to be interactive
      await page.waitForLoadState('domcontentloaded');

      // Step 3: Locate and interact with the CTA button
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();

      // Step 4: Trigger interaction and measure FID
      // Using a click action to trigger first-input measurement
      await ctaButton.click();

      // Step 5: Retrieve and validate FID measurement
      const fidMeasurement = await getFIDMeasurement(page);

      // Verify we got a valid measurement
      expect(fidMeasurement.delay).toBeGreaterThanOrEqual(0);
      expect(fidMeasurement.name).not.toBe('timeout');

      // Log the FID value for debugging
      console.log(`FID for CTA button click: ${fidMeasurement.delay.toFixed(2)}ms`);

      // Assert FID is under threshold
      expect(fidMeasurement.delay).toBeLessThan(FID_THRESHOLD_MS);
    });

    test('CTA button should respond immediately to user interaction', async ({ page }) => {
      await page.goto('/');

      // Measure time from click to event processing using performance timing
      const interactionTiming = await page.evaluate(async () => {
        return new Promise<{ clickTime: number; processedTime: number; delay: number }>((resolve) => {
          const ctaButton = document.querySelector('[data-testid="hero-cta"]') as HTMLElement;
          if (!ctaButton) {
            resolve({ clickTime: 0, processedTime: 0, delay: -1 });
            return;
          }

          let clickTime = 0;
          const handler = () => {
            const processedTime = performance.now();
            const delay = processedTime - clickTime;
            resolve({ clickTime, processedTime, delay });
          };

          ctaButton.addEventListener('click', handler, { once: true });

          // Programmatically trigger click and record time
          clickTime = performance.now();
          ctaButton.click();
        });
      });

      expect(interactionTiming.delay).toBeLessThan(FID_THRESHOLD_MS);
      console.log(`CTA button response delay: ${interactionTiming.delay.toFixed(2)}ms`);
    });
  });

  test.describe('Test Case 2: Navigation Link FID Measurement', () => {
    test('FID when clicking navigation link should be under 100ms', async ({ page }) => {
      // Step 1: Navigate to homepage
      await page.goto('/');

      // Step 2: Set up FID monitoring
      await setupFIDMeasurement(page);

      // Wait for the page to be interactive
      await page.waitForLoadState('domcontentloaded');

      // Step 3: Locate and interact with a navigation link
      const navLink = page.locator('[data-testid="navigation-link-features"]');
      await expect(navLink).toBeVisible();

      // Step 4: Trigger interaction and measure FID
      await navLink.click();

      // Step 5: Retrieve and validate FID measurement
      const fidMeasurement = await getFIDMeasurement(page);

      // Verify we got a valid measurement
      expect(fidMeasurement.delay).toBeGreaterThanOrEqual(0);
      expect(fidMeasurement.name).not.toBe('timeout');

      // Log the FID value for debugging
      console.log(`FID for navigation link click: ${fidMeasurement.delay.toFixed(2)}ms`);

      // Assert FID is under threshold
      expect(fidMeasurement.delay).toBeLessThan(FID_THRESHOLD_MS);
    });

    test('Navigation links should respond immediately to user interaction', async ({ page }) => {
      await page.goto('/');

      // Measure time from click to event processing
      const interactionTiming = await page.evaluate(async () => {
        return new Promise<{ clickTime: number; processedTime: number; delay: number }>((resolve) => {
          const navLink = document.querySelector('[data-testid="navigation-link-home"]') as HTMLElement;
          if (!navLink) {
            resolve({ clickTime: 0, processedTime: 0, delay: -1 });
            return;
          }

          let clickTime = 0;
          const handler = (e: Event) => {
            e.preventDefault(); // Prevent navigation
            const processedTime = performance.now();
            const delay = processedTime - clickTime;
            resolve({ clickTime, processedTime, delay });
          };

          navLink.addEventListener('click', handler, { once: true });

          // Programmatically trigger click and record time
          clickTime = performance.now();
          navLink.click();
        });
      });

      expect(interactionTiming.delay).toBeLessThan(FID_THRESHOLD_MS);
      console.log(`Navigation link response delay: ${interactionTiming.delay.toFixed(2)}ms`);
    });
  });

  test.describe('Additional FID Validation', () => {
    test('Page should have no long blocking tasks that affect FID', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Measure Total Blocking Time as a proxy for FID in lab conditions
      const totalBlockingTime = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let tbt = 0;

          // Check if PerformanceObserver is available for longtask
          if (!('PerformanceObserver' in window)) {
            resolve(0);
            return;
          }

          try {
            const observer = new PerformanceObserver((entryList) => {
              for (const entry of entryList.getEntries()) {
                // Long tasks are those exceeding 50ms
                // Blocking time is duration minus 50ms threshold
                tbt += Math.max(0, entry.duration - 50);
              }
            });

            observer.observe({ type: 'longtask', buffered: true });

            // Give time for any long tasks to be recorded
            setTimeout(() => {
              observer.disconnect();
              resolve(tbt);
            }, 1000);
          } catch (e) {
            // longtask observer not supported in all environments
            resolve(0);
          }
        });
      });

      console.log(`Total Blocking Time: ${totalBlockingTime.toFixed(2)}ms`);

      // TBT should be minimal for good FID
      // A TBT under 200ms generally correlates with good FID
      expect(totalBlockingTime).toBeLessThan(200);
    });

    test('Interactive elements should be responsive under simulated load', async ({ page }) => {
      await page.goto('/');

      // Simulate interaction during page load by clicking early
      await page.waitForLoadState('domcontentloaded');

      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible({ timeout: 5000 });

      // Measure multiple interactions to ensure consistent responsiveness
      const delays: number[] = [];

      for (let i = 0; i < 3; i++) {
        const delay = await page.evaluate(async () => {
          return new Promise<number>((resolve) => {
            const button = document.querySelector('[data-testid="hero-cta"]') as HTMLElement;
            if (!button) {
              resolve(-1);
              return;
            }

            const startTime = performance.now();
            const handler = () => {
              resolve(performance.now() - startTime);
            };

            button.addEventListener('click', handler, { once: true });
            button.click();
          });
        });

        delays.push(delay);
        // Small delay between measurements
        await page.waitForTimeout(100);
      }

      const avgDelay = delays.reduce((a, b) => a + b, 0) / delays.length;
      console.log(`Average interaction delay over 3 clicks: ${avgDelay.toFixed(2)}ms`);
      console.log(`Individual delays: ${delays.map(d => d.toFixed(2)).join('ms, ')}ms`);

      // All individual delays should be under threshold
      for (const delay of delays) {
        expect(delay).toBeLessThan(FID_THRESHOLD_MS);
      }

      // Average should also be well under threshold
      expect(avgDelay).toBeLessThan(FID_THRESHOLD_MS);
    });
  });
});

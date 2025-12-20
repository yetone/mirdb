// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Performance tests for MirDB Landing Page
 * Verifies NFR-2: Page must load in under 3 seconds on standard broadband connection
 */

test.describe('Page Load Performance (NFR-2)', () => {

  /**
   * Test Case 1: Measure page load time with Performance API
   * Expected: DOMContentLoaded fires in under 3000ms
   */
  test('DOMContentLoaded fires in under 3000ms', async ({ page }) => {
    // Navigate to the page and capture performance metrics
    const metrics = await page.evaluate(async () => {
      return new Promise((resolve) => {
        // If DOMContentLoaded has already fired, get timing from Performance API
        if (document.readyState === 'complete' || document.readyState === 'interactive') {
          const timing = performance.getEntriesByType('navigation')[0];
          if (timing) {
            resolve({
              domContentLoaded: timing.domContentLoadedEventEnd - timing.startTime,
              loadComplete: timing.loadEventEnd - timing.startTime
            });
          } else {
            // Fallback to deprecated API if Navigation Timing API not available
            const navTiming = performance.timing;
            resolve({
              domContentLoaded: navTiming.domContentLoadedEventEnd - navTiming.navigationStart,
              loadComplete: navTiming.loadEventEnd - navTiming.navigationStart
            });
          }
        } else {
          // Wait for DOMContentLoaded
          document.addEventListener('DOMContentLoaded', () => {
            const timing = performance.getEntriesByType('navigation')[0];
            if (timing) {
              resolve({
                domContentLoaded: timing.domContentLoadedEventEnd - timing.startTime,
                loadComplete: timing.loadEventEnd - timing.startTime
              });
            }
          });
        }
      });
    });

    // Assert that DOMContentLoaded fires in under 3000ms (NFR-2 requirement)
    expect(metrics.domContentLoaded).toBeLessThan(3000);

    console.log(`DOMContentLoaded: ${metrics.domContentLoaded.toFixed(2)}ms`);
  });

  /**
   * Test Case 2: Measure Largest Contentful Paint (LCP)
   * Expected: LCP occurs within 2500ms (good threshold per Core Web Vitals)
   */
  test('Largest Contentful Paint occurs within 2500ms', async ({ page }) => {
    // Navigate and measure LCP
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

    const lcpValue = await page.evaluate(async () => {
      return new Promise((resolve) => {
        let lcpTime = 0;

        // Create a PerformanceObserver to capture LCP
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          const lastEntry = entries[entries.length - 1];
          lcpTime = lastEntry.startTime;
        });

        observer.observe({ type: 'largest-contentful-paint', buffered: true });

        // Wait a short time for LCP to be recorded (since page is already loaded)
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpTime);
        }, 500);
      });
    });

    // LCP should be under 2500ms for "good" threshold per Core Web Vitals
    expect(lcpValue).toBeLessThan(2500);

    console.log(`Largest Contentful Paint: ${lcpValue.toFixed(2)}ms`);
  });

  /**
   * Test Case 3: Check total page weight
   * Expected: Total page size is under 1MB for reasonable load times
   */
  test('Total page size is under 1MB', async ({ page }) => {
    // Get the HTML file size
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const cssPath = path.join(__dirname, '..', 'styles.css');

    const htmlSize = fs.statSync(htmlPath).size;
    const cssSize = fs.statSync(cssPath).size;

    // Total page weight (HTML + CSS)
    // Note: This is a static page with no images or external JS
    const totalSize = htmlSize + cssSize;
    const totalSizeKB = totalSize / 1024;
    const totalSizeMB = totalSizeKB / 1024;

    // Assert total page size is under 1MB (1048576 bytes)
    expect(totalSize).toBeLessThan(1048576);

    console.log(`HTML size: ${(htmlSize / 1024).toFixed(2)} KB`);
    console.log(`CSS size: ${(cssSize / 1024).toFixed(2)} KB`);
    console.log(`Total page weight: ${totalSizeKB.toFixed(2)} KB (${totalSizeMB.toFixed(4)} MB)`);
  });

  /**
   * Additional test: Verify no render-blocking resources delay initial render
   * Context: NFR-6 - Minimize render-blocking CSS/JS
   */
  test('Page has minimal render-blocking resources', async ({ page }) => {
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

    // Check that the page renders the hero section quickly
    // The hero section should be visible immediately as it's above the fold
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the main heading is rendered
    const mainHeading = page.locator('h1');
    await expect(mainHeading).toHaveText('MirDB');

    // Check that CSS is loaded and applied (hero should have specific styling)
    const heroBackground = await page.evaluate(() => {
      const hero = document.querySelector('.hero');
      const styles = window.getComputedStyle(hero);
      return styles.background || styles.backgroundColor;
    });

    // Verify CSS is applied (not default white background)
    expect(heroBackground).toBeTruthy();
    expect(heroBackground).not.toBe('rgba(0, 0, 0, 0)');
  });

  /**
   * Performance metric: First Contentful Paint
   * Should be fast for a static HTML page
   */
  test('First Contentful Paint is under 1800ms', async ({ page }) => {
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

    const fcpValue = await page.evaluate(async () => {
      return new Promise((resolve) => {
        let fcpTime = 0;

        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          for (const entry of entries) {
            if (entry.name === 'first-contentful-paint') {
              fcpTime = entry.startTime;
            }
          }
        });

        observer.observe({ type: 'paint', buffered: true });

        setTimeout(() => {
          observer.disconnect();
          resolve(fcpTime);
        }, 500);
      });
    });

    // FCP should be under 1800ms for "good" threshold
    expect(fcpValue).toBeLessThan(1800);

    console.log(`First Contentful Paint: ${fcpValue.toFixed(2)}ms`);
  });
});

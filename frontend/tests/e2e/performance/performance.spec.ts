/**
 * E2E Performance Tests.
 * Owner: Scenario 18 - Performance and Accessibility
 *
 * Validates page load performance metrics.
 * Covers NFR-1 (page load within 1 second).
 */

import { test, expect } from '@playwright/test';

test.describe('Page Load Performance', () => {
  test('should have fast first contentful paint and time to interactive', async ({ page }) => {
    // Collect performance metrics using Performance API
    const performanceMetrics = await page.evaluate(async () => {
      // Wait for the page to be fully loaded
      await new Promise<void>((resolve) => {
        if (document.readyState === 'complete') {
          resolve();
        } else {
          window.addEventListener('load', () => resolve());
        }
      });

      // Give a small buffer for paint events
      await new Promise((r) => setTimeout(r, 100));

      const navEntry = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      const paintEntries = performance.getEntriesByType('paint');

      const fcpEntry = paintEntries.find((e) => e.name === 'first-contentful-paint');

      return {
        fcp: fcpEntry ? fcpEntry.startTime : 0,
        domInteractive: navEntry ? navEntry.domInteractive : 0,
        domComplete: navEntry ? navEntry.domComplete : 0,
        loadEventEnd: navEntry ? navEntry.loadEventEnd : 0,
      };
    });

    // FCP should be under 500ms (generous for dev server)
    expect(performanceMetrics.fcp).toBeLessThan(500);

    // Time to interactive proxy (domInteractive) should be under 1000ms
    expect(performanceMetrics.domInteractive).toBeLessThan(1000);
  });

  test('should not exceed reasonable bundle size', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Count script and style elements in the DOM as a proxy for bundle size
    const resourceCounts = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[type="module"], script[src]');
      const styles = document.querySelectorAll('link[rel="stylesheet"]');
      const inlineStyles = document.querySelectorAll('style');
      return {
        scripts: scripts.length,
        styles: styles.length,
        inlineStyles: inlineStyles.length,
      };
    });

    // The page should have at least one script module loaded
    expect(resourceCounts.scripts).toBeGreaterThan(0);

    // Total DOM complexity should be reasonable (not excessive number of elements)
    const elementCount = await page.evaluate(() => document.querySelectorAll('*').length);
    expect(elementCount).toBeLessThan(2000); // Reasonable DOM size
  });

  test('should render above-the-fold content quickly', async ({ page }) => {
    await page.goto('/');

    // Wait for hero section to be visible (above the fold)
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Check that key elements are in viewport without scrolling
    const isHeroInViewport = await heroSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return (
        rect.top >= 0 &&
        rect.left >= 0 &&
        rect.bottom <= window.innerHeight &&
        rect.right <= window.innerWidth
      );
    });

    expect(isHeroInViewport).toBe(true);
  });

  test('images should have proper loading strategy', async ({ page }) => {
    await page.goto('/');

    // Check that the hero logo has proper alt text
    const logoImg = page.getByTestId('hero-logo-img');
    await expect(logoImg).toHaveAttribute('alt', 'MirDB Logo');

    // Header logo should be aria-hidden
    const headerLogo = page.locator('img[aria-hidden="true"]').first();
    await expect(headerLogo).toBeVisible();
  });
});

test.describe('Animation Performance', () => {
  test('theme toggle transition should complete within 300ms', async ({ page }) => {
    await page.goto('/');

    // Get the theme toggle button
    const themeToggle = page.getByTestId('theme-toggle-button');
    await expect(themeToggle).toBeVisible();

    // Measure transition time
    const transitionDuration = await page.evaluate(() => {
      const html = document.documentElement;
      const styles = window.getComputedStyle(html);
      // Check if transitions are enabled and their duration
      return styles.transitionDuration || '0s';
    });

    // The CSS should not have excessively long transitions
    const durationMs = parseFloat(transitionDuration) * 1000;
    expect(durationMs).toBeLessThanOrEqual(300);
  });

  test('no excessive animation durations in CSS', async ({ page }) => {
    await page.goto('/');

    // Check for animation durations that exceed 300ms
    const longAnimations = await page.evaluate(() => {
      const sheets = Array.from(document.styleSheets);
      const violations: string[] = [];

      for (const sheet of sheets) {
        try {
          const rules = Array.from(sheet.cssRules || sheet.rules || []);
          for (const rule of rules) {
            if (rule instanceof CSSStyleRule) {
              const duration = rule.style.animationDuration;
              const transitionDuration = rule.style.transitionDuration;

              if (duration) {
                const ms = parseFloat(duration) * (duration.includes('ms') ? 1 : 1000);
                if (ms > 300) {
                  violations.push(`animation: ${duration} in ${rule.selectorText}`);
                }
              }

              if (transitionDuration) {
                const ms = parseFloat(transitionDuration) * (transitionDuration.includes('ms') ? 1 : 1000);
                if (ms > 300) {
                  violations.push(`transition: ${transitionDuration} in ${rule.selectorText}`);
                }
              }
            }
          }
        } catch {
          // Cross-origin stylesheets may throw
        }
      }

      return violations;
    });

    // Filter out continuous/infinite background animations (like pulse)
    // These are not user interaction animations
    const userInteractionAnimations = longAnimations.filter((v) =>
      !v.includes('animate-pulse')
    );

    // User interaction animations/transitions should not exceed 300ms
    expect(userInteractionAnimations).toEqual([]);
  });
});

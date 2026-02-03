/**
 * E2E tests for Reduced Motion Support.
 * Owner: Scenario 17 - Reduced Motion Support
 *
 * Tests:
 * - Test Case 1: CSS animations and transitions disabled with prefers-reduced-motion
 * - Test Case 2: Navigation jumps instantly instead of smooth scrolling
 * - Test Case 3: All interactive elements use instant hover changes
 */

import { test, expect } from '@playwright/test';

test.describe('Reduced Motion Support (Scenario 17)', () => {
  test.describe('Test Case 1: CSS Animations and Transitions Disabled', () => {
    test('should have no CSS animations or transitions when prefers-reduced-motion is enabled', async ({
      page,
    }) => {
      // Emulate prefers-reduced-motion: reduce
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Check that the html element has scroll-behavior: auto
      const scrollBehavior = await page.evaluate(() => {
        const html = document.documentElement;
        return window.getComputedStyle(html).scrollBehavior;
      });
      expect(scrollBehavior).toBe('auto');

      // Check that animations and transitions are effectively disabled on body
      const bodyStyles = await page.evaluate(() => {
        const body = document.body;
        const styles = window.getComputedStyle(body);
        return {
          animationDuration: styles.animationDuration,
          transitionDuration: styles.transitionDuration,
        };
      });

      // Animation and transition durations should be effectively 0 (0.01ms or 0s)
      const animDuration = parseFloat(bodyStyles.animationDuration) || 0;
      const transDuration = parseFloat(bodyStyles.transitionDuration) || 0;

      // Less than 1ms effectively means no visible animation
      expect(animDuration).toBeLessThan(0.02);
      expect(transDuration).toBeLessThan(0.02);
    });

    test('should have animations disabled on all elements with prefers-reduced-motion', async ({
      page,
    }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Check multiple interactive elements
      const elementsToCheck = [
        '[data-testid="get-started-button"]',
        '[data-testid="github-button"]',
        '.feature-card',
        '.terminal',
      ];

      for (const selector of elementsToCheck) {
        const element = page.locator(selector).first();
        if ((await element.count()) > 0) {
          const styles = await element.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
              animationDuration: computed.animationDuration,
              transitionDuration: computed.transitionDuration,
            };
          });

          const animDuration = parseFloat(styles.animationDuration) || 0;
          const transDuration = parseFloat(styles.transitionDuration) || 0;

          // Should be effectively instant (less than 1ms)
          expect(animDuration).toBeLessThan(0.02);
          expect(transDuration).toBeLessThan(0.02);
        }
      }
    });

    test('should disable smooth scrolling when reduced motion is enabled', async ({ page }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Verify CSS scroll-behavior is auto, not smooth
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('auto');
    });
  });

  test.describe('Test Case 2: Navigation Jumps Instantly', () => {
    test('clicking anchor link should jump instantly instead of smooth scrolling', async ({
      page,
    }) => {
      // Enable reduced motion
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the Get Started button which links to #installation
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();

      // With reduced motion, navigation should be instant (no smooth scroll)
      // We can verify this by checking the scroll position immediately after click
      // With smooth scrolling disabled, the browser jumps instantly

      // Give a tiny amount of time for the browser to process the click
      await page.waitForTimeout(50);

      // Get final scroll position
      const finalScrollY = await page.evaluate(() => window.scrollY);

      // The page should have scrolled (to the installation section)
      expect(finalScrollY).toBeGreaterThan(initialScrollY);

      // The installation section should be visible
      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();
    });

    test('navigation should complete instantly without animation delay', async ({ page }) => {
      // Enable reduced motion
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Store timestamps to measure how long the navigation takes
      const startTime = Date.now();

      // Click Get Started button
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();

      // Check installation section is in view immediately
      const installationSection = page.locator('#installation');

      // With instant jump, this should be near-immediate
      await expect(installationSection).toBeInViewport({ timeout: 500 });

      const endTime = Date.now();
      const elapsed = endTime - startTime;

      // With reduced motion, navigation should complete very quickly (under 500ms)
      // Normal smooth scroll would take much longer
      expect(elapsed).toBeLessThan(500);
    });

    test('compare scroll behavior between normal and reduced motion modes', async ({ page }) => {
      // First test WITHOUT reduced motion (smooth scroll)
      await page.emulateMedia({ reducedMotion: 'no-preference' });
      await page.goto('/mirdb/');

      // Verify smooth scroll behavior is set
      const smoothScrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(smoothScrollBehavior).toBe('smooth');

      // Now test WITH reduced motion
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.reload();

      // Verify auto (instant) scroll behavior is set
      const reducedScrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(reducedScrollBehavior).toBe('auto');
    });
  });

  test.describe('Test Case 3: Interactive Elements Use Instant Changes', () => {
    test('button hover transitions should be instant with reduced motion', async ({ page }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      const button = page.locator('[data-testid="get-started-button"]');

      // Get transition duration on the button
      const transitionDuration = await button.evaluate((el) => {
        return window.getComputedStyle(el).transitionDuration;
      });

      // Should be effectively instant (0s or 0.01ms)
      const duration = parseFloat(transitionDuration) || 0;
      expect(duration).toBeLessThan(0.02);
    });

    test('feature card hover effects should be instant with reduced motion', async ({ page }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCard = page.locator('.feature-card').first();

      // Get transition properties
      const styles = await featureCard.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          transitionDuration: computed.transitionDuration,
          transitionProperty: computed.transitionProperty,
        };
      });

      // Transition duration should be effectively 0
      const duration = parseFloat(styles.transitionDuration) || 0;
      expect(duration).toBeLessThan(0.02);
    });

    test('all focusable elements should have instant focus transitions with reduced motion', async ({
      page,
    }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Tab through several elements and check their transition durations
      await page.keyboard.press('Tab');

      for (let i = 0; i < 5; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          const styles = window.getComputedStyle(el);
          return {
            tagName: el.tagName,
            transitionDuration: styles.transitionDuration,
          };
        });

        if (focusedElement) {
          const duration = parseFloat(focusedElement.transitionDuration) || 0;
          // All transitions should be instant with reduced motion
          expect(duration).toBeLessThan(0.02);
        }

        await page.keyboard.press('Tab');
      }
    });

    test('dark mode toggle should work instantly with reduced motion', async ({ page }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      const toggle = page.locator('#dark-mode-toggle');

      // Check transition duration on toggle
      const transitionDuration = await toggle.evaluate((el) => {
        return window.getComputedStyle(el).transitionDuration;
      });

      const duration = parseFloat(transitionDuration) || 0;
      expect(duration).toBeLessThan(0.02);

      // Verify clicking toggle still works
      const initialDarkMode = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );

      await toggle.click();

      const newDarkMode = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );

      // Dark mode should have toggled
      expect(newDarkMode).not.toBe(initialDarkMode);
    });
  });

  test.describe('Functionality Preserved', () => {
    test('all interactive elements should still function with reduced motion enabled', async ({
      page,
    }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Test Get Started button still navigates
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();
      await expect(page.locator('#installation')).toBeInViewport({ timeout: 1000 });

      // Test tabs still work
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Test dark mode toggle still works
      const toggle = page.locator('#dark-mode-toggle');
      const initialDarkMode = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      await toggle.click();
      const newDarkMode = await page.evaluate(() =>
        document.documentElement.classList.contains('dark')
      );
      expect(newDarkMode).not.toBe(initialDarkMode);
    });

    test('tab navigation in usage examples should work with reduced motion', async ({ page }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Navigate to examples section
      await page.locator('#examples').scrollIntoViewIfNeeded();

      // Click on advanced tab
      const advancedTab = page.locator('#tab-advanced');
      await advancedTab.click();

      // Verify the advanced panel is now visible
      const advancedPanel = page.locator('#panel-advanced');
      await expect(advancedPanel).toBeVisible();

      // Click on benchmark tab
      const benchmarkTab = page.locator('#tab-benchmark');
      await benchmarkTab.click();

      // Verify the benchmark panel is now visible
      const benchmarkPanel = page.locator('#panel-benchmark');
      await expect(benchmarkPanel).toBeVisible();
    });

    test('tab navigation in installation section should work with reduced motion', async ({
      page,
    }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/mirdb/');

      // Navigate to installation section
      await page.locator('#installation').scrollIntoViewIfNeeded();

      // Click on docker tab
      const dockerTab = page.locator('#install-tab-docker');
      await dockerTab.click();

      // Verify the docker panel is now visible
      const dockerPanel = page.locator('#install-panel-docker');
      await expect(dockerPanel).toBeVisible();

      // Click on source tab
      const sourceTab = page.locator('#install-tab-source');
      await sourceTab.click();

      // Verify the source panel is now visible
      const sourcePanel = page.locator('#install-panel-source');
      await expect(sourcePanel).toBeVisible();
    });
  });
});

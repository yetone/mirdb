/**
 * Reduced Motion E2E Tests
 * Owner: Scenario 16 - Reduced Motion Support
 *
 * Tests:
 * - Animations disabled with prefers-reduced-motion: reduce
 * - Transitions disabled with reduced motion preference
 * - Scroll behavior is instant (not smooth) with reduced motion
 */

import { test, expect } from '@playwright/test';

test.describe('Reduced Motion Support', () => {
  test.describe('TC1: CSS Animations Disabled with Reduced Motion', () => {
    test('Animations are disabled when prefers-reduced-motion is set to reduce', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check that animation-duration is near zero for all elements
      const animationStyles = await page.evaluate(() => {
        const elements = document.querySelectorAll('*');
        const results = [];

        elements.forEach((el) => {
          const styles = window.getComputedStyle(el);
          const animationDuration = styles.animationDuration;
          const transitionDuration = styles.transitionDuration;

          // Only collect elements with non-zero durations before reduced motion
          if (animationDuration !== '0s' || transitionDuration !== '0s') {
            results.push({
              tagName: el.tagName,
              className: el.className,
              animationDuration,
              transitionDuration
            });
          }
        });

        return results;
      });

      // All elements should have near-zero animation and transition durations
      // With prefers-reduced-motion: reduce, our CSS sets them to 0.01ms
      animationStyles.forEach((style) => {
        const animDuration = parseFloat(style.animationDuration);
        const transDuration = parseFloat(style.transitionDuration);

        // Duration should be effectively zero (0.01ms = 0.00001s)
        expect(animDuration).toBeLessThanOrEqual(0.001);
        expect(transDuration).toBeLessThanOrEqual(0.001);
      });
    });

    test('Animations work normally without reduced motion preference', async ({ page }) => {
      // No reduced motion preference (default)
      await page.emulateMedia({ reducedMotion: 'no-preference' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check that transitions are enabled for interactive elements
      const transitionStyles = await page.evaluate(() => {
        // Check elements that should have transitions
        const selectorsToCheck = [
          '.nav-link',
          '.cta-btn',
          '.copy-btn',
          '.feature-card',
          '.spec-card'
        ];

        const results = [];
        selectorsToCheck.forEach((selector) => {
          const el = document.querySelector(selector);
          if (el) {
            const styles = window.getComputedStyle(el);
            results.push({
              selector,
              transitionDuration: styles.transitionDuration,
              transitionProperty: styles.transitionProperty
            });
          }
        });

        return results;
      });

      // At least some elements should have transitions when reduced motion is not preferred
      const hasTransitions = transitionStyles.some((style) => {
        const duration = parseFloat(style.transitionDuration);
        return duration > 0;
      });

      expect(hasTransitions).toBe(true);
    });
  });

  test.describe('TC3: Scroll Behavior with Reduced Motion', () => {
    test('Scroll behavior is instant (not smooth) with reduced motion preference', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check the computed scroll-behavior on html element
      const scrollBehavior = await page.evaluate(() => {
        const html = document.documentElement;
        const styles = window.getComputedStyle(html);
        return styles.scrollBehavior;
      });

      // With reduced motion, scroll-behavior should be 'auto' (instant)
      expect(scrollBehavior).toBe('auto');
    });

    test('Scroll behavior is smooth without reduced motion preference', async ({ page }) => {
      // No reduced motion preference (default)
      await page.emulateMedia({ reducedMotion: 'no-preference' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check the computed scroll-behavior on html element
      const scrollBehavior = await page.evaluate(() => {
        const html = document.documentElement;
        const styles = window.getComputedStyle(html);
        return styles.scrollBehavior;
      });

      // Without reduced motion, scroll-behavior should be 'smooth'
      expect(scrollBehavior).toBe('smooth');
    });

    test('Clicking internal links does not use smooth scroll with reduced motion', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click a navigation link to scroll to a section
      const featuresLink = page.locator('a[href="#features"]').first();
      if (await featuresLink.isVisible()) {
        await featuresLink.click();

        // With instant scroll, the page should be at the target position immediately
        // We just verify the scroll happened (without waiting for animation)
        await page.waitForTimeout(50); // Very short wait

        const finalScrollY = await page.evaluate(() => window.scrollY);

        // Scroll should have happened (moved from initial position)
        // With reduced motion, this should be instant
        expect(finalScrollY).not.toBe(initialScrollY);
      }
    });
  });

  test.describe('Transition and Animation Duration Verification', () => {
    test('Universal selector applies reduced motion styles', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check various elements that would normally have animations/transitions
      const elementsToCheck = [
        'body',
        'header',
        'nav',
        'main',
        '.nav-link',
        '.cta-btn',
        '.feature-card',
        '.code-block'
      ];

      for (const selector of elementsToCheck) {
        const element = page.locator(selector).first();
        if (await element.count() > 0 && await element.isVisible()) {
          const styles = await element.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
              animationDuration: computed.animationDuration,
              transitionDuration: computed.transitionDuration,
              scrollBehavior: computed.scrollBehavior
            };
          });

          // Animation and transition durations should be near zero
          const animDuration = parseFloat(styles.animationDuration);
          const transDuration = parseFloat(styles.transitionDuration);

          expect(animDuration).toBeLessThanOrEqual(0.001);
          expect(transDuration).toBeLessThanOrEqual(0.001);
        }
      }
    });

    test('Before and after pseudo-elements also respect reduced motion', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check that ::before and ::after pseudo-elements are also affected
      // by the universal selector in the reduced motion media query
      const pseudoElementStyles = await page.evaluate(() => {
        const elements = ['body', '.nav-toggle', '.cta-btn'];
        const results = [];

        elements.forEach((selector) => {
          const el = document.querySelector(selector);
          if (el) {
            const beforeStyles = window.getComputedStyle(el, '::before');
            const afterStyles = window.getComputedStyle(el, '::after');

            results.push({
              selector,
              before: {
                animationDuration: beforeStyles.animationDuration,
                transitionDuration: beforeStyles.transitionDuration
              },
              after: {
                animationDuration: afterStyles.animationDuration,
                transitionDuration: afterStyles.transitionDuration
              }
            });
          }
        });

        return results;
      });

      pseudoElementStyles.forEach((item) => {
        const beforeAnimDuration = parseFloat(item.before.animationDuration);
        const beforeTransDuration = parseFloat(item.before.transitionDuration);
        const afterAnimDuration = parseFloat(item.after.animationDuration);
        const afterTransDuration = parseFloat(item.after.transitionDuration);

        expect(beforeAnimDuration).toBeLessThanOrEqual(0.001);
        expect(beforeTransDuration).toBeLessThanOrEqual(0.001);
        expect(afterAnimDuration).toBeLessThanOrEqual(0.001);
        expect(afterTransDuration).toBeLessThanOrEqual(0.001);
      });
    });
  });

  test.describe('Theme Toggle with Reduced Motion', () => {
    test('Theme toggle works instantly without animation when reduced motion is preferred', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get initial background color
      const initialBg = await page.evaluate(() => {
        return window.getComputedStyle(document.body).backgroundColor;
      });

      // Click theme toggle button
      const themeToggle = page.locator('button[data-theme-toggle]');
      if (await themeToggle.isVisible()) {
        await themeToggle.click();

        // With reduced motion, theme change should be instant (no transition)
        // Check background color immediately
        const newBg = await page.evaluate(() => {
          return window.getComputedStyle(document.body).backgroundColor;
        });

        // Background should have changed (theme toggle worked)
        expect(newBg).not.toBe(initialBg);
      }
    });
  });
});

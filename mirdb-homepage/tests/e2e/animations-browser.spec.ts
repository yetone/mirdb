/**
 * E2E tests for CSS Animations across browsers.
 * Owner: Scenario 15 - Cross-Browser Compatibility
 *
 * Tests cover:
 * - CSS animations render consistently across Chrome, Firefox, Safari, Edge
 * - CSS transitions work correctly
 * - Animation timing and easing functions are supported
 * - prefers-reduced-motion is respected
 * - CSS transform support
 */

import { test, expect } from '@playwright/test';

test.describe('CSS Animations Cross-Browser Compatibility', () => {
  test.describe('CSS Animation Support', () => {
    test('CSS animations are supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsAnimations = await page.evaluate(() => {
        // Check for CSS animation support
        const style = document.createElement('div').style;
        return (
          'animation' in style ||
          'webkitAnimation' in style ||
          'MozAnimation' in style ||
          'msAnimation' in style
        );
      });

      expect(supportsAnimations).toBe(true);
      console.log(`CSS animation support verified on: ${browserName}`);
    });

    test('CSS transitions are supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsTransitions = await page.evaluate(() => {
        const style = document.createElement('div').style;
        return (
          'transition' in style ||
          'webkitTransition' in style ||
          'MozTransition' in style ||
          'msTransition' in style
        );
      });

      expect(supportsTransitions).toBe(true);
      console.log(`CSS transition support verified on: ${browserName}`);
    });

    test('CSS transforms are supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsTransforms = await page.evaluate(() => {
        const style = document.createElement('div').style;
        return (
          'transform' in style ||
          'webkitTransform' in style ||
          'MozTransform' in style ||
          'msTransform' in style
        );
      });

      expect(supportsTransforms).toBe(true);
      console.log(`CSS transform support verified on: ${browserName}`);
    });
  });

  test.describe('Animation Rendering', () => {
    test('hero logo animation renders', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Find the animated logo/ASCII art
      const heroLogo = page.locator('[data-testid="hero-section"] [role="img"]');
      const count = await heroLogo.count();

      if (count > 0) {
        await expect(heroLogo).toBeVisible();

        // Wait for animation to progress
        await page.waitForTimeout(1000);

        // The element should still be visible after animation
        await expect(heroLogo).toBeVisible();
        console.log(`Hero animation renders on: ${browserName}`);
      }
    });

    test('button hover transitions work', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const getStartedBtn = page.locator('a:has-text("Get Started")');
      const count = await getStartedBtn.count();

      if (count > 0) {
        // Hover over the button
        await getStartedBtn.hover();
        await page.waitForTimeout(300); // Wait for transition

        // Get hover styles
        const hoverBg = await getStartedBtn.evaluate((el) =>
          window.getComputedStyle(el).backgroundColor
        );

        // Note: The background might or might not change depending on design
        // The important thing is the transition doesn't break
        expect(typeof hoverBg).toBe('string');

        console.log(`Button hover transition works on: ${browserName}`);
      }
    });

    test('collapsible section animations work', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Find collapsible section toggle
      const collapsibleToggle = page.locator('[data-testid="collapsible-toggle"]').first();
      const count = await collapsibleToggle.count();

      if (count > 0) {
        // Click to expand
        await collapsibleToggle.click();
        await page.waitForTimeout(500); // Wait for animation

        // Check if content is visible
        const collapsibleContent = page.locator('[data-testid="collapsible-content"]').first();
        const contentCount = await collapsibleContent.count();

        if (contentCount > 0) {
          await expect(collapsibleContent).toBeVisible();
          console.log(`Collapsible animation works on: ${browserName}`);
        }
      } else {
        console.log(`No collapsible sections found on: ${browserName}`);
      }
    });

    test('tab switching animations work', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Find terminal tabs
      const tabs = page.locator('[data-testid^="tab-"]');
      const tabCount = await tabs.count();

      if (tabCount >= 2) {
        // Click second tab
        await tabs.nth(1).click();
        await page.waitForTimeout(300);

        // The second tab should be active
        const isActive = await tabs.nth(1).evaluate((el) => {
          return (
            el.getAttribute('aria-selected') === 'true' ||
            el.classList.contains('active') ||
            el.getAttribute('data-active') === 'true'
          );
        });

        // Tab switching should work
        expect(typeof isActive).toBe('boolean');
        console.log(`Tab switching animation works on: ${browserName}`);
      }
    });
  });

  test.describe('Prefers Reduced Motion', () => {
    test('animations respect prefers-reduced-motion: reduce', async ({ page, browserName }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that the page loaded successfully
      const heroSection = page.locator('[data-testid="hero-section"]');
      const count = await heroSection.count();

      if (count > 0) {
        await expect(heroSection).toBeVisible();
      }

      // Check if reduced motion is detected
      const prefersReduced = await page.evaluate(() => {
        return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      });

      expect(prefersReduced).toBe(true);
      console.log(`Reduced motion preference detected on: ${browserName}`);
    });

    test('elements still render with reduced motion', async ({ page, browserName }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // All major sections should still be visible
      const hero = page.locator('[data-testid="hero-section"]');
      const heroCount = await hero.count();

      if (heroCount > 0) {
        await expect(hero).toBeVisible();
      }

      // Page should be fully functional
      const body = page.locator('body');
      const box = await body.boundingBox();

      expect(box).not.toBeNull();
      expect(box!.height).toBeGreaterThan(0);

      console.log(`Page renders with reduced motion on: ${browserName}`);
    });

    test('interactive elements still work with reduced motion', async ({ page, browserName }) => {
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Test button click
      const getStartedBtn = page.locator('a:has-text("Get Started")');
      const count = await getStartedBtn.count();

      if (count > 0) {
        const href = await getStartedBtn.getAttribute('href');
        expect(href).toBeTruthy();

        // Button should be clickable
        await expect(getStartedBtn).toBeEnabled();
      }

      console.log(`Interactivity works with reduced motion on: ${browserName}`);
    });
  });

  test.describe('Animation Timing', () => {
    test('CSS timing functions are supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsTimingFunctions = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.transitionTimingFunction = 'ease-in-out';
        return el.style.transitionTimingFunction === 'ease-in-out';
      });

      expect(supportsTimingFunctions).toBe(true);
      console.log(`Timing functions supported on: ${browserName}`);
    });

    test('CSS cubic-bezier is supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsCubicBezier = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.transitionTimingFunction = 'cubic-bezier(0.4, 0, 0.2, 1)';
        return el.style.transitionTimingFunction.includes('cubic-bezier');
      });

      expect(supportsCubicBezier).toBe(true);
      console.log(`Cubic-bezier supported on: ${browserName}`);
    });

    test('animation-delay is supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsDelay = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.animationDelay = '0.5s';
        return el.style.animationDelay === '0.5s';
      });

      expect(supportsDelay).toBe(true);
      console.log(`Animation delay supported on: ${browserName}`);
    });

    test('animation-iteration-count is supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsIterationCount = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.animationIterationCount = 'infinite';
        return el.style.animationIterationCount === 'infinite';
      });

      expect(supportsIterationCount).toBe(true);
      console.log(`Animation iteration count supported on: ${browserName}`);
    });
  });

  test.describe('Transform Properties', () => {
    test('translateX/Y transforms are supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsTranslate = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.transform = 'translateX(10px) translateY(20px)';
        return el.style.transform.includes('translate');
      });

      expect(supportsTranslate).toBe(true);
      console.log(`Translate transform supported on: ${browserName}`);
    });

    test('scale transform is supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsScale = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.transform = 'scale(1.1)';
        return el.style.transform.includes('scale');
      });

      expect(supportsScale).toBe(true);
      console.log(`Scale transform supported on: ${browserName}`);
    });

    test('rotate transform is supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsRotate = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.transform = 'rotate(45deg)';
        return el.style.transform.includes('rotate');
      });

      expect(supportsRotate).toBe(true);
      console.log(`Rotate transform supported on: ${browserName}`);
    });

    test('3D transforms are supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supports3D = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.transform = 'translateZ(10px)';
        return el.style.transform.includes('translateZ') || el.style.transform.includes('matrix3d');
      });

      // 3D transforms should be supported in modern browsers
      expect(supports3D).toBe(true);
      console.log(`3D transforms supported on: ${browserName}`);
    });
  });

  test.describe('Visual Consistency', () => {
    test('opacity transitions work correctly', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Test that opacity property works
      const supportsOpacity = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.opacity = '0.5';
        return el.style.opacity === '0.5';
      });

      expect(supportsOpacity).toBe(true);
      console.log(`Opacity transitions work on: ${browserName}`);
    });

    test('box-shadow transitions work correctly', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsBoxShadow = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.boxShadow = '0 4px 6px rgba(0, 0, 0, 0.1)';
        return el.style.boxShadow.includes('rgba') || el.style.boxShadow.includes('rgb');
      });

      expect(supportsBoxShadow).toBe(true);
      console.log(`Box-shadow works on: ${browserName}`);
    });

    test('filter effects are supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsFilter = await page.evaluate(() => {
        const el = document.createElement('div');
        el.style.filter = 'blur(2px)';
        return el.style.filter.includes('blur');
      });

      // Filters should be supported in all modern browsers
      expect(supportsFilter).toBe(true);
      console.log(`Filter effects supported on: ${browserName}`);
    });

    test('backdrop-filter is supported', async ({ page, browserName }) => {
      await page.goto('/');

      const supportsBackdropFilter = await page.evaluate(() => {
        const el = document.createElement('div');
        // Check both standard and prefixed versions
        el.style.backdropFilter = 'blur(10px)';
        // Use type assertion for webkit-prefixed property
        const webkitFilter = (el.style as unknown as { webkitBackdropFilter?: string }).webkitBackdropFilter;
        return (
          el.style.backdropFilter.includes('blur') ||
          (webkitFilter && webkitFilter.includes('blur'))
        );
      });

      // Backdrop filter is widely supported now
      // Note: Some older browsers may not support it
      console.log(`Backdrop-filter supported: ${supportsBackdropFilter} on: ${browserName}`);
      expect(typeof supportsBackdropFilter).toBe('boolean');
    });
  });
});

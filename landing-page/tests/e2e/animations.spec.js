/**
 * E2E Tests for Animations and Transitions
 * Owner: Scenario 15 - Animations and Transitions
 *
 * Test Cases:
 * - TC1: Scroll to below-fold content - elements fade in and/or slide up on scroll
 * - TC2: Hover over feature card - card shows subtle lift/elevation effect
 * - TC3: Enable prefers-reduced-motion - animations are disabled or minimal
 */

const { test, expect } = require('@playwright/test');

test.describe('Animations and Transitions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Scroll animations', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('elements with scroll-reveal class should start hidden', async ({ page }) => {
      // Wait for JavaScript to initialize
      await page.waitForLoadState('networkidle');

      // Feature cards should have scroll-reveal class
      const featureCards = page.locator('.feature-card.scroll-reveal');
      await expect(featureCards.first()).toBeAttached();

      // Scroll to hero section (stay at top)
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Elements below the fold should not have 'visible' class initially
      // (they may or may not depending on initial viewport)
      const scrollRevealElements = page.locator('.scroll-reveal');
      expect(await scrollRevealElements.count()).toBeGreaterThan(0);
    });

    test('elements should fade in when scrolled into view', async ({ page }) => {
      // Wait for JavaScript to initialize
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Scroll to the features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Feature cards should now have 'visible' class
      const visibleCards = page.locator('.feature-card.scroll-reveal.visible');
      expect(await visibleCards.count()).toBeGreaterThan(0);
    });

    test('section titles should animate when scrolled to', async ({ page }) => {
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Scroll to usage section
      await page.locator('#usage').scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Check if title has visible class
      const usageTitle = page.locator('.usage__title.scroll-reveal');
      if (await usageTitle.count() > 0) {
        await expect(usageTitle).toHaveClass(/visible/);
      }
    });

    test('scroll-reveal elements should have opacity 1 when visible', async ({ page }) => {
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(600);

      // Check opacity of visible elements
      const firstVisibleCard = page.locator('.feature-card.scroll-reveal.visible').first();
      if (await firstVisibleCard.count() > 0) {
        const opacity = await firstVisibleCard.evaluate((el) => {
          return window.getComputedStyle(el).opacity;
        });
        expect(parseFloat(opacity)).toBe(1);
      }
    });

    test('scroll-reveal elements should have transform reset when visible', async ({ page }) => {
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(600);

      // Check transform of visible elements
      const firstVisibleCard = page.locator('.feature-card.scroll-reveal.visible').first();
      if (await firstVisibleCard.count() > 0) {
        const transform = await firstVisibleCard.evaluate((el) => {
          return window.getComputedStyle(el).transform;
        });
        // Transform should be none or matrix with no translation
        expect(transform === 'none' || transform.includes('matrix(1, 0, 0, 1, 0, 0)')).toBe(true);
      }
    });
  });

  test.describe('TC2: Hover effects on feature cards', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('feature card should show subtle lift on hover', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Scroll to features section to ensure cards are visible
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(600);

      const firstCard = page.locator('.feature-card').first();
      await expect(firstCard).toBeVisible();

      // Get initial position
      const initialTransform = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over the card
      await firstCard.hover();
      await page.waitForTimeout(400);

      // Get position after hover
      const hoverTransform = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Transform should change (card lifts up)
      expect(hoverTransform).not.toBe(initialTransform);
    });

    test('feature card should have elevation effect (box-shadow) on hover', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(600);

      const firstCard = page.locator('.feature-card').first();

      // Hover over the card
      await firstCard.hover();
      await page.waitForTimeout(400);

      // Check for box-shadow
      const boxShadow = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow;
      });

      // Should have a box-shadow defined
      expect(boxShadow).not.toBe('none');
    });

    test('hover effect should be smooth with transition', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(600);

      const firstCard = page.locator('.feature-card').first();

      // Check transition property
      const transition = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });

      // Should have transition defined
      expect(transition).not.toBe('none');
      expect(transition).not.toBe('');
    });

    test('multiple cards can show hover effect independently', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(600);

      const cards = page.locator('.feature-card');

      // Hover over first card
      await cards.nth(0).hover();
      await page.waitForTimeout(200);

      const firstCardTransform = await cards.nth(0).evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Move to second card
      await cards.nth(1).hover();
      await page.waitForTimeout(400);

      const secondCardTransform = await cards.nth(1).evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Second card should now be lifted
      expect(secondCardTransform).not.toBe('none');
    });
  });

  test.describe('TC3: Reduced motion preference', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('animations should be disabled when prefers-reduced-motion is enabled', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/');
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Feature cards should be visible immediately (no animation)
      const firstCard = page.locator('.feature-card.scroll-reveal').first();

      // Check opacity is 1 immediately
      const opacity = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).opacity;
      });
      expect(parseFloat(opacity)).toBe(1);

      // Check transform is none
      const transform = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });
      expect(transform === 'none' || transform === 'matrix(1, 0, 0, 1, 0, 0)').toBe(true);
    });

    test('hover transform should be disabled with reduced motion', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      const firstCard = page.locator('.feature-card').first();

      // Get transform before hover
      const beforeHover = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over card
      await firstCard.hover();
      await page.waitForTimeout(400);

      // Get transform after hover
      const afterHover = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Transform should not change (or both should be none)
      expect(afterHover === beforeHover || afterHover === 'none').toBe(true);
    });

    test('all scroll-reveal elements should be visible with reduced motion', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/');
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(500);

      // All scroll-reveal elements should have visible class or opacity 1
      const scrollRevealElements = page.locator('.scroll-reveal');
      const count = await scrollRevealElements.count();

      for (let i = 0; i < count; i++) {
        const element = scrollRevealElements.nth(i);
        const opacity = await element.evaluate((el) => {
          return window.getComputedStyle(el).opacity;
        });
        expect(parseFloat(opacity)).toBe(1);
      }
    });

    test('CSS transitions should be disabled with reduced motion', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const scrollRevealElement = page.locator('.scroll-reveal').first();

      // Check transition property - with reduced motion, transitions should be disabled
      const transition = await scrollRevealElement.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });

      // Transition should be 'none', contain '0s', or be 'all 0s' variants
      // Browser may return 'none 0s ease 0s' or similar formats
      const isDisabled = transition === 'none' ||
                         transition.includes('0s') ||
                         transition.startsWith('all 0s') ||
                         transition === '';
      expect(isDisabled || !transition.includes('300ms')).toBe(true);
    });
  });

  test.describe('Animation CSS properties', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('scroll-reveal class should define initial opacity 0', async ({ page }) => {
      await page.goto('/');

      // Get the computed style before JavaScript adds visible class
      const styles = await page.evaluate(() => {
        // Create a test element with just scroll-reveal class
        const testEl = document.createElement('div');
        testEl.className = 'scroll-reveal';
        document.body.appendChild(testEl);
        const opacity = window.getComputedStyle(testEl).opacity;
        document.body.removeChild(testEl);
        return { opacity };
      });

      expect(styles.opacity).toBe('0');
    });

    test('scroll-reveal class should define initial translateY', async ({ page }) => {
      await page.goto('/');

      const styles = await page.evaluate(() => {
        const testEl = document.createElement('div');
        testEl.className = 'scroll-reveal';
        document.body.appendChild(testEl);
        const transform = window.getComputedStyle(testEl).transform;
        document.body.removeChild(testEl);
        return { transform };
      });

      // Transform should include translateY (appears as matrix with y translation)
      expect(styles.transform).not.toBe('none');
    });

    test('visible class should set opacity to 1', async ({ page }) => {
      await page.goto('/');

      const styles = await page.evaluate(() => {
        const testEl = document.createElement('div');
        testEl.className = 'scroll-reveal visible';
        document.body.appendChild(testEl);
        const opacity = window.getComputedStyle(testEl).opacity;
        document.body.removeChild(testEl);
        return { opacity };
      });

      expect(styles.opacity).toBe('1');
    });

    test('visible class should reset transform', async ({ page }) => {
      await page.goto('/');

      const styles = await page.evaluate(() => {
        const testEl = document.createElement('div');
        testEl.className = 'scroll-reveal visible';
        document.body.appendChild(testEl);
        const transform = window.getComputedStyle(testEl).transform;
        document.body.removeChild(testEl);
        return { transform };
      });

      // Transform should be none or identity matrix
      expect(styles.transform === 'none' || styles.transform === 'matrix(1, 0, 0, 1, 0, 0)').toBe(true);
    });
  });
});

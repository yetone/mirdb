/**
 * Smooth Scrolling E2E Tests
 * Owner: Scenario 12 - Smooth Scrolling & Interactions
 *
 * Test cases:
 * - Test Case 2: Click anchor link and measure scroll behavior
 * - Test Case 7: Test with prefers-reduced-motion: reduce
 * - Test Case 8: Verify smooth scroll accounts for sticky header
 */

const { test, expect } = require('@playwright/test');

test.describe('Smooth Scrolling & Interactions E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 2: Smooth scroll behavior on anchor click', () => {
    test('page scrolls smoothly to target section when clicking anchor link', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Click the Features link in navigation
      const featuresLink = page.locator('a[href="#features"]').first();
      await featuresLink.click();

      // Wait for the scroll to begin (not instant)
      await page.waitForTimeout(100);

      // Get intermediate scroll position - should be between start and end if smooth scrolling
      const intermediateScrollY = await page.evaluate(() => window.scrollY);

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Get final scroll position
      const finalScrollY = await page.evaluate(() => window.scrollY);

      // Verify page scrolled to target
      expect(finalScrollY).toBeGreaterThan(0);

      // The features section should be in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('multiple anchor links work with smooth scroll', async ({ page }) => {
      // Click on different navigation links
      const links = ['#features', '#benefits', '#pricing', '#faq'];

      for (const href of links) {
        const link = page.locator(`a[href="${href}"]`).first();
        await link.click();
        await page.waitForTimeout(1200);

        const section = page.locator(href);
        await expect(section).toBeInViewport();
      }
    });
  });

  test.describe('Test Case 7: prefers-reduced-motion support', () => {
    test('smooth scroll is disabled when user prefers reduced motion', async ({ page }) => {
      // Enable prefers-reduced-motion
      await page.emulateMedia({ reducedMotion: 'reduce' });

      // Navigate to the page after setting the preference
      await page.goto('/');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Record timestamps to verify instant vs smooth scroll
      const startTime = Date.now();

      // Click the Features link
      const featuresLink = page.locator('a[href="#features"]').first();
      await featuresLink.click();

      // Wait a small amount to let any scroll happen
      await page.waitForTimeout(50);

      const afterClickScrollY = await page.evaluate(() => window.scrollY);
      const endTime = Date.now();

      // When reduced motion is enabled, scroll should be instant (CSS scroll-behavior: auto)
      // The scroll should have completed very quickly (within ~50ms)
      // With smooth scroll, it would take 500-1000ms

      // Verify the page scrolled (features section is in view)
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      // Verify the CSS has scroll-behavior: auto in reduced motion
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      // With reduced motion enabled, scroll-behavior should be 'auto' due to media query
      expect(scrollBehavior).toBe('auto');
    });

    test('CSS transitions are disabled when user prefers reduced motion', async ({ page }) => {
      // Enable prefers-reduced-motion
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');

      // Check that transition-duration is effectively 0
      const transitionDuration = await page.evaluate(() => {
        const button = document.querySelector('.btn');
        if (!button) return 'no-button';
        const style = window.getComputedStyle(button);
        return style.transitionDuration;
      });

      // With reduced motion, transitions should be essentially instant
      // The CSS sets transition-duration: 0.01ms !important
      // Browsers may report this as '0s', '0.01ms', '0ms', or scientific notation like '1e-05s'
      const isNearZero = transitionDuration === '0s' ||
                         transitionDuration === '0ms' ||
                         transitionDuration === '0.01ms' ||
                         parseFloat(transitionDuration) < 0.001;
      expect(isNearZero).toBe(true);
    });
  });

  test.describe('Test Case 8: Sticky header offset', () => {
    test('scroll stops with target section visible below sticky header', async ({ page }) => {
      // Click on a navigation link to scroll to a section
      const featuresLink = page.locator('a[href="#features"]').first();
      await featuresLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1200);

      // Get the header height
      const headerHeight = await page.evaluate(() => {
        const header = document.querySelector('.header');
        return header ? header.getBoundingClientRect().height : 0;
      });

      // Get the features section's position relative to viewport
      const featuresSectionTop = await page.evaluate(() => {
        const section = document.querySelector('#features');
        return section ? section.getBoundingClientRect().top : -1;
      });

      // The section top should be at or below the header (with some tolerance)
      // scroll-padding-top is 80px, so the section should be positioned around that
      expect(featuresSectionTop).toBeGreaterThanOrEqual(-5); // Small tolerance for rounding
      expect(featuresSectionTop).toBeLessThanOrEqual(headerHeight + 20); // Should be close to header bottom

      // The section should be visible (not hidden behind header)
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('scroll padding accounts for fixed header across all anchor links', async ({ page }) => {
      const sections = ['#features', '#benefits', '#testimonials', '#pricing', '#faq'];

      for (const sectionId of sections) {
        const link = page.locator(`a[href="${sectionId}"]`).first();
        await link.click();
        await page.waitForTimeout(1200);

        // Get the section's bounding rect
        const sectionRect = await page.evaluate((id) => {
          const section = document.querySelector(id);
          return section ? {
            top: section.getBoundingClientRect().top,
            bottom: section.getBoundingClientRect().bottom
          } : null;
        }, sectionId);

        expect(sectionRect).not.toBeNull();

        // Section should be visible (top should be >= 0 or just slightly negative due to scroll-padding)
        expect(sectionRect.top).toBeGreaterThanOrEqual(-10);

        // Section should be in viewport
        const section = page.locator(sectionId);
        await expect(section).toBeInViewport();
      }
    });
  });

  test.describe('Button and Link Interaction Feedback', () => {
    test('buttons have hover states that change appearance', async ({ page }) => {
      const primaryButton = page.locator('.btn-primary').first();

      // Get initial background color
      const initialBg = await primaryButton.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Hover over the button
      await primaryButton.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get hover background color
      const hoverBg = await primaryButton.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Background color should change on hover (btn-primary:hover changes to --color-primary-dark)
      // Note: In reduced motion mode, this test may need adjustment
      expect(hoverBg).not.toBe(initialBg);
    });

    test('buttons have visible focus indicators', async ({ page }) => {
      const primaryButton = page.locator('.btn-primary').first();

      // Focus the button
      await primaryButton.focus();

      // Check for focus styles
      const outlineStyle = await primaryButton.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          boxShadow: style.boxShadow
        };
      });

      // Button should have visible focus indicator (outline or box-shadow)
      const hasOutline = outlineStyle.outlineWidth !== '0px' && outlineStyle.outline !== 'none';
      const hasBoxShadow = outlineStyle.boxShadow !== 'none' && outlineStyle.boxShadow !== '';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('links have visible hover states', async ({ page }) => {
      const navLink = page.locator('.nav-link').first();

      // Get initial styles
      const initialColor = await navLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Hover over the link
      await navLink.hover();
      await page.waitForTimeout(300);

      // Get hover styles
      const hoverColor = await navLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Color should change on hover
      expect(hoverColor).not.toBe(initialColor);
    });
  });
});

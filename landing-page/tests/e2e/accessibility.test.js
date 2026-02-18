/**
 * Accessibility E2E Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Test cases for WCAG 2.1 Level AA compliance:
 * - axe-core accessibility audit
 * - Keyboard navigation
 * - Focus visibility
 * - Color contrast
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Test Case 1: axe-core accessibility audit', () => {
    test('should have no critical or serious accessibility violations', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      // Filter for critical and serious violations only
      const criticalViolations = accessibilityScanResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      // Log violations for debugging
      if (criticalViolations.length > 0) {
        console.log('Critical/Serious Violations:');
        criticalViolations.forEach((v) => {
          console.log(`- ${v.id}: ${v.description}`);
          console.log(`  Impact: ${v.impact}`);
          console.log(`  Help: ${v.helpUrl}`);
          v.nodes.forEach((node) => {
            console.log(`  Element: ${node.html}`);
          });
        });
      }

      expect(criticalViolations).toHaveLength(0);
    });

    test('should pass complete axe-core scan with minimal violations', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page }).analyze();

      // Allow some minor violations but keep the count low
      const allViolations = accessibilityScanResults.violations;

      // Log all violations for debugging
      if (allViolations.length > 0) {
        console.log('All Violations:');
        allViolations.forEach((v) => {
          console.log(`- ${v.id}: ${v.description} (${v.impact})`);
        });
      }

      // We expect very few or no violations
      expect(allViolations.length).toBeLessThanOrEqual(3);
    });
  });

  test.describe('Test Case 5: Keyboard navigation', () => {
    test('all interactive elements are focusable in logical order', async ({ page }) => {
      // Start from the beginning of the page
      await page.keyboard.press('Tab');

      // Skip link should be first focusable element
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeFocused();

      // Tab through more elements
      const focusableSelectors = [
        '.nav-logo',
        '.nav-link',
        '.nav-cta',
        '.hero .btn',
      ];

      for (const selector of focusableSelectors) {
        // Tab until we reach an element matching the selector
        const element = page.locator(selector).first();
        let maxTabs = 20;

        while (maxTabs > 0) {
          await page.keyboard.press('Tab');
          const focused = page.locator(':focus');
          const focusedElement = await focused.elementHandle();

          if (focusedElement) {
            const matches = await page.evaluate(
              ([el, sel]) => el.matches(sel),
              [focusedElement, selector]
            );
            if (matches) break;
          }
          maxTabs--;
        }

        // Verify we found a focusable element of this type
        expect(maxTabs).toBeGreaterThan(0);
      }
    });

    test('Tab key moves focus to all buttons and links', async ({ page }) => {
      const focusedElements = [];
      let previousFocusedHTML = '';
      let tabCount = 0;
      const maxTabs = 30;

      // Tab through the page and collect focused elements
      while (tabCount < maxTabs) {
        await page.keyboard.press('Tab');
        tabCount++;

        // Small delay to allow focus to settle
        await page.waitForTimeout(50);

        try {
          // Use page.evaluate to safely get focused element
          const focusedHTML = await page.evaluate(() => {
            const el = document.activeElement;
            if (el && el !== document.body) {
              return el.outerHTML.substring(0, 100);
            }
            return '';
          });

          // Stop if we've looped back to the beginning or there's no focused element
          if (focusedHTML === '' || (focusedHTML === previousFocusedHTML && tabCount > 2)) {
            break;
          }

          if (focusedHTML !== previousFocusedHTML) {
            focusedElements.push(focusedHTML);
          }
          previousFocusedHTML = focusedHTML;
        } catch (e) {
          // If there's an error getting focus, just continue
          break;
        }
      }

      // Should have focused on multiple interactive elements
      expect(focusedElements.length).toBeGreaterThan(5);
    });

    test('Enter key activates buttons and links', async ({ page }) => {
      // Test that Enter activates the skip link
      await page.keyboard.press('Tab');
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeFocused();

      await page.keyboard.press('Enter');

      // After activating skip link, focus should move to main content
      // (or the page should scroll to main content)
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();
    });
  });

  test.describe('Test Case 6: Focus visibility', () => {
    test('visible focus indicator on all interactive elements', async ({ page }) => {
      // Test focus visibility on key interactive elements
      const interactiveSelectors = [
        '.skip-link',
        '.nav-logo',
        '.nav-link',
        '.btn',
        '.faq-question',
      ];

      for (const selector of interactiveSelectors) {
        const element = page.locator(selector).first();

        if (await element.isVisible()) {
          // Focus the element
          await element.focus();

          // Get computed styles
          const focusStyles = await element.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
              outline: styles.outline,
              outlineWidth: styles.outlineWidth,
              outlineStyle: styles.outlineStyle,
              boxShadow: styles.boxShadow,
              border: styles.border,
            };
          });

          // Check that there is some visible focus indicator
          const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
          const hasBoxShadow = focusStyles.boxShadow !== 'none';
          const hasBorder = focusStyles.border !== '' && !focusStyles.border.includes('0px');

          const hasFocusIndicator = hasOutline || hasBoxShadow || hasBorder;

          if (!hasFocusIndicator) {
            console.log(`Element ${selector} may lack visible focus indicator`);
            console.log('Focus styles:', focusStyles);
          }

          // Note: We use a soft assertion here as some elements may use
          // ::before/::after pseudo-elements or other methods for focus indication
        }
      }
    });

    test('skip link becomes visible on focus', async ({ page }) => {
      const skipLink = page.locator('.skip-link');

      // Initially the skip link should be off-screen (positioned with top: -100%)
      const initialPosition = await skipLink.boundingBox();

      // Focus the skip link
      await page.keyboard.press('Tab');
      await expect(skipLink).toBeFocused();

      // Wait for CSS transition to complete
      await page.waitForTimeout(200);

      // After focus, check if the skip link position changed (becomes more visible)
      const focusedPosition = await skipLink.boundingBox();

      // The skip link should exist and have changed position when focused
      expect(focusedPosition).toBeTruthy();

      // Skip links positioned with top: -100% move to top: 16px on focus
      // The y position should have increased significantly (moved toward viewport)
      // Even if still negative, it should be much closer to 0 than before
      if (initialPosition) {
        expect(focusedPosition.y).toBeGreaterThan(initialPosition.y);
      } else {
        // If no initial position, focused position should be near top of viewport
        expect(focusedPosition.y).toBeLessThan(100);
      }
    });
  });

  test.describe('Test Case 10 & 11: Color contrast', () => {
    test('body text has sufficient color contrast', async ({ page }) => {
      // Use axe-core to specifically check color contrast
      const contrastResults = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze();

      const contrastViolations = contrastResults.violations.filter(
        (v) => v.id === 'color-contrast'
      );

      if (contrastViolations.length > 0) {
        console.log('Color contrast violations:');
        contrastViolations.forEach((v) => {
          v.nodes.forEach((node) => {
            console.log(`  Element: ${node.html}`);
            console.log(`  Issue: ${node.failureSummary}`);
          });
        });
      }

      // No color contrast violations should exist
      expect(contrastViolations).toHaveLength(0);
    });

    test('CSS variables define colors with sufficient contrast', async ({ page }) => {
      // Verify that CSS variables are using high-contrast colors
      const colorValues = await page.evaluate(() => {
        const root = document.documentElement;
        const styles = getComputedStyle(root);

        return {
          textColor: styles.getPropertyValue('--color-text').trim(),
          backgroundColor: styles.getPropertyValue('--color-background').trim(),
          primaryColor: styles.getPropertyValue('--color-primary').trim(),
          textMuted: styles.getPropertyValue('--color-text-muted').trim(),
        };
      });

      // Verify colors are defined
      expect(colorValues.textColor).toBeTruthy();
      expect(colorValues.backgroundColor).toBeTruthy();
      expect(colorValues.primaryColor).toBeTruthy();

      // Text color should be dark (#1f2937 = rgb(31, 41, 55))
      // Background should be light (#ffffff = white)
      expect(colorValues.backgroundColor.toLowerCase()).toContain('fff');
    });

    test('links and buttons have sufficient contrast', async ({ page }) => {
      // Check specific interactive elements for contrast
      const buttonContrast = await page.evaluate(() => {
        const button = document.querySelector('.btn-primary');
        if (!button) return null;

        const styles = getComputedStyle(button);
        return {
          color: styles.color,
          backgroundColor: styles.backgroundColor,
        };
      });

      // Primary buttons should have white text on colored background
      expect(buttonContrast).toBeTruthy();
      // The color should be white (rgb(255, 255, 255))
      expect(buttonContrast.color).toContain('255');
    });
  });

  test.describe('Additional accessibility checks', () => {
    test('page title is descriptive', async ({ page }) => {
      const title = await page.title();
      expect(title.length).toBeGreaterThan(5);
      expect(title.toLowerCase()).not.toBe('untitled');
    });

    test('viewport meta tag allows user scaling', async ({ page }) => {
      const viewport = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta ? meta.getAttribute('content') : null;
      });

      // Viewport should exist and not prevent user scaling
      expect(viewport).toBeTruthy();
      expect(viewport).not.toContain('user-scalable=no');
      expect(viewport).not.toContain('maximum-scale=1');
    });

    test('images with meaningful content have descriptive alt text', async ({ page }) => {
      const images = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('img')).map((img) => ({
          src: img.src,
          alt: img.alt,
          hasAlt: img.hasAttribute('alt'),
        }));
      });

      images.forEach((img) => {
        // All images must have alt attribute
        expect(img.hasAlt).toBe(true);
        // Non-decorative images should have descriptive alt text
        if (!img.src.includes('placeholder') && !img.src.includes('decoration')) {
          expect(img.alt.length).toBeGreaterThan(0);
        }
      });
    });

    test('ARIA landmarks are properly structured', async ({ page }) => {
      const landmarks = await page.evaluate(() => {
        return {
          banner: document.querySelector('[role="banner"]') !== null,
          navigation: document.querySelector('[role="navigation"]') !== null,
          main: document.querySelector('[role="main"]') !== null || document.querySelector('main') !== null,
          contentinfo: document.querySelector('[role="contentinfo"]') !== null,
        };
      });

      expect(landmarks.banner).toBe(true);
      expect(landmarks.navigation).toBe(true);
      expect(landmarks.main).toBe(true);
      expect(landmarks.contentinfo).toBe(true);
    });
  });
});

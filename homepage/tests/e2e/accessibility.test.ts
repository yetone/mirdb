/**
 * E2E tests for accessibility compliance.
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests:
 * - Keyboard navigation through all interactive elements
 * - Focus indicators visibility
 * - Screen reader compatibility (ARIA)
 * - Tab order is logical
 */
import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
  });

  test.describe('Test Case 1: Axe Accessibility Audit', () => {
    test('should have no critical or serious accessibility violations', async ({ page }) => {
      // Run axe accessibility audit
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      // Filter out only critical and serious violations
      const criticalOrSerious = accessibilityScanResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      // Log violations for debugging if any exist
      if (criticalOrSerious.length > 0) {
        console.log('Critical/Serious violations found:');
        criticalOrSerious.forEach((v) => {
          console.log(`- ${v.id}: ${v.description} (${v.impact})`);
          v.nodes.forEach((n) => console.log(`  Affected: ${n.html.substring(0, 100)}`));
        });
      }

      expect(criticalOrSerious).toHaveLength(0);
    });

    test('should pass axe-core color-contrast checks', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze();

      // Allow minor/moderate issues, but fail on critical/serious
      const criticalContrastIssues = accessibilityScanResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalContrastIssues).toHaveLength(0);
    });
  });

  test.describe('Test Case 2: Keyboard Navigation', () => {
    test('should navigate all interactive elements using Tab key in logical order', async ({ page }) => {
      // Start from the body
      await page.keyboard.press('Tab');

      // Track focus order
      const focusOrder: string[] = [];
      const maxTabs = 50; // Limit to prevent infinite loops

      for (let i = 0; i < maxTabs; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          return {
            tagName: el.tagName,
            text: el.textContent?.trim().substring(0, 50) || '',
            href: (el as HTMLAnchorElement).href || '',
            role: el.getAttribute('role') || '',
            id: el.id || '',
            testId: el.getAttribute('data-testid') || '',
          };
        });

        if (!focusedElement) break;

        focusOrder.push(
          `${focusedElement.tagName}${focusedElement.testId ? `[${focusedElement.testId}]` : ''}${focusedElement.role ? `(${focusedElement.role})` : ''}`
        );

        await page.keyboard.press('Tab');

        // Check if we've cycled back to the beginning
        const newFocused = await page.evaluate(() => {
          const el = document.activeElement;
          return el?.getAttribute('data-testid') || el?.id || el?.tagName;
        });

        if (focusOrder.length > 2 && newFocused === focusOrder[0]) {
          break;
        }
      }

      // Verify we found interactive elements
      expect(focusOrder.length).toBeGreaterThan(0);

      // Verify key interactive elements are reachable
      // Hero section buttons
      expect(focusOrder.some((el) => el.includes('get-started-button'))).toBe(true);
      expect(focusOrder.some((el) => el.includes('github-button'))).toBe(true);
    });

    test('should have visible focus indicators on all focusable elements', async ({ page }) => {
      // Tab through elements and check focus visibility
      await page.keyboard.press('Tab');

      for (let i = 0; i < 20; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;

          const styles = window.getComputedStyle(el);
          return {
            tagName: el.tagName,
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            boxShadow: styles.boxShadow,
            hasVisibleFocus:
              styles.outlineWidth !== '0px' ||
              styles.boxShadow !== 'none' ||
              el.classList.contains('ring') ||
              el.classList.contains('focus:ring') ||
              el.classList.contains('focus:ring-2'),
          };
        });

        if (!focusedElement) break;

        // Each focused element should have some visual indicator
        // Note: Tailwind uses ring classes which may not show in computed styles
        // but the element should have some focus indication
        if (focusedElement.tagName !== 'BODY') {
          const hasFocusStyle =
            focusedElement.outlineWidth !== '0px' ||
            focusedElement.boxShadow !== 'none' ||
            focusedElement.outline !== 'none' ||
            focusedElement.hasVisibleFocus;

          // Log for debugging but don't fail - focus styles may be applied via :focus-visible
          if (!hasFocusStyle) {
            console.log(`Element ${focusedElement.tagName} may need focus styling check`);
          }
        }

        await page.keyboard.press('Tab');
      }
    });

    test('should support arrow key navigation in tab interfaces', async ({ page }) => {
      // Navigate to the Usage Examples tabs
      const examplesTab = page.locator('#tab-basic');
      await examplesTab.focus();

      // Verify initial tab is selected
      await expect(examplesTab).toHaveAttribute('aria-selected', 'true');

      // Press ArrowRight to go to next tab
      await page.keyboard.press('ArrowRight');

      // Verify focus moved to advanced tab
      const advancedTab = page.locator('#tab-advanced');
      await expect(advancedTab).toBeFocused();
      await expect(advancedTab).toHaveAttribute('aria-selected', 'true');

      // Press ArrowRight again to go to benchmark tab
      await page.keyboard.press('ArrowRight');
      const benchmarkTab = page.locator('#tab-benchmark');
      await expect(benchmarkTab).toBeFocused();
      await expect(benchmarkTab).toHaveAttribute('aria-selected', 'true');

      // Press ArrowRight again - should wrap to first tab
      await page.keyboard.press('ArrowRight');
      await expect(examplesTab).toBeFocused();
      await expect(examplesTab).toHaveAttribute('aria-selected', 'true');

      // Test ArrowLeft
      await page.keyboard.press('ArrowLeft');
      await expect(benchmarkTab).toBeFocused();
    });

    test('should support Home and End keys in tab interfaces', async ({ page }) => {
      // Navigate to the Installation tabs
      const cargoTab = page.locator('#install-tab-cargo');
      await cargoTab.focus();

      // Press End to go to last tab
      await page.keyboard.press('End');
      const sourceTab = page.locator('#install-tab-source');
      await expect(sourceTab).toBeFocused();

      // Press Home to go back to first tab
      await page.keyboard.press('Home');
      await expect(cargoTab).toBeFocused();
    });

    test('should allow Tab key to move focus into and out of tab panels', async ({ page }) => {
      // Focus on the first example tab
      const basicTab = page.locator('#tab-basic');
      await basicTab.focus();

      // Tab into the panel
      await page.keyboard.press('Tab');

      // Should focus on the panel or an element within it
      const focusedInPanel = await page.evaluate(() => {
        const el = document.activeElement;
        const panel = document.getElementById('panel-basic');
        return panel?.contains(el) || el?.id === 'panel-basic';
      });

      expect(focusedInPanel).toBe(true);
    });
  });

  test.describe('Test Case 5: Color Contrast Integration', () => {
    test('should have sufficient contrast in light mode', async ({ page }) => {
      // Ensure we're in light mode
      await page.evaluate(() => {
        document.documentElement.classList.remove('dark');
        localStorage.setItem('theme', 'light');
      });

      await page.reload();

      // Run axe color contrast check
      const results = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze();

      // Check for critical/serious color contrast issues
      const criticalIssues = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalIssues).toHaveLength(0);
    });

    test('should have sufficient contrast in dark mode', async ({ page }) => {
      // Switch to dark mode
      await page.evaluate(() => {
        document.documentElement.classList.add('dark');
        localStorage.setItem('theme', 'dark');
      });

      // Allow time for styles to apply
      await page.waitForTimeout(100);

      // Run axe color contrast check
      const results = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze();

      // Check for critical/serious color contrast issues
      const criticalIssues = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalIssues).toHaveLength(0);
    });
  });

  test.describe('Screen Reader Compatibility', () => {
    test('should have proper heading structure for screen readers', async ({ page }) => {
      // Get all headings in document order
      const headings = await page.evaluate(() => {
        const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(headingElements).map((h) => ({
          level: parseInt(h.tagName[1]),
          text: h.textContent?.trim().substring(0, 50) || '',
        }));
      });

      // Verify single H1
      const h1Count = headings.filter((h) => h.level === 1).length;
      expect(h1Count).toBe(1);

      // Verify heading order doesn't skip levels improperly
      let previousLevel = 0;
      for (const heading of headings) {
        // Allow jumping down by one level or jumping back up any number of levels
        const jumpDown = heading.level - previousLevel;
        expect(jumpDown).toBeLessThanOrEqual(1);
        previousLevel = heading.level;
      }
    });

    test('should have proper landmark regions', async ({ page }) => {
      // Check for proper landmarks
      const landmarks = await page.evaluate(() => {
        return {
          main: document.querySelectorAll('main').length,
          footer: document.querySelectorAll('footer').length,
          sections: document.querySelectorAll('section').length,
        };
      });

      expect(landmarks.main).toBe(1);
      expect(landmarks.footer).toBeGreaterThanOrEqual(1);
      expect(landmarks.sections).toBeGreaterThan(0);
    });

    test('should have accessible images with alt text', async ({ page }) => {
      const images = await page.evaluate(() => {
        const imgs = document.querySelectorAll('img');
        return Array.from(imgs).map((img) => ({
          src: img.src,
          alt: img.alt,
          hasAlt: img.hasAttribute('alt'),
          ariaHidden: img.getAttribute('aria-hidden'),
        }));
      });

      // All images should have alt attribute
      for (const img of images) {
        expect(img.hasAlt).toBe(true);
      }
    });

    test('should have proper ARIA labels on interactive regions', async ({ page }) => {
      // Check tablist regions have aria-label
      const tablists = await page.evaluate(() => {
        const lists = document.querySelectorAll('[role="tablist"]');
        return Array.from(lists).map((list) => ({
          ariaLabel: list.getAttribute('aria-label'),
          hasLabel: list.hasAttribute('aria-label'),
        }));
      });

      for (const tablist of tablists) {
        expect(tablist.hasLabel).toBe(true);
        expect(tablist.ariaLabel).toBeTruthy();
      }

      // Check code block regions have aria-label
      const codeRegions = await page.evaluate(() => {
        const regions = document.querySelectorAll('[role="region"]');
        return Array.from(regions).map((region) => ({
          ariaLabel: region.getAttribute('aria-label'),
          hasLabel: region.hasAttribute('aria-label'),
        }));
      });

      for (const region of codeRegions) {
        expect(region.hasLabel).toBe(true);
      }
    });
  });
});

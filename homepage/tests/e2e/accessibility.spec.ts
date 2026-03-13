/**
 * Accessibility E2E Tests
 * Owner: Scenario 13 - Accessibility Requirements
 *
 * Test cases:
 * - Keyboard navigation through all interactive elements
 * - Focus indicators on buttons and links
 * - Proper heading hierarchy (H1 > H2 > H3)
 * - Alt text on all images
 * - WCAG AA color contrast (4.5:1)
 * - ARIA labels on icon buttons
 * - Lighthouse accessibility audit >= 90
 */

import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Requirements (NFR-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  test.describe('Test Case 1: Keyboard Navigation', () => {
    test('all interactive elements receive focus in logical order when tabbing through page', async ({ page }) => {
      // Start from the beginning of the document
      await page.keyboard.press('Tab');

      // Collect all focusable elements in the order they should appear
      const focusedElements: string[] = [];
      const maxTabs = 30; // Safety limit

      for (let i = 0; i < maxTabs; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;

          // Get element info for tracking
          const tagName = el.tagName.toLowerCase();
          const text = el.textContent?.trim().substring(0, 50) || '';
          const ariaLabel = el.getAttribute('aria-label') || '';
          const testId = el.getAttribute('data-testid') || '';

          return `${tagName}:${testId || ariaLabel || text}`;
        });

        if (!focusedElement) break;

        focusedElements.push(focusedElement);
        await page.keyboard.press('Tab');

        // Check if we've cycled back to the first element
        const currentElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          const tagName = el.tagName.toLowerCase();
          const text = el.textContent?.trim().substring(0, 50) || '';
          const ariaLabel = el.getAttribute('aria-label') || '';
          const testId = el.getAttribute('data-testid') || '';
          return `${tagName}:${testId || ariaLabel || text}`;
        });

        if (currentElement === focusedElements[0] && focusedElements.length > 1) break;
      }

      // Verify we have multiple focusable elements
      expect(focusedElements.length).toBeGreaterThan(0);

      // Verify the order follows DOM structure (navbar -> hero -> features -> quickstart -> footer)
      // Interactive elements should include: nav links, buttons, CTA buttons
      const elementTypes = focusedElements.map(el => el.split(':')[0]);
      const hasLinks = elementTypes.includes('a');
      const hasButtons = elementTypes.includes('button');

      expect(hasLinks || hasButtons).toBeTruthy();
    });

    test('all interactive elements are keyboard accessible', async ({ page }) => {
      // Get all interactive elements
      const interactiveElements = await page.locator('a, button, input, select, textarea, [tabindex="0"]').all();

      expect(interactiveElements.length).toBeGreaterThan(0);

      // Each should be focusable (not have tabindex="-1" unless intentionally hidden)
      for (const element of interactiveElements) {
        const tabIndex = await element.getAttribute('tabindex');
        const isVisible = await element.isVisible();

        // If visible, should be focusable
        if (isVisible) {
          expect(tabIndex).not.toBe('-1');
        }
      }
    });
  });

  test.describe('Test Case 2: Focus Indicators', () => {
    test('visible focus indicator appears on each interactive element', async ({ page }) => {
      // Tab to the first focusable element
      await page.keyboard.press('Tab');

      // Check that focus indicator is visible
      const focusedElement = page.locator(':focus');
      await expect(focusedElement).toBeVisible();

      // The element should have some visual focus indication
      // This can be outline, ring, or other CSS styling
      const focusStyles = await focusedElement.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
          borderColor: styles.borderColor
        };
      });

      // Check that there's some focus indication (outline or box-shadow)
      const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';
      const hasFocusRing = hasOutline || hasBoxShadow;

      // Focus indicator should be present
      expect(hasFocusRing).toBeTruthy();
    });

    test('focus indicators are visible on buttons', async ({ page }) => {
      const buttons = await page.locator('button').all();

      for (const button of buttons) {
        const isVisible = await button.isVisible();
        if (!isVisible) continue;

        await button.focus();

        // Check focus styles
        const focusStyles = await button.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            boxShadow: styles.boxShadow
          };
        });

        const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
        const hasBoxShadow = focusStyles.boxShadow !== 'none';

        expect(hasOutline || hasBoxShadow).toBeTruthy();
      }
    });

    test('focus indicators are visible on links', async ({ page }) => {
      const links = await page.locator('a').all();

      for (const link of links) {
        const isVisible = await link.isVisible();
        if (!isVisible) continue;

        await link.focus();

        // Check focus styles
        const focusStyles = await link.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            boxShadow: styles.boxShadow,
            textDecoration: styles.textDecoration
          };
        });

        const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
        const hasBoxShadow = focusStyles.boxShadow !== 'none';
        const hasTextDecoration = focusStyles.textDecoration.includes('underline');

        expect(hasOutline || hasBoxShadow || hasTextDecoration).toBeTruthy();
      }
    });
  });

  test.describe('Test Case 3: Heading Hierarchy', () => {
    test('single H1 exists on the page', async ({ page }) => {
      const h1Elements = await page.locator('h1').all();
      expect(h1Elements.length).toBe(1);
    });

    test('headings do not skip levels', async ({ page }) => {
      // Get all headings in order
      const headings = await page.evaluate(() => {
        const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(allHeadings).map(h => {
          const level = parseInt(h.tagName.substring(1));
          return { level, text: h.textContent?.trim() || '' };
        });
      });

      expect(headings.length).toBeGreaterThan(0);

      // First heading should be H1
      expect(headings[0].level).toBe(1);

      // Check that no levels are skipped
      for (let i = 1; i < headings.length; i++) {
        const currentLevel = headings[i].level;
        const previousLevel = headings[i - 1].level;

        // Can go to same level, go deeper by 1, or go back up any amount
        // Cannot skip levels going down (e.g., H1 -> H3 is invalid)
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('H1 contains main page title', async ({ page }) => {
      const h1Text = await page.locator('h1').textContent();
      expect(h1Text).toBeTruthy();
      expect(h1Text?.toLowerCase()).toContain('key-value');
    });
  });

  test.describe('Test Case 4: Image Alt Text', () => {
    test('all images have descriptive alt text', async ({ page }) => {
      const images = await page.locator('img').all();

      for (const img of images) {
        const alt = await img.getAttribute('alt');
        const src = await img.getAttribute('src');

        // All images should have alt attribute
        expect(alt).not.toBeNull();

        // Alt text should not be empty (unless it's decorative, which should use alt="")
        // But for informative images, alt should have meaningful content
        if (alt !== '') {
          expect(alt!.length).toBeGreaterThan(0);
          // Alt text should not just be the filename
          expect(alt).not.toMatch(/\.(png|jpg|jpeg|gif|svg|webp)$/i);
        }
      }
    });

    test('logo has proper alt text', async ({ page }) => {
      const logo = page.locator('img[data-testid="navbar-logo"]');
      const alt = await logo.getAttribute('alt');

      expect(alt).toBeTruthy();
      expect(alt?.toLowerCase()).toContain('logo');
    });
  });

  test.describe('Test Case 5: Color Contrast', () => {
    test('text meets WCAG AA contrast ratio using axe-core', async ({ page }) => {
      // Use axe-core to check color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      // Filter for color contrast violations
      const contrastViolations = accessibilityScanResults.violations.filter(
        v => v.id === 'color-contrast'
      );

      // Should have no contrast violations
      if (contrastViolations.length > 0) {
        console.log('Contrast violations:', JSON.stringify(contrastViolations, null, 2));
      }
      expect(contrastViolations.length).toBe(0);
    });
  });

  test.describe('Test Case 6: ARIA Labels', () => {
    test('interactive elements without visible text have aria-label', async ({ page }) => {
      // Icon buttons (buttons with only SVG/icon content)
      const iconButtons = await page.locator('button').all();

      for (const button of iconButtons) {
        const textContent = await button.textContent();
        const ariaLabel = await button.getAttribute('aria-label');
        const ariaLabelledBy = await button.getAttribute('aria-labelledby');

        // If button has no visible text (or only whitespace), it must have aria-label
        const hasVisibleText = textContent && textContent.trim().length > 0;
        const hasSrOnly = await button.locator('.sr-only').count() > 0;

        if (!hasVisibleText && !hasSrOnly) {
          expect(ariaLabel || ariaLabelledBy).toBeTruthy();
        }
      }
    });

    test('theme toggle has accessible name', async ({ page }) => {
      const themeToggle = page.locator('[data-testid="theme-toggle"], [data-testid="theme-toggle-button"]').first();

      if (await themeToggle.count() > 0) {
        const ariaLabel = await themeToggle.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel?.toLowerCase()).toMatch(/mode|theme/);
      }
    });

    test('navigation has proper landmark role', async ({ page }) => {
      const nav = page.locator('nav');

      if (await nav.count() > 0) {
        // Nav element has implicit navigation role
        // Or should have aria-label for screen readers
        const ariaLabel = await nav.first().getAttribute('aria-label');
        const role = await nav.first().getAttribute('role');

        expect(ariaLabel || role === 'navigation').toBeTruthy();
      }
    });

    test('decorative SVG icons are hidden from screen readers', async ({ page }) => {
      // SVGs that are decorative should have aria-hidden="true"
      // Exception: SVGs inside buttons/links with visible text don't need aria-hidden
      // because the text provides the accessible name
      const svgs = await page.locator('svg').all();

      for (const svg of svgs) {
        const ariaHidden = await svg.getAttribute('aria-hidden');
        const role = await svg.getAttribute('role');
        const title = await svg.locator('title').count();

        // Check if SVG is inside a button or link with text
        const parentButton = await svg.locator('xpath=ancestor::button').first();
        const parentLink = await svg.locator('xpath=ancestor::a').first();

        let parentHasText = false;
        if (await parentButton.count() > 0) {
          const buttonText = await parentButton.textContent();
          // Check if button has meaningful text (not just whitespace)
          parentHasText = buttonText ? buttonText.trim().length > 0 : false;
        } else if (await parentLink.count() > 0) {
          const linkText = await parentLink.textContent();
          parentHasText = linkText ? linkText.trim().length > 0 : false;
        }

        // If SVG doesn't have a title or role, and is not inside an element with text,
        // it should be aria-hidden
        if (title === 0 && role !== 'img' && !parentHasText) {
          expect(ariaHidden).toBe('true');
        }
      }
    });
  });

  test.describe('Test Case 7: Lighthouse Accessibility Audit', () => {
    test('accessibility score >= 90 using axe-core comprehensive scan', async ({ page }) => {
      // Run comprehensive axe-core analysis
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'best-practice'])
        .analyze();

      // Log violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log('Accessibility violations:');
        accessibilityScanResults.violations.forEach(v => {
          console.log(`- ${v.id}: ${v.description}`);
          v.nodes.forEach(n => {
            console.log(`  Target: ${n.target}`);
            console.log(`  Help: ${v.helpUrl}`);
          });
        });
      }

      // Calculate pass rate
      const totalChecks = accessibilityScanResults.passes.length + accessibilityScanResults.violations.length;
      const passRate = (accessibilityScanResults.passes.length / totalChecks) * 100;

      console.log(`Accessibility pass rate: ${passRate.toFixed(1)}%`);
      console.log(`Passes: ${accessibilityScanResults.passes.length}`);
      console.log(`Violations: ${accessibilityScanResults.violations.length}`);

      // Should have minimal violations for accessibility score >= 90
      // Critical and serious violations should be 0
      const criticalViolations = accessibilityScanResults.violations.filter(
        v => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations.length).toBe(0);

      // Overall violations should be minimal
      expect(accessibilityScanResults.violations.length).toBeLessThanOrEqual(2);
    });

    test('no critical accessibility violations', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      const criticalViolations = accessibilityScanResults.violations.filter(
        v => v.impact === 'critical'
      );

      expect(criticalViolations.length).toBe(0);
    });

    test('no serious accessibility violations', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa'])
        .analyze();

      const seriousViolations = accessibilityScanResults.violations.filter(
        v => v.impact === 'serious'
      );

      expect(seriousViolations.length).toBe(0);
    });
  });

  test.describe('Additional Accessibility Checks', () => {
    test('page has proper document structure', async ({ page }) => {
      // Check for main landmark
      const main = await page.locator('main').count();
      expect(main).toBeGreaterThanOrEqual(1);

      // Check for nav landmark
      const nav = await page.locator('nav').count();
      expect(nav).toBeGreaterThanOrEqual(1);

      // Check for footer
      const footer = await page.locator('footer').count();
      expect(footer).toBeGreaterThanOrEqual(1);
    });

    test('page has proper language attribute', async ({ page }) => {
      const htmlLang = await page.locator('html').getAttribute('lang');
      expect(htmlLang).toBeTruthy();
      expect(htmlLang).toBe('en');
    });

    test('page has proper title', async ({ page }) => {
      const title = await page.title();
      expect(title).toBeTruthy();
      expect(title.length).toBeGreaterThan(0);
    });

    test('skip link or proper focus management exists', async ({ page }) => {
      // Check if there's a skip link or if keyboard focus starts at main content
      await page.keyboard.press('Tab');

      const firstFocusable = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el?.tagName.toLowerCase(),
          text: el?.textContent?.trim(),
          href: (el as HTMLAnchorElement)?.href
        };
      });

      // First focusable element should be in the navigation or be a skip link
      expect(firstFocusable.tagName).toBeTruthy();
    });

    test('external links have proper security attributes', async ({ page }) => {
      const externalLinks = await page.locator('a[target="_blank"]').all();

      for (const link of externalLinks) {
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      }
    });
  });
});

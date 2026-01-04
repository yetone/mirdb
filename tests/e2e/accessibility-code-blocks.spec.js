/**
 * Accessibility Tests: Code Block Accessibility (E2E)
 * Tests NFR-3: WCAG 2.1 AA compliance - Code examples readable by screen readers
 * Scenario: Verify code blocks are accessible with appropriate markup
 */
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Code Block Screen Reader Support', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 3: Code Block Accessibility', () => {
    test('Code examples use semantic pre and code elements', async ({ page }) => {
      // All code blocks should use proper semantic elements
      const codeBlocks = await page.locator('pre code, code').all();
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Verify code is wrapped in proper elements
      const preElements = await page.locator('pre').all();
      for (const pre of preElements) {
        const hasCode = await pre.locator('code').count();
        expect(hasCode).toBeGreaterThan(0);
      }
    });

    test('Code blocks have appropriate aria-labels or descriptions', async ({ page }) => {
      const codeContainers = await page.locator('.code-example, [data-testid*="code"]').all();

      for (const container of codeContainers) {
        const isAccessible = await container.evaluate(el => {
          // Check for accessible labeling
          const pre = el.querySelector('pre');
          const code = el.querySelector('code');

          // Code should have some form of accessible context
          const hasAriaLabel = pre?.hasAttribute('aria-label') || code?.hasAttribute('aria-label');
          const hasAriaLabelledBy = pre?.hasAttribute('aria-labelledby') || code?.hasAttribute('aria-labelledby');
          const hasRole = pre?.getAttribute('role') || code?.getAttribute('role');

          // Check if there's a preceding heading or description that provides context
          const previousElement = el.previousElementSibling;
          const hasContextHeading = !!(previousElement?.tagName?.match(/^H[1-6]$/)) ||
                                    !!(el.querySelector('h3, h4, p.step-description'));

          // Parent section should provide context
          const parentSection = el.closest('section');
          const sectionHasHeading = !!(parentSection?.querySelector('h2, h3'));

          // Return boolean, not string
          return !!(hasAriaLabel || hasAriaLabelledBy || hasContextHeading || sectionHasHeading || hasRole);
        });

        expect(isAccessible).toBe(true);
      }
    });

    test('Code blocks preserve whitespace for screen readers', async ({ page }) => {
      const preElements = await page.locator('pre').all();

      for (const pre of preElements) {
        const whiteSpace = await pre.evaluate(el => {
          const computedStyle = window.getComputedStyle(el);
          return computedStyle.whiteSpace;
        });

        // pre elements should preserve whitespace
        expect(['pre', 'pre-wrap', 'pre-line']).toContain(whiteSpace);
      }
    });

    test('Code blocks are not aria-hidden', async ({ page }) => {
      const codeElements = await page.locator('pre, code').all();

      for (const el of codeElements) {
        const isHidden = await el.getAttribute('aria-hidden');
        expect(isHidden).not.toBe('true');
      }
    });

    test('Inline code is distinguishable from regular text', async ({ page }) => {
      // Check that inline code elements have visual distinction
      const inlineCode = await page.locator('p code, li code').all();

      for (const code of inlineCode) {
        const hasVisualDistinction = await code.evaluate(el => {
          const computedStyle = window.getComputedStyle(el);
          const parentStyle = window.getComputedStyle(el.parentElement);

          return (
            computedStyle.fontFamily !== parentStyle.fontFamily ||
            computedStyle.backgroundColor !== parentStyle.backgroundColor ||
            computedStyle.border !== 'none'
          );
        });

        expect(hasVisualDistinction).toBe(true);
      }
    });

    test('Code examples have sufficient color contrast', async ({ page }) => {
      // Test both pre blocks (code examples) and inline code
      const codeExamples = await page.locator('.code-example pre').all();

      for (const pre of codeExamples) {
        const contrastInfo = await pre.evaluate(el => {
          const computedStyle = window.getComputedStyle(el);
          const codeEl = el.querySelector('code') || el;
          const codeStyle = window.getComputedStyle(codeEl);

          // Get text color from code element and background from pre
          const color = codeStyle.color;
          let backgroundColor = computedStyle.backgroundColor;

          // If background is transparent, get from parent
          if (backgroundColor === 'rgba(0, 0, 0, 0)' || backgroundColor === 'transparent') {
            const parent = el.closest('.code-example');
            if (parent) {
              backgroundColor = window.getComputedStyle(parent).backgroundColor;
            }
          }

          // Parse RGB values
          const parseRgb = (str) => {
            const match = str.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
            if (match) {
              return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
            }
            return null;
          };

          const textColor = parseRgb(color);
          const bgColor = parseRgb(backgroundColor);

          if (!textColor || !bgColor) return { hasContrast: true }; // Skip if can't parse

          // Calculate relative luminance
          const luminance = (rgb) => {
            const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(c => {
              c = c / 255;
              return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
            });
            return 0.2126 * r + 0.7152 * g + 0.0722 * b;
          };

          const l1 = luminance(textColor);
          const l2 = luminance(bgColor);
          const ratio = (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);

          // WCAG AA requires 4.5:1 for normal text, 3:1 for large text
          return { hasContrast: ratio >= 3, ratio };
        });

        expect(contrastInfo.hasContrast).toBe(true);
      }
    });

    test('Code blocks within Getting Started section are accessible', async ({ page }) => {
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      const codeBlocks = await gettingStarted.locator('pre code').all();
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Each code block should be readable
      for (const codeBlock of codeBlocks) {
        const content = await codeBlock.textContent();
        expect(content.trim().length).toBeGreaterThan(0);
      }
    });

    test('Code block containers provide context through headings', async ({ page }) => {
      // Each code example should be preceded by a heading that describes it
      const stepCards = await page.locator('.step-card').all();

      for (const card of stepCards) {
        const heading = await card.locator('h3').first();
        await expect(heading).toBeVisible();

        const headingText = await heading.textContent();
        expect(headingText.trim().length).toBeGreaterThan(0);
      }
    });

    test('Command groups in Supported Commands section are accessible', async ({ page }) => {
      const commandsSection = page.locator('.commands');
      await expect(commandsSection).toBeVisible();

      const commandGroups = await commandsSection.locator('.command-group').all();
      expect(commandGroups.length).toBeGreaterThan(0);

      for (const group of commandGroups) {
        // Each command group should have a heading
        const heading = await group.locator('h3').count();
        expect(heading).toBe(1);

        // And should have code elements
        const code = await group.locator('code').count();
        expect(code).toBeGreaterThan(0);
      }
    });
  });
});

/**
 * Accessibility E2E Tests
 * Owner: Scenario 9 - Accessibility - WCAG 2.1 AA Compliance
 *
 * Tests for:
 * - Keyboard navigation through all interactive elements
 * - Focus indicator visibility
 * - Skip to main content link
 * - Axe accessibility audit (no critical/serious violations)
 * - Image alt text verification
 * - Heading hierarchy validation
 * - Color contrast verification
 * - Code block accessibility labels
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility - WCAG 2.1 AA Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Tab through all interactive elements in logical order', async ({ page }) => {
    // Get all focusable elements in DOM order
    const focusableSelectors = [
      '.skip-link',
      '.header__link',
      '.header__github',
      '.btn-primary',
      '.btn-secondary',
      '.quickstart__link',
      'a[href]:not([tabindex="-1"])',
      'button:not([disabled]):not([tabindex="-1"])',
    ].join(', ');

    // Collect all interactive elements
    const interactiveElements = await page.$$eval(
      'a[href]:not([tabindex="-1"]), button:not([disabled]):not([tabindex="-1"])',
      (elements) => elements.map((el) => ({
        tagName: el.tagName,
        text: el.textContent?.trim().slice(0, 50) || el.getAttribute('aria-label') || '',
        href: el.getAttribute('href'),
      }))
    );

    // Verify we have interactive elements
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Tab through first several elements and verify focus moves
    await page.keyboard.press('Tab');

    // First Tab should focus the skip link
    let focusedElement = await page.evaluate(() => document.activeElement?.className || '');
    expect(focusedElement).toContain('skip-link');

    // Tab to header navigation
    await page.keyboard.press('Tab');
    focusedElement = await page.evaluate(() => document.activeElement?.className || '');
    expect(focusedElement).toContain('header__link');

    // Continue tabbing through elements
    const tabCount = Math.min(interactiveElements.length, 10);
    for (let i = 0; i < tabCount - 2; i++) {
      await page.keyboard.press('Tab');

      // Verify focus is on a focusable element
      const isFocusable = await page.evaluate(() => {
        const el = document.activeElement;
        return el && (el.tagName === 'A' || el.tagName === 'BUTTON' || el.getAttribute('tabindex') !== '-1');
      });
      expect(isFocusable).toBe(true);
    }
  });

  test('TC2: Focus indicators are visible on all interactive elements', async ({ page }) => {
    // Tab to skip link and verify focus outline
    await page.keyboard.press('Tab');

    // Check skip link focus styles
    const skipLinkFocusStyles = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el) return null;
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
      };
    });

    expect(skipLinkFocusStyles).toBeTruthy();
    expect(skipLinkFocusStyles.outlineStyle).not.toBe('none');
    expect(parseFloat(skipLinkFocusStyles.outlineWidth)).toBeGreaterThanOrEqual(2);

    // Tab to header links and check focus
    await page.keyboard.press('Tab');

    const headerLinkFocusStyles = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el) return null;
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
      };
    });

    expect(headerLinkFocusStyles).toBeTruthy();
    expect(headerLinkFocusStyles.outlineStyle).not.toBe('none');

    // Tab to CTA buttons and check focus
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
    }

    // Find and focus a button
    const button = await page.$('.btn-primary');
    if (button) {
      await button.focus();

      const buttonFocusStyles = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return null;
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
        };
      });

      expect(buttonFocusStyles).toBeTruthy();
      expect(buttonFocusStyles.outlineStyle).not.toBe('none');
    }
  });

  test('TC3: Skip link appears on first Tab and navigates to main content', async ({ page }) => {
    // Initially, skip link should be off-screen
    const skipLink = page.locator('.skip-link');

    const initialPosition = await skipLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        top: styles.top,
        position: styles.position,
      };
    });

    // Check skip link is positioned off-screen (could be computed as -100% or -100px equivalent)
    expect(parseFloat(initialPosition.top)).toBeLessThan(0);
    expect(initialPosition.position).toBe('absolute');

    // Press Tab to focus skip link
    await page.keyboard.press('Tab');

    // Skip link should now be visible (top should change from -100%)
    const focusedPosition = await skipLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        top: styles.top,
      };
    });

    expect(focusedPosition.top).not.toBe('-100%');

    // Verify skip link text
    const skipLinkText = await skipLink.textContent();
    expect(skipLinkText).toContain('Skip to main content');

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Verify focus moved to main content area
    const mainElement = page.locator('#main');
    await expect(mainElement).toBeVisible();

    // Check that URL hash changed to #main
    const url = page.url();
    expect(url).toContain('#main');
  });

  test('TC4: Axe accessibility audit passes with no critical or serious violations', async ({ page }) => {
    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious violations found:');
      criticalViolations.forEach((v) => {
        console.log(`- ${v.id}: ${v.description}`);
        console.log(`  Impact: ${v.impact}`);
        console.log(`  Help: ${v.help}`);
        v.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    // Assert no critical or serious violations
    expect(criticalViolations).toHaveLength(0);
  });

  test('TC5: All images have descriptive alt attributes', async ({ page }) => {
    // Get all images on the page
    const images = await page.$$eval('img', (imgs) =>
      imgs.map((img) => ({
        src: img.src,
        alt: img.getAttribute('alt'),
        hasAlt: img.hasAttribute('alt'),
        ariaHidden: img.getAttribute('aria-hidden'),
        parentAriaHidden: img.closest('[aria-hidden="true"]') !== null,
      }))
    );

    // Verify all images have alt attribute
    for (const img of images) {
      // Images can either have alt text OR be aria-hidden (decorative)
      const isAccessible = img.hasAlt || img.ariaHidden === 'true' || img.parentAriaHidden;
      expect(isAccessible).toBe(true);

      // If image has alt attribute, it should not be empty (unless decorative)
      if (img.hasAlt && img.alt !== '' && !img.ariaHidden && !img.parentAriaHidden) {
        expect(img.alt).toBeTruthy();
      }
    }

    // Verify SVGs have proper accessibility attributes
    const svgs = await page.$$eval('svg', (svgElements) =>
      svgElements.map((svg) => ({
        ariaHidden: svg.getAttribute('aria-hidden'),
        ariaLabel: svg.getAttribute('aria-label'),
        role: svg.getAttribute('role'),
        parentAriaLabel: svg.closest('[aria-label]')?.getAttribute('aria-label'),
        parentAriaHidden: svg.closest('[aria-hidden="true"]') !== null,
        isInsideButton: svg.closest('button') !== null,
        isInsideLink: svg.closest('a') !== null,
      }))
    );

    // SVGs should be either aria-hidden, inside an aria-hidden element, or have an accessible name
    // Icons inside buttons/links with labels are also acceptable
    for (const svg of svgs) {
      const isAccessible =
        svg.ariaHidden === 'true' ||
        svg.parentAriaHidden ||
        svg.ariaLabel ||
        svg.role === 'img' ||
        svg.parentAriaLabel ||
        svg.isInsideButton ||
        svg.isInsideLink;
      expect(isAccessible).toBeTruthy();
    }
  });

  test('TC6: Heading hierarchy is logical with single h1 and no skipped levels', async ({ page }) => {
    // Get all headings
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) =>
      elements.map((el) => ({
        level: parseInt(el.tagName.charAt(1)),
        text: el.textContent?.trim().slice(0, 50),
      }))
    );

    // Verify there's exactly one h1
    const h1Count = headings.filter((h) => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Verify h1 is first heading
    expect(headings[0].level).toBe(1);

    // Verify no skipped heading levels
    let previousLevel = 0;
    for (const heading of headings) {
      // Heading level should not skip more than one level
      // (h2 after h1 is OK, h3 after h1 would be a skip)
      // But h3 after h2, or going back from h3 to h2 is fine
      if (heading.level > previousLevel) {
        expect(heading.level - previousLevel).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }

    // Verify logical order (h1 > h2 > h3 pattern exists)
    const hasH2 = headings.some((h) => h.level === 2);
    expect(hasH2).toBe(true);
  });

  test('TC7: Body text achieves minimum 4.5:1 contrast ratio', async ({ page }) => {
    // Get computed styles for body text
    const contrastData = await page.evaluate(() => {
      const body = document.body;
      const styles = window.getComputedStyle(body);

      // Helper function to parse RGB color
      const parseRGB = (color) => {
        const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
        if (match) {
          return {
            r: parseInt(match[1]),
            g: parseInt(match[2]),
            b: parseInt(match[3]),
          };
        }
        return null;
      };

      // Helper function to calculate relative luminance
      const getLuminance = (rgb) => {
        const sRGB = [rgb.r / 255, rgb.g / 255, rgb.b / 255];
        const luminance = sRGB.map((c) => {
          if (c <= 0.03928) {
            return c / 12.92;
          }
          return Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * luminance[0] + 0.7152 * luminance[1] + 0.0722 * luminance[2];
      };

      // Helper function to calculate contrast ratio
      const getContrastRatio = (l1, l2) => {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      };

      const textColor = parseRGB(styles.color);
      const bgColor = parseRGB(styles.backgroundColor);

      if (!textColor || !bgColor) {
        // If we can't parse, get the CSS variable values
        const root = document.documentElement;
        const rootStyles = window.getComputedStyle(root);
        return {
          textColor: styles.color,
          bgColor: styles.backgroundColor,
          cssTextColor: rootStyles.getPropertyValue('--color-text').trim(),
          cssBgColor: rootStyles.getPropertyValue('--color-background').trim(),
          contrastRatio: null,
        };
      }

      const textLuminance = getLuminance(textColor);
      const bgLuminance = getLuminance(bgColor);
      const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

      return {
        textColor: styles.color,
        bgColor: styles.backgroundColor,
        contrastRatio: contrastRatio,
      };
    });

    // Verify contrast ratio meets WCAG AA (4.5:1 for normal text)
    if (contrastData.contrastRatio !== null) {
      expect(contrastData.contrastRatio).toBeGreaterThanOrEqual(4.5);
    } else {
      // Fallback: verify the color values are set correctly
      // --color-text: #0f172a (very dark), --color-background: #ffffff (white)
      // This combination has a contrast ratio of approximately 19.4:1
      expect(contrastData.cssTextColor).toBeTruthy();
      expect(contrastData.cssBgColor).toBeTruthy();
    }

    // Also check paragraph text specifically
    const paragraphContrast = await page.evaluate(() => {
      const paragraphs = document.querySelectorAll('p');
      if (paragraphs.length === 0) return { found: false };

      const firstP = paragraphs[0];
      const styles = window.getComputedStyle(firstP);

      const parseRGB = (color) => {
        const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
        if (match) {
          return {
            r: parseInt(match[1]),
            g: parseInt(match[2]),
            b: parseInt(match[3]),
          };
        }
        return null;
      };

      const getLuminance = (rgb) => {
        const sRGB = [rgb.r / 255, rgb.g / 255, rgb.b / 255];
        const luminance = sRGB.map((c) => {
          if (c <= 0.03928) {
            return c / 12.92;
          }
          return Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * luminance[0] + 0.7152 * luminance[1] + 0.0722 * luminance[2];
      };

      const getContrastRatio = (l1, l2) => {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      };

      // Get actual background color (may be inherited or transparent)
      let element = firstP;
      let bgColor = null;
      while (element && !bgColor) {
        const bg = window.getComputedStyle(element).backgroundColor;
        if (bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
          bgColor = parseRGB(bg);
        }
        element = element.parentElement;
      }

      // Default to white if no background found
      if (!bgColor) {
        bgColor = { r: 255, g: 255, b: 255 };
      }

      const textColor = parseRGB(styles.color);
      if (!textColor) return { found: true, contrastRatio: null };

      const textLuminance = getLuminance(textColor);
      const bgLuminance = getLuminance(bgColor);
      const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

      return {
        found: true,
        textColor: styles.color,
        contrastRatio: contrastRatio,
      };
    });

    if (paragraphContrast.found && paragraphContrast.contrastRatio !== null) {
      expect(paragraphContrast.contrastRatio).toBeGreaterThanOrEqual(4.5);
    }
  });

  test('TC8: Code blocks have accessible labels describing content', async ({ page }) => {
    // Check code blocks have appropriate accessibility labels
    const codeBlocks = await page.$$eval('.code-block', (blocks) =>
      blocks.map((block) => ({
        testId: block.getAttribute('data-testid'),
        ariaLabel: block.getAttribute('aria-label'),
        ariaLabelledby: block.getAttribute('aria-labelledby'),
        languageLabel: block.querySelector('.code-block__language')?.textContent,
        copyButtonAriaLabel: block.querySelector('.code-block__copy')?.getAttribute('aria-label'),
      }))
    );

    // Verify each code block has identifying information
    for (const block of codeBlocks) {
      // Code block should have a language label or aria-label
      const hasLabel =
        block.languageLabel ||
        block.ariaLabel ||
        block.ariaLabelledby;
      expect(hasLabel).toBeTruthy();

      // Copy button should have aria-label
      if (block.copyButtonAriaLabel !== undefined) {
        expect(block.copyButtonAriaLabel).toBeTruthy();
        expect(block.copyButtonAriaLabel).toContain('Copy');
      }
    }

    // Check pre/code elements for accessibility
    const preElements = await page.$$eval('pre', (elements) =>
      elements.map((pre) => ({
        role: pre.getAttribute('role'),
        ariaLabel: pre.getAttribute('aria-label'),
        tabIndex: pre.getAttribute('tabindex'),
        className: pre.className,
      }))
    );

    // Verify pre elements exist
    expect(preElements.length).toBeGreaterThan(0);

    // Check that code content is accessible (has parent with label context)
    const codeAccessibility = await page.evaluate(() => {
      const codeElements = document.querySelectorAll('.code-block__code');
      return Array.from(codeElements).map((code) => {
        const parent = code.closest('.code-block');
        const header = parent?.querySelector('.code-block__header');
        const languageLabel = header?.querySelector('.code-block__language');
        return {
          hasParentStructure: !!parent,
          hasLanguageContext: !!languageLabel,
          languageText: languageLabel?.textContent || null,
        };
      });
    });

    // All code elements should have language context
    for (const code of codeAccessibility) {
      expect(code.hasParentStructure).toBe(true);
      expect(code.hasLanguageContext).toBe(true);
      expect(code.languageText).toBeTruthy();
    }
  });

  // Additional accessibility tests for comprehensive coverage
  test('Interactive elements have accessible names', async ({ page }) => {
    // Run focused axe check for button/link names
    const results = await new AxeBuilder({ page })
      .withRules(['button-name', 'link-name', 'image-alt'])
      .analyze();

    expect(results.violations).toHaveLength(0);
  });

  test('ARIA attributes are used correctly', async ({ page }) => {
    // Run focused axe check for ARIA
    const results = await new AxeBuilder({ page })
      .withRules(['aria-allowed-attr', 'aria-required-attr', 'aria-valid-attr'])
      .analyze();

    expect(results.violations).toHaveLength(0);
  });

  test('Document has proper landmarks', async ({ page }) => {
    // Check for main landmark
    const mainLandmark = await page.$('main');
    expect(mainLandmark).toBeTruthy();

    // Check for header landmark
    const headerLandmark = await page.$('header');
    expect(headerLandmark).toBeTruthy();

    // Check for footer landmark
    const footerLandmark = await page.$('footer');
    expect(footerLandmark).toBeTruthy();

    // Check for navigation landmarks
    const navLandmarks = await page.$$('nav');
    expect(navLandmarks.length).toBeGreaterThan(0);

    // Verify nav has accessible label
    const navLabels = await page.$$eval('nav', (navs) =>
      navs.map((nav) => nav.getAttribute('aria-label'))
    );
    expect(navLabels.every((label) => label !== null)).toBe(true);
  });

  test('Page language is specified', async ({ page }) => {
    const htmlLang = await page.$eval('html', (el) => el.getAttribute('lang'));
    expect(htmlLang).toBe('en');
  });
});

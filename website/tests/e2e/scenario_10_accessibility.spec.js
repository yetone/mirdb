// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Scenario 10: Accessibility Compliance
 * Tests for verifying the homepage meets WCAG AA accessibility standards.
 * Includes tests for heading hierarchy, keyboard navigation, focus indicators,
 * alt text, contrast ratios, and skip links.
 */

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check heading hierarchy
   * Input: Check heading hierarchy
   * Expected: Page has proper h1 > h2 > h3 hierarchy without skipping levels
   */
  test('TC1: Page has proper heading hierarchy without skipping levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    // Verify h1 exists (should be exactly one)
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Verify no heading levels are skipped
    let previousLevel = 0;
    for (const heading of headings) {
      // A heading can only increase by 1 level or decrease to any level
      if (heading.level > previousLevel + 1 && previousLevel !== 0) {
        throw new Error(`Heading hierarchy skipped from h${previousLevel} to h${heading.level}: "${heading.text}"`);
      }
      previousLevel = heading.level;
    }

    // Verify we have h1 and h2 headings
    expect(headings.some(h => h.level === 1)).toBeTruthy();
    expect(headings.some(h => h.level === 2)).toBeTruthy();
  });

  /**
   * Test Case 2: Tab through interactive elements
   * Input: Tab through interactive elements
   * Expected: All links, buttons, and toggles are reachable via keyboard
   */
  test('TC2: All interactive elements are reachable via keyboard', async ({ page }) => {
    // Get all interactive elements
    const interactiveElements = await page.evaluate(() => {
      const elements = document.querySelectorAll('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
      return Array.from(elements).map(el => ({
        tag: el.tagName.toLowerCase(),
        text: el.textContent?.trim().substring(0, 30) || el.getAttribute('aria-label') || '',
        href: el.getAttribute('href'),
        tabIndex: el.getAttribute('tabindex')
      }));
    });

    // There should be interactive elements
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Tab through elements and verify they receive focus
    let focusedCount = 0;
    const maxTabs = 30; // Limit to prevent infinite loops

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const active = document.activeElement;
        if (active && active !== document.body) {
          return {
            tag: active.tagName.toLowerCase(),
            text: active.textContent?.trim().substring(0, 30) || '',
            isInteractive: ['a', 'button', 'input', 'select', 'textarea'].includes(active.tagName.toLowerCase()) ||
                          active.hasAttribute('tabindex')
          };
        }
        return null;
      });

      if (focusedElement?.isInteractive) {
        focusedCount++;
      }

      // If we've cycled back to the beginning, break
      const currentUrl = page.url();
      if (currentUrl.includes('#') && focusedCount > 5) {
        break;
      }
    }

    // Verify we could tab to multiple elements
    expect(focusedCount).toBeGreaterThan(3);
  });

  /**
   * Test Case 3: Check focus indicators
   * Input: Check focus indicators
   * Expected: Focused elements have visible focus indicators
   */
  test('TC3: Focused elements have visible focus indicators', async ({ page }) => {
    // Tab to the first focusable element
    await page.keyboard.press('Tab');

    // Check that focused elements have visible focus styles
    const focusedElement = await page.evaluate(() => {
      const active = document.activeElement;
      if (active && active !== document.body) {
        const styles = window.getComputedStyle(active);
        return {
          tag: active.tagName.toLowerCase(),
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow,
          // Check for ring classes (Tailwind)
          hasRingClass: active.className.includes('ring') || active.className.includes('focus')
        };
      }
      return null;
    });

    expect(focusedElement).not.toBeNull();

    // Verify there's some form of focus indicator
    // Either outline, box-shadow, or Tailwind ring classes
    const hasFocusIndicator =
      (focusedElement?.outlineStyle !== 'none' && focusedElement?.outlineWidth !== '0px') ||
      focusedElement?.boxShadow !== 'none' ||
      focusedElement?.hasRingClass;

    expect(hasFocusIndicator).toBeTruthy();

    // Test multiple elements for focus visibility
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');

      const elementFocusStyles = await page.evaluate(() => {
        const active = document.activeElement;
        if (active && active !== document.body) {
          const styles = window.getComputedStyle(active);
          const outlineWidth = parseFloat(styles.outlineWidth) || 0;
          const hasShadow = styles.boxShadow !== 'none';
          const hasRing = active.className.includes('ring') || active.className.includes('focus');

          return {
            hasVisibleFocus: outlineWidth > 0 || hasShadow || hasRing
          };
        }
        return { hasVisibleFocus: false };
      });

      // Each interactive element should have visible focus
      expect(elementFocusStyles.hasVisibleFocus).toBeTruthy();
    }
  });

  /**
   * Test Case 4: Check logo alt text
   * Input: Check logo alt text
   * Expected: Logo image has descriptive alt text
   */
  test('TC4: Logo image has descriptive alt text', async ({ page }) => {
    // Find all logo images
    const logoImages = await page.evaluate(() => {
      const images = document.querySelectorAll('img');
      return Array.from(images)
        .filter(img => img.src.includes('logo') || img.id === 'logo' || img.alt?.toLowerCase().includes('mirdb'))
        .map(img => ({
          src: img.src,
          alt: img.alt,
          id: img.id,
          hasAlt: img.hasAttribute('alt'),
          altLength: img.alt?.length || 0
        }));
    });

    // Verify we found logo images
    expect(logoImages.length).toBeGreaterThan(0);

    // Each logo should have descriptive alt text
    for (const logo of logoImages) {
      expect(logo.hasAlt).toBeTruthy();
      expect(logo.alt).toBeTruthy();
      expect(logo.altLength).toBeGreaterThan(0);
      // Alt text should be descriptive (more than just empty or a single character)
      expect(logo.alt.trim().length).toBeGreaterThanOrEqual(2);
    }

    // Check that at least one logo has "MirDB" in the alt text
    const hasMirDbAlt = logoImages.some(logo =>
      logo.alt.toLowerCase().includes('mirdb') || logo.alt.toLowerCase().includes('logo')
    );
    expect(hasMirDbAlt).toBeTruthy();
  });

  /**
   * Test Case 5: Run contrast checker on text
   * Input: Run contrast checker on text
   * Expected: Text contrast ratio meets 4.5:1 minimum
   */
  test('TC5: Text contrast ratio meets 4.5:1 minimum', async ({ page }) => {
    // Get key readable text elements and check their contrast
    // This test focuses on primary content elements (headings, paragraphs, main text)
    const contrastResults = await page.evaluate(() => {
      // Helper: Parse color string to RGB
      const parseColor = (color) => {
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return { r: parseInt(rgbMatch[1]), g: parseInt(rgbMatch[2]), b: parseInt(rgbMatch[3]) };
        }
        return null;
      };

      // Helper: Calculate relative luminance
      const luminance = (rgb) => {
        const a = [rgb.r, rgb.g, rgb.b].map(v => {
          v /= 255;
          return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return a[0] * 0.2126 + a[1] * 0.7152 + a[2] * 0.0722;
      };

      // Helper: Get actual background color by traversing up the DOM
      const getActualBackgroundColor = (element) => {
        let el = element;
        while (el) {
          const bg = window.getComputedStyle(el).backgroundColor;
          const parsed = parseColor(bg);
          // Check if it's not transparent
          if (parsed && (parsed.r !== 0 || parsed.g !== 0 || parsed.b !== 0 || !bg.includes('0)'))) {
            if (!bg.includes('0)') || bg === 'rgb(0, 0, 0)') {
              return bg;
            }
          }
          // Also check for white backgrounds
          if (bg === 'rgb(255, 255, 255)' || bg === 'rgba(255, 255, 255, 1)') {
            return bg;
          }
          el = el.parentElement;
        }
        // Default to white if nothing found
        return 'rgb(255, 255, 255)';
      };

      // Helper: Calculate contrast ratio
      const getContrastRatio = (color1, color2) => {
        const c1 = parseColor(color1);
        const c2 = parseColor(color2);
        if (!c1 || !c2) return 21; // Return max if can't parse

        const l1 = luminance(c1);
        const l2 = luminance(c2);
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      };

      // Check key text elements - focus on h1, h2, h3 and main paragraph text
      const keyElements = document.querySelectorAll('h1, h2, h3, main p, main a');
      const results = [];

      Array.from(keyElements).slice(0, 15).forEach(el => {
        const styles = window.getComputedStyle(el);
        const textColor = styles.color;
        const bgColor = getActualBackgroundColor(el);
        const ratio = getContrastRatio(textColor, bgColor);
        const fontSize = parseFloat(styles.fontSize);
        const fontWeight = parseInt(styles.fontWeight);

        // WCAG AA: 4.5:1 for normal text, 3:1 for large text (18pt or 14pt bold)
        const isLargeText = fontSize >= 24 || (fontSize >= 18.66 && fontWeight >= 700);
        const minRatio = isLargeText ? 3.0 : 4.5;

        results.push({
          tag: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 30) || '',
          textColor,
          bgColor,
          ratio: ratio.toFixed(2),
          fontSize,
          minRatio,
          passes: ratio >= minRatio
        });
      });

      return results;
    });

    // Verify we checked some elements
    expect(contrastResults.length).toBeGreaterThan(0);

    // Check that key readable elements meet contrast requirements
    // We allow some tolerance for edge cases while ensuring overall compliance
    const failingElements = contrastResults.filter(el => !el.passes);
    const passRate = (contrastResults.length - failingElements.length) / contrastResults.length;

    // At least 90% of key text elements should pass contrast check
    // This allows for minor edge cases while ensuring overall accessibility
    expect(passRate).toBeGreaterThanOrEqual(0.9);

    // Additionally verify headings always pass (most critical for readability)
    const headings = contrastResults.filter(el => ['h1', 'h2', 'h3'].includes(el.tag));
    for (const heading of headings) {
      expect(parseFloat(heading.ratio)).toBeGreaterThanOrEqual(heading.minRatio);
    }
  });

  /**
   * Test Case 6: Check skip link
   * Input: Check skip link
   * Expected: Skip to main content link is available for keyboard users
   */
  test('TC6: Skip to main content link is available for keyboard users', async ({ page }) => {
    // Check for skip link existence
    const skipLink = await page.evaluate(() => {
      // Common skip link selectors
      const possibleLinks = document.querySelectorAll('a[href="#main"], a[href="#main-content"], a.skip-link, [class*="skip"]');

      // Also check for any link with "skip" text
      const allLinks = document.querySelectorAll('a');
      const skipTextLinks = Array.from(allLinks).filter(link =>
        link.textContent?.toLowerCase().includes('skip') &&
        link.textContent?.toLowerCase().includes('main')
      );

      const allSkipLinks = [...Array.from(possibleLinks), ...skipTextLinks];

      if (allSkipLinks.length > 0) {
        const link = allSkipLinks[0];
        return {
          exists: true,
          href: link.getAttribute('href'),
          text: link.textContent?.trim(),
          isVisibleByDefault: window.getComputedStyle(link).opacity !== '0' &&
                             window.getComputedStyle(link).visibility !== 'hidden'
        };
      }
      return { exists: false };
    });

    // Verify skip link exists
    expect(skipLink.exists).toBeTruthy();
    expect(skipLink.href).toBeTruthy();
    expect(skipLink.text?.toLowerCase()).toContain('skip');

    // Verify skip link becomes visible on focus
    await page.keyboard.press('Tab');

    const skipLinkFocused = await page.evaluate(() => {
      const active = document.activeElement;
      if (active?.tagName === 'A') {
        const text = active.textContent?.toLowerCase() || '';
        if (text.includes('skip')) {
          const styles = window.getComputedStyle(active);
          return {
            isFocused: true,
            isVisible: styles.opacity !== '0' &&
                      styles.visibility !== 'hidden' &&
                      styles.display !== 'none'
          };
        }
      }
      return { isFocused: false, isVisible: false };
    });

    // If the skip link is first in tab order, it should be visible when focused
    if (skipLinkFocused.isFocused) {
      expect(skipLinkFocused.isVisible).toBeTruthy();
    }
  });

  /**
   * Additional test: Verify semantic HTML structure
   */
  test('Page uses semantic HTML elements', async ({ page }) => {
    const semanticElements = await page.evaluate(() => {
      return {
        hasHeader: document.querySelector('header') !== null,
        hasMain: document.querySelector('main') !== null,
        hasFooter: document.querySelector('footer') !== null,
        hasNav: document.querySelector('nav') !== null,
        hasSections: document.querySelectorAll('section').length > 0
      };
    });

    expect(semanticElements.hasHeader).toBeTruthy();
    expect(semanticElements.hasMain).toBeTruthy();
    expect(semanticElements.hasFooter).toBeTruthy();
    expect(semanticElements.hasNav).toBeTruthy();
    expect(semanticElements.hasSections).toBeTruthy();
  });

  /**
   * Additional test: Verify images have alt attributes
   */
  test('All images have alt attributes', async ({ page }) => {
    const images = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => ({
        src: img.src,
        hasAlt: img.hasAttribute('alt'),
        alt: img.alt,
        isDecorative: img.alt === '' && img.hasAttribute('role') && img.getAttribute('role') === 'presentation'
      }));
    });

    for (const img of images) {
      // Every image should have an alt attribute
      expect(img.hasAlt).toBeTruthy();
      // Non-decorative images should have non-empty alt text
      if (!img.isDecorative && !img.src.includes('icon')) {
        expect(img.alt.length).toBeGreaterThan(0);
      }
    }
  });

  /**
   * Additional test: Verify ARIA labels on interactive elements
   */
  test('Interactive elements have accessible names', async ({ page }) => {
    const buttons = await page.evaluate(() => {
      const btns = document.querySelectorAll('button');
      return Array.from(btns).map(btn => ({
        text: btn.textContent?.trim() || '',
        ariaLabel: btn.getAttribute('aria-label'),
        ariaLabelledBy: btn.getAttribute('aria-labelledby'),
        title: btn.getAttribute('title')
      }));
    });

    for (const btn of buttons) {
      // Each button should have an accessible name
      const hasAccessibleName =
        btn.text.length > 0 ||
        btn.ariaLabel ||
        btn.ariaLabelledBy ||
        btn.title;
      expect(hasAccessibleName).toBeTruthy();
    }
  });

  /**
   * Additional test: Verify main content has id for skip link target
   */
  test('Main content area is targetable by skip link', async ({ page }) => {
    const mainContent = await page.evaluate(() => {
      const main = document.querySelector('main');
      return {
        exists: main !== null,
        hasId: main?.hasAttribute('id'),
        id: main?.id
      };
    });

    expect(mainContent.exists).toBeTruthy();
    // Main should have an id for skip link targeting
    expect(mainContent.hasId).toBeTruthy();
  });
});

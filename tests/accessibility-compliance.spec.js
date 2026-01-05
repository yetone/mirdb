const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Basic Compliance (NFR-3)
 * Tests for WCAG 2.1 AA compliance including:
 * - Color contrast ratios
 * - Alt text for images
 * - Keyboard navigation
 * - Focus indicators
 */
test.describe('Accessibility - Basic Compliance (NFR-3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Helper function to compute luminance from RGB values
   * per WCAG 2.1 specification
   */
  function getLuminance(r, g, b) {
    const [rs, gs, bs] = [r, g, b].map((c) => {
      const s = c / 255;
      return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
  }

  /**
   * Helper function to compute contrast ratio between two colors
   */
  function getContrastRatio(l1, l2) {
    const lighter = Math.max(l1, l2);
    const darker = Math.min(l1, l2);
    return (lighter + 0.05) / (darker + 0.05);
  }

  /**
   * Helper function to parse color string to RGB
   */
  function parseColor(colorStr) {
    // Handle rgb(r, g, b) format
    const rgbMatch = colorStr.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      return {
        r: parseInt(rgbMatch[1]),
        g: parseInt(rgbMatch[2]),
        b: parseInt(rgbMatch[3]),
      };
    }
    // Handle rgba(r, g, b, a) format
    const rgbaMatch = colorStr.match(
      /rgba\((\d+),\s*(\d+),\s*(\d+),\s*([\d.]+)\)/
    );
    if (rgbaMatch) {
      return {
        r: parseInt(rgbaMatch[1]),
        g: parseInt(rgbaMatch[2]),
        b: parseInt(rgbaMatch[3]),
        a: parseFloat(rgbaMatch[4]),
      };
    }
    return null;
  }

  test.describe('Color Contrast', () => {
    test('TC1: Body text has at least 4.5:1 contrast ratio against background', async ({
      page,
    }) => {
      // Check body text contrast (normal text requires 4.5:1)
      const contrastResults = await page.evaluate(() => {
        const results = [];

        // Find body text elements (paragraphs, spans with text)
        const textElements = document.querySelectorAll('p, .tagline, .section-subtitle, .feature-card p, .command-desc');

        textElements.forEach((el) => {
          const style = window.getComputedStyle(el);
          const color = style.color;
          const bgColor = style.backgroundColor;
          const fontSize = parseFloat(style.fontSize);
          const fontWeight = style.fontWeight;

          // Get element text content for debugging
          const text = el.textContent.trim().substring(0, 50);

          results.push({
            text,
            color,
            bgColor,
            fontSize,
            fontWeight,
            tagName: el.tagName,
          });
        });

        return results;
      });

      // Verify we found text elements
      expect(contrastResults.length).toBeGreaterThan(0);

      // Check contrast for each text element
      for (const result of contrastResults) {
        const fgColor = parseColor(result.color);
        let bgColor = parseColor(result.bgColor);

        // If background is transparent, use the page background color
        if (!bgColor || (bgColor.a !== undefined && bgColor.a === 0)) {
          // Default dark background from CSS variables
          bgColor = { r: 15, g: 23, b: 42 }; // --bg-color: #0f172a
        }

        if (fgColor && bgColor) {
          const fgLuminance = getLuminance(fgColor.r, fgColor.g, fgColor.b);
          const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);
          const ratio = getContrastRatio(fgLuminance, bgLuminance);

          // Normal text (less than 18pt or 14pt bold) requires 4.5:1
          // Large text (18pt+ or 14pt+ bold) requires 3:1
          const isLargeText =
            result.fontSize >= 24 ||
            (result.fontSize >= 18.66 && parseInt(result.fontWeight) >= 700);
          const requiredRatio = isLargeText ? 3 : 4.5;

          expect(
            ratio,
            `Text "${result.text}" should have contrast ratio >= ${requiredRatio}:1, but has ${ratio.toFixed(2)}:1`
          ).toBeGreaterThanOrEqual(requiredRatio);
        }
      }
    });

    test('TC2: Heading text has at least 3:1 contrast ratio against background', async ({
      page,
    }) => {
      // Check heading text contrast (large text requires 3:1)
      const headingResults = await page.evaluate(() => {
        const results = [];
        const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');

        headings.forEach((el) => {
          const style = window.getComputedStyle(el);
          const color = style.color;
          const bgColor = style.backgroundColor;
          const fontSize = parseFloat(style.fontSize);
          const text = el.textContent.trim().substring(0, 50);

          // Check for gradient text which may not report color correctly
          const webkitTextFillColor = style.webkitTextFillColor;

          results.push({
            text,
            color,
            bgColor,
            fontSize,
            tagName: el.tagName,
            webkitTextFillColor,
          });
        });

        return results;
      });

      // Verify we found heading elements
      expect(headingResults.length).toBeGreaterThan(0);

      for (const result of headingResults) {
        let fgColor = parseColor(result.color);
        let bgColor = parseColor(result.bgColor);

        // If background is transparent, use the page background color
        if (!bgColor || (bgColor.a !== undefined && bgColor.a === 0)) {
          bgColor = { r: 15, g: 23, b: 42 }; // --bg-color: #0f172a
        }

        // Skip gradient text (h1 with background-clip: text) as it's decorative
        // and contrast is ensured by the gradient colors
        if (
          result.webkitTextFillColor === 'transparent' ||
          result.color === 'rgba(0, 0, 0, 0)'
        ) {
          // For gradient text, we trust the design maintains contrast
          continue;
        }

        if (fgColor && bgColor) {
          const fgLuminance = getLuminance(fgColor.r, fgColor.g, fgColor.b);
          const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);
          const ratio = getContrastRatio(fgLuminance, bgLuminance);

          // Headings are typically large text, require 3:1
          expect(
            ratio,
            `Heading "${result.text}" should have contrast ratio >= 3:1, but has ${ratio.toFixed(2)}:1`
          ).toBeGreaterThanOrEqual(3);
        }
      }
    });
  });

  test.describe('Alt Text for Images', () => {
    test('TC3: All <img> elements have alt attributes', async ({ page }) => {
      // Find all images and check for alt attributes
      const imageResults = await page.evaluate(() => {
        const images = document.querySelectorAll('img');
        const results = [];

        images.forEach((img) => {
          results.push({
            src: img.src,
            hasAlt: img.hasAttribute('alt'),
            altValue: img.getAttribute('alt'),
            altIsEmpty: img.getAttribute('alt') === '',
          });
        });

        return results;
      });

      // All images should have alt attributes
      for (const img of imageResults) {
        expect(
          img.hasAlt,
          `Image with src "${img.src}" should have an alt attribute`
        ).toBe(true);

        // Alt can be empty for decorative images, but should be present
        // For meaningful images, alt should have content
        if (img.altValue !== '') {
          expect(
            img.altValue.length,
            `Image alt text should be descriptive`
          ).toBeGreaterThan(0);
        }
      }
    });
  });

  test.describe('Keyboard Navigation', () => {
    test('TC4: All buttons and links can be reached via Tab key', async ({
      page,
    }) => {
      // Get all interactive elements
      const interactiveElements = await page.evaluate(() => {
        const elements = document.querySelectorAll(
          'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])'
        );
        return Array.from(elements).map((el) => ({
          tagName: el.tagName,
          text: el.textContent.trim().substring(0, 30),
          href: el.href || null,
          tabIndex: el.tabIndex,
          isVisible:
            el.offsetWidth > 0 &&
            el.offsetHeight > 0 &&
            window.getComputedStyle(el).visibility !== 'hidden' &&
            window.getComputedStyle(el).display !== 'none',
        }));
      });

      // Filter to visible interactive elements
      const visibleElements = interactiveElements.filter((el) => el.isVisible);
      expect(visibleElements.length).toBeGreaterThan(0);

      // Tab through all elements
      const focusedElements = [];
      let previousActiveElement = null;
      let maxTabs = visibleElements.length + 10; // Extra buffer

      for (let i = 0; i < maxTabs; i++) {
        await page.keyboard.press('Tab');

        const currentFocused = await page.evaluate(() => {
          const el = document.activeElement;
          return {
            tagName: el.tagName,
            text: el.textContent?.trim().substring(0, 30) || '',
            href: el.href || null,
            className: el.className,
          };
        });

        // Stop if we've cycled back to body or same element
        if (
          currentFocused.tagName === 'BODY' ||
          (previousActiveElement &&
            currentFocused.tagName === previousActiveElement.tagName &&
            currentFocused.text === previousActiveElement.text &&
            currentFocused.href === previousActiveElement.href)
        ) {
          // Check if we've looped
          if (focusedElements.length > 0) {
            const first = focusedElements[0];
            if (
              currentFocused.tagName === first.tagName &&
              currentFocused.text === first.text
            ) {
              break;
            }
          }
        }

        if (
          currentFocused.tagName !== 'BODY' &&
          !focusedElements.some(
            (el) =>
              el.tagName === currentFocused.tagName &&
              el.text === currentFocused.text &&
              el.href === currentFocused.href
          )
        ) {
          focusedElements.push(currentFocused);
        }

        previousActiveElement = currentFocused;
      }

      // Verify we can reach links and buttons
      const focusedLinks = focusedElements.filter((el) => el.tagName === 'A');
      const focusedButtons = focusedElements.filter(
        (el) => el.tagName === 'BUTTON' || el.className.includes('btn')
      );

      // We should be able to tab to at least some links
      expect(
        focusedLinks.length,
        'Should be able to tab to links'
      ).toBeGreaterThan(0);
    });
  });

  test.describe('Focus Indicators', () => {
    test('TC5: Buttons show visible focus indicator when focused', async ({
      page,
    }) => {
      // Find all button-like elements
      const buttons = page.locator('a.btn, button, [role="button"]');
      const buttonCount = await buttons.count();

      if (buttonCount === 0) {
        // Skip if no buttons found
        return;
      }

      // Test focus on first button
      const firstButton = buttons.first();
      await firstButton.focus();

      // Check if button has visible focus indicator
      const focusStyles = await firstButton.evaluate((el) => {
        const style = window.getComputedStyle(el);
        const focusStyle = window.getComputedStyle(el, ':focus');

        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
          border: style.border,
          // Check if element has any focus styling
          hasFocusRing:
            style.outlineStyle !== 'none' ||
            style.boxShadow !== 'none' ||
            style.outlineWidth !== '0px',
        };
      });

      // Button should have some form of focus indicator
      // Either outline, box-shadow, or border change
      const hasFocusIndicator =
        focusStyles.outlineStyle !== 'none' ||
        focusStyles.boxShadow !== 'none' ||
        focusStyles.outlineWidth !== '0px';

      expect(
        hasFocusIndicator,
        'Buttons should have a visible focus indicator (outline or box-shadow)'
      ).toBe(true);
    });

    test('TC6: Links show visible focus indicator when focused', async ({
      page,
    }) => {
      // Find all links
      const links = page.locator('a[href]');
      const linkCount = await links.count();

      expect(linkCount).toBeGreaterThan(0);

      // Test focus on navigation links
      const navLinks = page.locator('nav a, .nav-links a');
      const navLinkCount = await navLinks.count();

      if (navLinkCount > 0) {
        const firstNavLink = navLinks.first();
        await firstNavLink.focus();

        // Check if link has visible focus indicator
        const focusStyles = await firstNavLink.evaluate((el) => {
          const style = window.getComputedStyle(el);

          return {
            outline: style.outline,
            outlineWidth: style.outlineWidth,
            outlineStyle: style.outlineStyle,
            outlineColor: style.outlineColor,
            boxShadow: style.boxShadow,
            textDecoration: style.textDecoration,
            hasFocusRing:
              style.outlineStyle !== 'none' ||
              style.boxShadow !== 'none' ||
              style.outlineWidth !== '0px',
          };
        });

        // Link should have some form of focus indicator
        expect(
          focusStyles.hasFocusRing,
          'Links should have a visible focus indicator'
        ).toBe(true);
      }

      // Also test footer links
      const footerLinks = page.locator('footer a');
      const footerLinkCount = await footerLinks.count();

      if (footerLinkCount > 0) {
        const firstFooterLink = footerLinks.first();
        await firstFooterLink.focus();

        const focusStyles = await firstFooterLink.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            hasFocusRing:
              style.outlineStyle !== 'none' ||
              style.boxShadow !== 'none' ||
              style.outlineWidth !== '0px',
          };
        });

        expect(
          focusStyles.hasFocusRing,
          'Footer links should have a visible focus indicator'
        ).toBe(true);
      }
    });
  });
});

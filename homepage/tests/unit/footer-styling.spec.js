// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Footer Section Unit Tests - Styling and Layout
 *
 * REQ-7: Include footer with project links, license information, and contact details
 *
 * Test Case 5: Verify footer is consistently styled
 * Expected: Footer has consistent styling with rest of page design
 */

test.describe('Footer Styling Unit Tests (Scenario 7 - Test Case 5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 5a: Footer uses consistent color scheme
   */
  test('TC5a: Footer uses consistent color scheme with page design', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Footer should have dark background (consistent with text-color variable)
    const bgColor = await footer.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Dark background color (#333 = rgb(51, 51, 51))
    expect(bgColor).toMatch(/rgb\(51,\s*51,\s*51\)|#333/i);

    // Footer text should be white
    const textColor = await footer.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // White text (#fff = rgb(255, 255, 255))
    expect(textColor).toMatch(/rgb\(255,\s*255,\s*255\)|#fff/i);
  });

  /**
   * Test Case 5b: Footer links have consistent styling
   */
  test('TC5b: Footer links have consistent styling', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    const navLinks = footer.locator('[data-testid="footer-nav"] a');
    const linkCount = await navLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);

      // Links should have white color
      const color = await link.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(color).toMatch(/rgb\(255,\s*255,\s*255\)/);

      // Links should have no initial text decoration
      const textDecoration = await link.evaluate((el) => {
        return window.getComputedStyle(el).textDecorationLine;
      });
      expect(textDecoration).toBe('none');

      // Links should have transition for smooth hover effects
      const transition = await link.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });
      expect(transition).not.toBe('none');
      expect(transition.length).toBeGreaterThan(0);
    }
  });

  /**
   * Test Case 5c: Footer has proper spacing and layout
   */
  test('TC5c: Footer has proper spacing and layout', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Footer should have padding
    const padding = await footer.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        top: style.paddingTop,
        bottom: style.paddingBottom,
      };
    });
    expect(parseInt(padding.top)).toBeGreaterThan(0);
    expect(parseInt(padding.bottom)).toBeGreaterThan(0);

    // Footer should be centered
    const textAlign = await footer.evaluate((el) => {
      return window.getComputedStyle(el).textAlign;
    });
    expect(textAlign).toBe('center');
  });

  /**
   * Test Case 5d: Footer badges are properly styled
   */
  test('TC5d: Footer badges are properly styled and aligned', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    const badgesContainer = page.locator('[data-testid="footer-badges"]');
    await expect(badgesContainer).toBeVisible();

    // Badges container should use flexbox for layout
    const display = await badgesContainer.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('flex');

    // Badges should be centered
    const justifyContent = await badgesContainer.evaluate((el) => {
      return window.getComputedStyle(el).justifyContent;
    });
    expect(justifyContent).toBe('center');

    // Badges should have gap between them
    const gap = await badgesContainer.evaluate((el) => {
      return window.getComputedStyle(el).gap;
    });
    expect(gap).not.toBe('0px');
    expect(gap).not.toBe('normal');
  });

  /**
   * Test Case 5e: Footer navigation has proper spacing between links
   */
  test('TC5e: Footer navigation has proper spacing between links', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    const navLinks = footer.locator('[data-testid="footer-nav"] a');
    const linkCount = await navLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);

      // Links should have horizontal margin for spacing
      const margin = await link.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          left: style.marginLeft,
          right: style.marginRight,
        };
      });
      expect(parseInt(margin.left)).toBeGreaterThan(0);
      expect(parseInt(margin.right)).toBeGreaterThan(0);
    }
  });

  /**
   * Test Case 5f: Footer copyright text has appropriate styling
   */
  test('TC5f: Footer copyright text has appropriate styling', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    const copyright = page.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();

    // Copyright should have slightly reduced opacity or smaller font
    const fontSize = await copyright.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    // Should be smaller than standard body text (typically 16px)
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeLessThanOrEqual(16);
  });

  /**
   * Test Case 5g: Footer is consistent with overall page typography
   */
  test('TC5g: Footer uses consistent font family with page', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    const body = page.locator('body');

    // Get body font family
    const bodyFontFamily = await body.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Get footer font family
    const footerFontFamily = await footer.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Footer should inherit or use same font family as body
    expect(footerFontFamily).toBe(bodyFontFamily);
  });

  /**
   * Test Case 5h: Footer badges have proper image dimensions
   */
  test('TC5h: Footer badges have proper image dimensions', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Wait for badge images to load
    await page.waitForLoadState('networkidle');

    const statusBadge = page.locator('[data-testid="status-badge-img"]');
    const licenseBadge = page.locator('[data-testid="license-badge-img"]');

    // Badges should have consistent height styling
    const statusHeight = await statusBadge.evaluate((el) => {
      return window.getComputedStyle(el).height;
    });
    const licenseHeight = await licenseBadge.evaluate((el) => {
      return window.getComputedStyle(el).height;
    });

    // Both badges should have the same height
    expect(statusHeight).toBe(licenseHeight);
  });

  /**
   * Test Case 5i: Footer has proper vertical alignment
   */
  test('TC5i: Footer elements are vertically aligned properly', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Container should be within footer
    const container = footer.locator('.container');
    await expect(container).toBeVisible();

    // Footer nav should have margin bottom
    const footerNav = page.locator('[data-testid="footer-nav"]');
    const navMarginBottom = await footerNav.evaluate((el) => {
      return window.getComputedStyle(el).marginBottom;
    });
    expect(parseInt(navMarginBottom)).toBeGreaterThan(0);
  });

  /**
   * Test Case 5j: Footer links have visible focus states for accessibility
   */
  test('TC5j: Footer links have visible focus states', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    const githubLink = page.locator('[data-testid="footer-github-link"]');

    // Focus the link
    await githubLink.focus();

    // Check that there's an outline or visible focus indicator
    const outline = await githubLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineStyle: style.outlineStyle,
      };
    });

    // Should have visible focus outline
    expect(outline.outlineWidth).not.toBe('0px');
    expect(outline.outlineStyle).not.toBe('none');
  });
});

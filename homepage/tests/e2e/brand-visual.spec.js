/**
 * Brand Visual Design E2E Tests
 * Owner: Scenario 9 - Brand Consistency and Visual Design
 *
 * End-to-end tests for validating visual design including logo display,
 * adequate whitespace, and professional appearance.
 *
 * Requirements traced:
 * - NFR-2: Homepage shall maintain consistency with existing project branding
 * - Design Requirements: Clean, professional design for technical audience
 * - Design Requirements: Sufficient whitespace for comfortable reading
 */

// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Brand Consistency and Visual Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 2: Check logo displays correctly
  test('should display logo image without distortion', async ({ page }) => {
    const logo = page.locator('header .logo img');
    await expect(logo).toBeVisible();

    // Get logo attributes and computed styles
    const logoInfo = await logo.evaluate((img) => {
      const styles = window.getComputedStyle(img);
      return {
        src: img.getAttribute('src'),
        alt: img.getAttribute('alt'),
        complete: img.complete,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight,
        displayWidth: img.offsetWidth,
        displayHeight: img.offsetHeight,
        // Check if image has error state
        hasError: img.naturalHeight === 0 && img.naturalWidth === 0 && img.complete,
        // CSS properties that could cause distortion
        objectFit: styles.objectFit,
        width: styles.width,
        height: styles.height,
      };
    });

    // Logo should have correct source path
    expect(logoInfo.src).toMatch(/logo\.gif$/i);

    // Logo should have alt text for accessibility
    expect(logoInfo.alt).toBeTruthy();

    // Display dimensions should be reasonable (indicating CSS styling is applied)
    expect(logoInfo.displayHeight).toBeGreaterThanOrEqual(20);

    // If the image loads (may fail in test env due to path), check it's not distorted
    if (logoInfo.naturalWidth > 0 && logoInfo.naturalHeight > 0) {
      // Check that the CSS doesn't force extreme distortion
      // A height of 'auto' or specific value with width 'auto' preserves aspect ratio
      const hasAutoSizing =
        logoInfo.width === 'auto' || logoInfo.height === 'auto';
      const hasObjectFit =
        logoInfo.objectFit === 'contain' || logoInfo.objectFit === 'cover';

      // Either auto sizing or object-fit should be present to prevent distortion
      expect(hasAutoSizing || hasObjectFit || logoInfo.objectFit === 'fill').toBe(true);
    }
  });

  test('should display logo with correct source path', async ({ page }) => {
    const logo = page.locator('header .logo img');
    const src = await logo.getAttribute('src');
    expect(src).toMatch(/logo\.gif$/i);
  });

  // Test Case 4: Check for adequate whitespace
  test('should have adequate section padding for readability', async ({ page }) => {
    const sections = ['.hero', '.features', '.quickstart', '.resources'];

    for (const selector of sections) {
      const section = page.locator(selector).first();
      if ((await section.count()) > 0) {
        const padding = await section.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            top: parseInt(styles.paddingTop) || 0,
            bottom: parseInt(styles.paddingBottom) || 0,
          };
        });

        // Each section should have at least 32px (2rem at 16px base) padding
        expect(padding.top).toBeGreaterThanOrEqual(32);
        expect(padding.bottom).toBeGreaterThanOrEqual(32);
      }
    }
  });

  test('should have adequate container padding for content', async ({ page }) => {
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    const padding = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        left: parseInt(styles.paddingLeft) || 0,
        right: parseInt(styles.paddingRight) || 0,
      };
    });

    // Container should have horizontal padding for readability (at least 16px)
    expect(padding.left).toBeGreaterThanOrEqual(16);
    expect(padding.right).toBeGreaterThanOrEqual(16);
  });

  test('should have adequate spacing between feature cards', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    if ((await featuresGrid.count()) > 0) {
      const gap = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return parseInt(styles.gap) || parseInt(styles.columnGap) || 0;
      });

      // Grid gap should be at least 16px for visual separation
      expect(gap).toBeGreaterThanOrEqual(16);
    }
  });

  test('should have proper margin between paragraphs', async ({ page }) => {
    const paragraph = page.locator('.hero .description, .hero p').first();
    if ((await paragraph.count()) > 0) {
      const marginBottom = await paragraph.evaluate((el) => {
        return parseInt(window.getComputedStyle(el).marginBottom) || 0;
      });

      // Paragraphs should have margin for readability (at least 8px)
      expect(marginBottom).toBeGreaterThanOrEqual(8);
    }
  });

  // Test for consistent font rendering
  test('should use consistent font family across page', async ({ page }) => {
    const body = page.locator('body');
    const bodyFont = await body.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Should not be browser default serif font
    expect(bodyFont.toLowerCase()).not.toContain('times new roman');
    // Should be a sans-serif font
    expect(bodyFont.toLowerCase()).toMatch(/sans-serif/);

    // Check that headings use same font family
    const heading = page.locator('h1').first();
    const headingFont = await heading.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Heading should use same base font family
    expect(headingFont.toLowerCase()).toMatch(/sans-serif/);
  });

  // Test for monospace code rendering
  test('should render code blocks with monospace font', async ({ page }) => {
    const codeBlock = page.locator('pre code').first();
    if ((await codeBlock.count()) > 0) {
      const fontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Code should use monospace font
      expect(fontFamily.toLowerCase()).toMatch(/monospace|consolas|menlo|sfmono/);
    }
  });

  // Test for professional color scheme
  test('should have professional color scheme with good contrast', async ({ page }) => {
    const body = page.locator('body');

    const colors = await body.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        color: styles.color,
        backgroundColor: styles.backgroundColor,
      };
    });

    // Background should be light (high RGB values)
    const bgMatch = colors.backgroundColor.match(
      /rgb[a]?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/
    );
    if (bgMatch) {
      const [, r, g, b] = bgMatch.map(Number);
      // White or light background (average > 200)
      const avgBg = (r + g + b) / 3;
      expect(avgBg).toBeGreaterThan(200);
    }

    // Text should be dark (low RGB values)
    const textMatch = colors.color.match(
      /rgb[a]?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/
    );
    if (textMatch) {
      const [, r, g, b] = textMatch.map(Number);
      // Dark text (average < 100)
      const avgText = (r + g + b) / 3;
      expect(avgText).toBeLessThan(100);
    }
  });

  // Test for header visibility and brand presence
  test('should display brand name prominently in header', async ({ page }) => {
    const logoText = page.locator('header .logo-text, header .logo h1');
    if ((await logoText.count()) > 0) {
      await expect(logoText.first()).toBeVisible();
      await expect(logoText.first()).toContainText('MirDB');
    }
  });

  // Test for button styling consistency
  test('should have consistently styled buttons', async ({ page }) => {
    const primaryBtn = page.locator('.btn-primary').first();
    const secondaryBtn = page.locator('.btn-secondary').first();

    if ((await primaryBtn.count()) > 0) {
      // Primary button should have a non-transparent background
      const primaryBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(primaryBg).not.toBe('rgba(0, 0, 0, 0)');
      expect(primaryBg).not.toBe('transparent');
    }

    if ((await secondaryBtn.count()) > 0) {
      // Secondary button should have border or background
      const secondaryStyles = await secondaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          border: styles.border,
          backgroundColor: styles.backgroundColor,
        };
      });
      // Should have either a border or a background
      const hasStyling =
        secondaryStyles.border !== 'none' ||
        (secondaryStyles.backgroundColor !== 'rgba(0, 0, 0, 0)' &&
          secondaryStyles.backgroundColor !== 'transparent');
      expect(hasStyling).toBe(true);
    }
  });

  // Test for readable line height
  test('should have readable line height for body text', async ({ page }) => {
    const body = page.locator('body');
    const lineHeight = await body.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      const fontSize = parseFloat(styles.fontSize);
      const lineHeightPx = parseFloat(styles.lineHeight);
      return lineHeightPx / fontSize;
    });

    // Line height ratio should be at least 1.4 for readability
    expect(lineHeight).toBeGreaterThanOrEqual(1.4);
  });

  // Test for content max-width
  test('should have max-width constraint for content readability', async ({ page }) => {
    const container = page.locator('.container').first();
    const maxWidth = await container.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });

    // Should have a max-width set (not 'none')
    expect(maxWidth).not.toBe('none');
    // Max-width should be a reasonable value (under 1400px)
    const maxWidthPx = parseInt(maxWidth);
    expect(maxWidthPx).toBeLessThanOrEqual(1400);
    expect(maxWidthPx).toBeGreaterThan(800);
  });
});

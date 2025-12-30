// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Developer Tool Aesthetics Tests (NFR-5)
 * Verifies that design aligns with developer tool aesthetics:
 * - Clean, technical, professional appearance
 * - Consistent typography (monospace for code, sans-serif for content)
 * - Appropriate code block styling with syntax highlighting
 * - Consistent spacing and layout
 * - Limited, cohesive color palette
 */

test.describe('Developer Tool Aesthetics (NFR-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Typography Consistency', () => {
    test('TC1: Consistent font family usage - body uses sans-serif', async ({ page }) => {
      // Test Case 1: Verify consistent font family usage (monospace for code, sans-serif for content)
      // Expected: Code uses monospace font, body text uses clean sans-serif

      // Check body text uses sans-serif font family
      const body = page.locator('body');
      const bodyFontFamily = await body.evaluate(
        (el) => window.getComputedStyle(el).fontFamily
      );

      // Body should use system UI sans-serif stack
      expect(bodyFontFamily.toLowerCase()).toMatch(
        /apple|segoe|roboto|helvetica|arial|sans-serif/i
      );
    });

    test('TC1: Consistent font family usage - code uses monospace', async ({ page }) => {
      // Verify code elements use monospace fonts
      const codeElements = page.locator('code');
      const codeCount = await codeElements.count();
      expect(codeCount).toBeGreaterThan(0);

      // Check multiple code elements to ensure consistency
      for (let i = 0; i < Math.min(codeCount, 5); i++) {
        const codeElement = codeElements.nth(i);
        const fontFamily = await codeElement.evaluate(
          (el) => window.getComputedStyle(el).fontFamily
        );

        // Code should use monospace font
        expect(fontFamily.toLowerCase()).toMatch(
          /mono|consolas|courier|menlo|sf mono|fira code|monaco/i
        );
      }
    });

    test('TC1: Consistent font family usage - pre elements use monospace', async ({ page }) => {
      // Verify pre elements (code blocks) use monospace fonts
      const preElements = page.locator('pre');
      const preCount = await preElements.count();

      if (preCount > 0) {
        for (let i = 0; i < Math.min(preCount, 3); i++) {
          const preElement = preElements.nth(i);
          const codeInPre = preElement.locator('code');

          if (await codeInPre.count() > 0) {
            const fontFamily = await codeInPre.first().evaluate(
              (el) => window.getComputedStyle(el).fontFamily
            );
            expect(fontFamily.toLowerCase()).toMatch(
              /mono|consolas|courier|menlo|sf mono|fira code|monaco/i
            );
          }
        }
      }
    });

    test('TC1: Headings use consistent sans-serif font', async ({ page }) => {
      // Verify headings also use sans-serif fonts
      const headings = page.locator('h1, h2, h3, h4, h5, h6');
      const headingCount = await headings.count();
      expect(headingCount).toBeGreaterThan(0);

      for (let i = 0; i < Math.min(headingCount, 5); i++) {
        const heading = headings.nth(i);
        const fontFamily = await heading.evaluate(
          (el) => window.getComputedStyle(el).fontFamily
        );

        // Headings should use same sans-serif family as body
        expect(fontFamily.toLowerCase()).toMatch(
          /apple|segoe|roboto|helvetica|arial|sans-serif/i
        );
      }
    });
  });

  test.describe('Code Block Styling', () => {
    test('TC2: Code blocks have dark background for technical appearance', async ({ page }) => {
      // Test Case 2: Verify code blocks have syntax highlighting
      // Expected: Code blocks display with appropriate syntax highlighting colors

      const codeBlocks = page.locator('.code-block, pre');
      const blockCount = await codeBlocks.count();
      expect(blockCount).toBeGreaterThan(0);

      // Check code block styling
      const codeBlock = codeBlocks.first();
      const backgroundColor = await codeBlock.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.backgroundColor;
      });

      // Parse the RGB values - dark background means lower R, G, B values
      const rgbMatch = backgroundColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      expect(rgbMatch).not.toBeNull();

      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // Dark backgrounds typically have RGB values < 100
        const averageBrightness = (r + g + b) / 3;
        expect(averageBrightness).toBeLessThan(100);
      }
    });

    test('TC2: Code blocks have proper structure for syntax highlighting', async ({ page }) => {
      // Code blocks should have proper pre/code structure to support syntax highlighting
      const quickStartSection = page.locator('#quick-start, .quick-start');
      await expect(quickStartSection).toBeVisible();

      const preCodeBlocks = quickStartSection.locator('pre code');
      const count = await preCodeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // Verify code has content
      const codeContent = await preCodeBlocks.first().textContent();
      expect(codeContent).toBeTruthy();
      expect(codeContent.length).toBeGreaterThan(10);
    });

    test('TC2: Code text has appropriate color for readability', async ({ page }) => {
      // Code text should have light/readable color on dark background
      const codeElement = page.locator('.code-block code, pre code').first();
      await expect(codeElement).toBeVisible();

      const textColor = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Parse the RGB values - text on dark background should be light
      const rgbMatch = textColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      expect(rgbMatch).not.toBeNull();

      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // Light text typically has RGB values > 150
        const averageBrightness = (r + g + b) / 3;
        expect(averageBrightness).toBeGreaterThan(150);
      }
    });

    test('TC2: Code blocks have border styling', async ({ page }) => {
      // Developer tools often have subtle borders on code blocks
      const codeBlock = page.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      const borderStyle = await codeBlock.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          borderStyle: style.borderStyle,
          borderWidth: style.borderWidth,
          borderRadius: style.borderRadius
        };
      });

      // Should have border or at least rounded corners
      const hasBorder = borderStyle.borderStyle !== 'none' ||
        borderStyle.borderRadius !== '0px';
      expect(hasBorder).toBe(true);
    });
  });

  test.describe('Section Spacing Consistency', () => {
    test('TC3: Sections have consistent vertical spacing', async ({ page }) => {
      // Test Case 3: Verify consistent spacing between sections
      // Expected: Sections have consistent vertical spacing/padding

      const sections = page.locator('section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThan(2);

      // Collect padding values from all sections
      const paddingValues = [];
      for (let i = 0; i < sectionCount; i++) {
        const section = sections.nth(i);
        const padding = await section.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            top: parseFloat(style.paddingTop),
            bottom: parseFloat(style.paddingBottom)
          };
        });
        paddingValues.push(padding);
      }

      // Verify padding is consistent (allow some variance for special sections)
      // Most sections should have similar padding
      const topPaddings = paddingValues.map(p => p.top);
      const bottomPaddings = paddingValues.map(p => p.bottom);

      // All sections should have some padding
      for (const p of paddingValues) {
        expect(p.top).toBeGreaterThan(0);
        expect(p.bottom).toBeGreaterThan(0);
      }

      // Check that padding values are relatively consistent (within 50% variance)
      const avgTop = topPaddings.reduce((a, b) => a + b, 0) / topPaddings.length;
      for (const top of topPaddings) {
        expect(top).toBeGreaterThanOrEqual(avgTop * 0.5);
        expect(top).toBeLessThanOrEqual(avgTop * 1.5);
      }
    });

    test('TC3: Hero section has appropriate spacing', async ({ page }) => {
      const hero = page.locator('.hero, [data-testid="hero"]').first();
      await expect(hero).toBeVisible();

      const spacing = await hero.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          padding: style.padding,
          minHeight: style.minHeight
        };
      });

      // Hero should have minimum padding for proper spacing
      expect(spacing.padding).not.toBe('0px');
    });

    test('TC3: Feature cards have consistent gap spacing', async ({ page }) => {
      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      const gap = await featureGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gap || style.gridGap;
      });

      // Grid should have gap defined
      expect(gap).toBeTruthy();
      expect(gap).not.toBe('normal');
      expect(gap).not.toBe('0px');
    });

    test('TC3: Content has max-width constraints for readability', async ({ page }) => {
      // Professional developer tools constrain content width for readability
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();

      const maxWidth = await heroContent.evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      // Hero content should have max-width constraint
      expect(maxWidth).not.toBe('none');
    });
  });

  test.describe('Color Palette Professionalism', () => {
    test('TC4: Background uses dark, professional color', async ({ page }) => {
      // Test Case 4: Verify color palette is limited and professional
      // Expected: Design uses a cohesive, limited color palette appropriate for dev tools

      const body = page.locator('body');
      const bgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Parse RGB values
      const rgbMatch = bgColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      expect(rgbMatch).not.toBeNull();

      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // Developer tools typically have dark backgrounds (low RGB values)
        const averageBrightness = (r + g + b) / 3;
        expect(averageBrightness).toBeLessThan(100);
      }
    });

    test('TC4: CSS custom properties define cohesive color palette', async ({ page }) => {
      // Check that CSS variables are being used for consistent colors
      const root = page.locator(':root');

      // Get computed CSS custom property values
      const cssVars = await page.evaluate(() => {
        const html = document.documentElement;
        const style = getComputedStyle(html);
        return {
          colorPrimary: style.getPropertyValue('--color-primary').trim(),
          colorBackground: style.getPropertyValue('--color-background').trim(),
          colorSurface: style.getPropertyValue('--color-surface').trim(),
          colorText: style.getPropertyValue('--color-text').trim(),
          colorAccent: style.getPropertyValue('--color-accent').trim()
        };
      });

      // Verify core color variables are defined
      expect(cssVars.colorPrimary).toBeTruthy();
      expect(cssVars.colorBackground).toBeTruthy();
      expect(cssVars.colorText).toBeTruthy();
    });

    test('TC4: Text colors provide good contrast', async ({ page }) => {
      // Verify text is readable against backgrounds
      const body = page.locator('body');

      const colors = await body.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          textColor: style.color,
          bgColor: style.backgroundColor
        };
      });

      // Parse both colors
      const textRgb = colors.textColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      const bgRgb = colors.bgColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);

      expect(textRgb).not.toBeNull();
      expect(bgRgb).not.toBeNull();

      if (textRgb && bgRgb) {
        const textBrightness = (parseInt(textRgb[1]) + parseInt(textRgb[2]) + parseInt(textRgb[3])) / 3;
        const bgBrightness = (parseInt(bgRgb[1]) + parseInt(bgRgb[2]) + parseInt(bgRgb[3])) / 3;

        // There should be significant contrast between text and background
        const contrastDiff = Math.abs(textBrightness - bgBrightness);
        expect(contrastDiff).toBeGreaterThan(100);
      }
    });

    test('TC4: Accent color used consistently for highlights', async ({ page }) => {
      // Check that accent colors are used for important elements
      const tagline = page.locator('.tagline').first();
      await expect(tagline).toBeVisible();

      const taglineColor = await tagline.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      const featureHeading = page.locator('.feature-card h3').first();
      await expect(featureHeading).toBeVisible();

      const featureColor = await featureHeading.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Both should use the accent color (cyan tones in this design)
      // Check they're similar colors (both using accent)
      expect(taglineColor).toBeTruthy();
      expect(featureColor).toBeTruthy();

      // They should both have non-white, non-gray tones indicating accent use
      const taglineRgb = taglineColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      const featureRgb = featureColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);

      if (taglineRgb && featureRgb) {
        // Accent colors typically have more saturation (color difference between RGB)
        const taglineSaturation = Math.max(
          Math.abs(parseInt(taglineRgb[1]) - parseInt(taglineRgb[2])),
          Math.abs(parseInt(taglineRgb[2]) - parseInt(taglineRgb[3])),
          Math.abs(parseInt(taglineRgb[1]) - parseInt(taglineRgb[3]))
        );
        expect(taglineSaturation).toBeGreaterThan(0);
      }
    });

    test('TC4: Primary action button uses consistent brand color', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      const btnStyle = await primaryBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          backgroundColor: style.backgroundColor,
          color: style.color
        };
      });

      // Button should have visible background color
      expect(btnStyle.backgroundColor).toBeTruthy();
      expect(btnStyle.backgroundColor).not.toBe('transparent');
      expect(btnStyle.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

      // Button text should be light/white on colored background
      const textRgb = btnStyle.color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      if (textRgb) {
        const brightness = (parseInt(textRgb[1]) + parseInt(textRgb[2]) + parseInt(textRgb[3])) / 3;
        expect(brightness).toBeGreaterThan(200); // Light/white text
      }
    });
  });

  test.describe('Professional Visual Details', () => {
    test('Buttons have rounded corners (modern developer tool style)', async ({ page }) => {
      const buttons = page.locator('.btn');
      const buttonCount = await buttons.count();
      expect(buttonCount).toBeGreaterThan(0);

      const borderRadius = await buttons.first().evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });

      // Modern dev tools use rounded corners
      expect(borderRadius).not.toBe('0px');
      expect(parseInt(borderRadius)).toBeGreaterThan(0);
    });

    test('Cards have subtle borders and rounded corners', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();
      await expect(featureCard).toBeVisible();

      const cardStyle = await featureCard.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          borderRadius: style.borderRadius,
          border: style.border,
          borderColor: style.borderColor
        };
      });

      // Should have rounded corners
      expect(cardStyle.borderRadius).not.toBe('0px');

      // Should have subtle border
      expect(cardStyle.border).toBeTruthy();
    });

    test('Transitions are used for smooth interactions', async ({ page }) => {
      const btn = page.locator('.btn').first();
      await expect(btn).toBeVisible();

      const transition = await btn.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });

      // Professional dev tools have smooth transitions
      expect(transition).toBeTruthy();
      expect(transition).not.toBe('none');
      expect(transition).not.toBe('all 0s ease 0s');
    });

    test('Page has consistent line-height for readability', async ({ page }) => {
      const body = page.locator('body');
      const lineHeight = await body.evaluate((el) => {
        return window.getComputedStyle(el).lineHeight;
      });

      // Line height should be set for readability (1.5-1.8 typical)
      // Computed value might be 'normal' or a pixel value
      expect(lineHeight).toBeTruthy();

      if (lineHeight !== 'normal') {
        const lhValue = parseFloat(lineHeight);
        expect(lhValue).toBeGreaterThan(0);
      }
    });
  });
});

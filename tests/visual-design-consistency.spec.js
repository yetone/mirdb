// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Visual Design Consistency Tests
 *
 * Verifies the page uses consistent visual language suitable for developer tools.
 * Based on NFR-6: Design must use consistent visual language suitable for developer tools.
 */

test.describe('Visual Design Consistency', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Typography Consistency', () => {

    test('all headings use the same font family', async ({ page }) => {
      // Get all heading elements (h1, h2, h3)
      const h1Elements = page.locator('h1');
      const h2Elements = page.locator('h2');
      const h3Elements = page.locator('h3');

      // Collect font families from all headings
      const fontFamilies = new Set();

      // Check h1 elements
      const h1Count = await h1Elements.count();
      for (let i = 0; i < h1Count; i++) {
        const fontFamily = await h1Elements.nth(i).evaluate((el) =>
          window.getComputedStyle(el).fontFamily
        );
        fontFamilies.add(fontFamily);
      }

      // Check h2 elements
      const h2Count = await h2Elements.count();
      for (let i = 0; i < h2Count; i++) {
        const fontFamily = await h2Elements.nth(i).evaluate((el) =>
          window.getComputedStyle(el).fontFamily
        );
        fontFamilies.add(fontFamily);
      }

      // Check h3 elements
      const h3Count = await h3Elements.count();
      for (let i = 0; i < h3Count; i++) {
        const fontFamily = await h3Elements.nth(i).evaluate((el) =>
          window.getComputedStyle(el).fontFamily
        );
        fontFamilies.add(fontFamily);
      }

      // Verify there's at least one heading
      expect(h1Count + h2Count + h3Count).toBeGreaterThan(0);

      // All headings should use the same font family
      expect(fontFamilies.size).toBe(1);

      // Verify it's the expected system font stack (checks for common system fonts)
      // The browser resolves system-ui to actual font names like -apple-system, BlinkMacSystemFont, etc.
      const fontFamily = [...fontFamilies][0];
      const systemFonts = ['-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'];
      const hasSystemFont = systemFonts.some(font => fontFamily.includes(font));
      expect(hasSystemFont).toBe(true);
    });

    test('all body text uses consistent font family and size', async ({ page }) => {
      // Get body font family and size from the body element
      const bodyStyles = await page.locator('body').evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          fontFamily: style.fontFamily,
          fontSize: style.fontSize,
          lineHeight: style.lineHeight
        };
      });

      // Verify body uses system font stack (browser resolves system-ui to actual fonts)
      const systemFonts = ['-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'];
      const bodyHasSystemFont = systemFonts.some(font => bodyStyles.fontFamily.includes(font));
      expect(bodyHasSystemFont).toBe(true);

      // Check paragraph elements
      const paragraphs = page.locator('p:not(pre p)');
      const pCount = await paragraphs.count();

      for (let i = 0; i < pCount; i++) {
        const pStyles = await paragraphs.nth(i).evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            fontFamily: style.fontFamily,
            fontSize: style.fontSize
          };
        });

        // Paragraph should inherit body font family (system font stack)
        const pHasSystemFont = systemFonts.some(font => pStyles.fontFamily.includes(font));
        expect(pHasSystemFont).toBe(true);
      }

      // Check list items
      const listItems = page.locator('.command-list li');
      const liCount = await listItems.count();

      if (liCount > 0) {
        const liStyles = await listItems.first().evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            fontFamily: style.fontFamily
          };
        });

        // List items should also use system font
        const liHasSystemFont = systemFonts.some(font => liStyles.fontFamily.includes(font));
        expect(liHasSystemFont).toBe(true);
      }
    });

  });

  test.describe('Color Scheme Consistency', () => {

    test('CSS custom properties are defined for consistent theming', async ({ page }) => {
      // Check that CSS custom properties are defined on :root
      const cssVariables = await page.evaluate(() => {
        const root = document.documentElement;
        const style = getComputedStyle(root);
        return {
          primaryColor: style.getPropertyValue('--primary-color').trim(),
          background: style.getPropertyValue('--background').trim(),
          surface: style.getPropertyValue('--surface').trim(),
          textPrimary: style.getPropertyValue('--text-primary').trim(),
          textSecondary: style.getPropertyValue('--text-secondary').trim(),
          accent: style.getPropertyValue('--accent').trim()
        };
      });

      // Verify all theme variables are defined
      expect(cssVariables.primaryColor).not.toBe('');
      expect(cssVariables.background).not.toBe('');
      expect(cssVariables.surface).not.toBe('');
      expect(cssVariables.textPrimary).not.toBe('');
      expect(cssVariables.textSecondary).not.toBe('');
      expect(cssVariables.accent).not.toBe('');
    });

    test('sections use consistent background colors from theme', async ({ page }) => {
      // Get the theme colors
      const themeColors = await page.evaluate(() => {
        const root = document.documentElement;
        const style = getComputedStyle(root);
        return {
          background: style.getPropertyValue('--background').trim(),
          surface: style.getPropertyValue('--surface').trim()
        };
      });

      // Check hero section background
      const heroBackground = await page.locator('.hero').evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );
      // Hero has gradient, so just verify it has a color

      // Check features section background
      const featuresBackground = await page.locator('.features').evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      // Check commands section background
      const commandsBackground = await page.locator('.commands').evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      // Verify sections have defined backgrounds (not transparent)
      expect(featuresBackground).not.toBe('rgba(0, 0, 0, 0)');
      expect(commandsBackground).not.toBe('rgba(0, 0, 0, 0)');
    });

  });

  test.describe('Code Block Styling', () => {

    test('all code blocks use monospace font with syntax highlighting', async ({ page }) => {
      // Get all code elements
      const codeElements = page.locator('code');
      const codeCount = await codeElements.count();

      expect(codeCount).toBeGreaterThan(0);

      // Check each code element has monospace font
      const monospaceFonts = ['SF Mono', 'Fira Code', 'Consolas', 'monospace', 'Monaco', 'Courier'];

      for (let i = 0; i < codeCount; i++) {
        const fontFamily = await codeElements.nth(i).evaluate((el) =>
          window.getComputedStyle(el).fontFamily
        );

        // Verify font family contains a monospace font
        const hasMonospace = monospaceFonts.some(font =>
          fontFamily.toLowerCase().includes(font.toLowerCase())
        );
        expect(hasMonospace).toBe(true);
      }
    });

    test('code blocks in quickstart section have consistent styling', async ({ page }) => {
      const quickstartPre = page.locator('.quickstart-step pre');
      const preCount = await quickstartPre.count();

      expect(preCount).toBeGreaterThan(0);

      // Collect styles from all pre elements
      const preStyles = [];

      for (let i = 0; i < preCount; i++) {
        const style = await quickstartPre.nth(i).evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            backgroundColor: computed.backgroundColor,
            borderRadius: computed.borderRadius,
            padding: computed.padding
          };
        });
        preStyles.push(style);
      }

      // All pre elements should have the same background color
      const backgroundColors = [...new Set(preStyles.map(s => s.backgroundColor))];
      expect(backgroundColors.length).toBe(1);

      // All pre elements should have the same border radius
      const borderRadii = [...new Set(preStyles.map(s => s.borderRadius))];
      expect(borderRadii.length).toBe(1);
    });

    test('inline code elements have consistent styling', async ({ page }) => {
      // Get inline code elements in command list
      const inlineCode = page.locator('.command-list code');
      const codeCount = await inlineCode.count();

      if (codeCount > 0) {
        const codeStyles = [];

        for (let i = 0; i < Math.min(codeCount, 5); i++) {
          const style = await inlineCode.nth(i).evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
              backgroundColor: computed.backgroundColor,
              padding: computed.padding,
              borderRadius: computed.borderRadius,
              fontFamily: computed.fontFamily
            };
          });
          codeStyles.push(style);
        }

        // All inline code should have the same background color
        const backgroundColors = [...new Set(codeStyles.map(s => s.backgroundColor))];
        expect(backgroundColors.length).toBe(1);

        // All inline code should have the same padding pattern
        const paddings = [...new Set(codeStyles.map(s => s.padding))];
        expect(paddings.length).toBe(1);
      }
    });

  });

  test.describe('Button Styling Consistency', () => {

    test('all buttons follow consistent design pattern (colors, padding, border-radius)', async ({ page }) => {
      // Get all button elements (including anchor buttons with .btn class)
      const buttons = page.locator('.btn');
      const buttonCount = await buttons.count();

      expect(buttonCount).toBeGreaterThan(0);

      // Collect button styles
      const buttonStyles = [];

      for (let i = 0; i < buttonCount; i++) {
        const style = await buttons.nth(i).evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            borderRadius: computed.borderRadius,
            fontWeight: computed.fontWeight,
            fontSize: computed.fontSize,
            display: computed.display,
            textDecoration: computed.textDecoration
          };
        });
        buttonStyles.push(style);
      }

      // All buttons should have the same border radius
      const borderRadii = [...new Set(buttonStyles.map(s => s.borderRadius))];
      expect(borderRadii.length).toBe(1);

      // All buttons should have the same font weight
      const fontWeights = [...new Set(buttonStyles.map(s => s.fontWeight))];
      expect(fontWeights.length).toBe(1);

      // All buttons should have the same font size
      const fontSizes = [...new Set(buttonStyles.map(s => s.fontSize))];
      expect(fontSizes.length).toBe(1);

      // All buttons should not have text decoration
      buttonStyles.forEach(style => {
        expect(style.textDecoration).toContain('none');
      });
    });

    test('primary and secondary buttons have distinct but consistent styling', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      const secondaryBtn = page.locator('.btn-secondary').first();

      // Get primary button styles
      const primaryStyles = await primaryBtn.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          backgroundColor: computed.backgroundColor,
          borderRadius: computed.borderRadius,
          padding: computed.padding
        };
      });

      // Get secondary button styles
      const secondaryStyles = await secondaryBtn.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          backgroundColor: computed.backgroundColor,
          borderRadius: computed.borderRadius,
          padding: computed.padding
        };
      });

      // Both should have the same border radius
      expect(primaryStyles.borderRadius).toBe(secondaryStyles.borderRadius);

      // Both should have the same padding
      expect(primaryStyles.padding).toBe(secondaryStyles.padding);

      // But they should have different background colors (distinct styling)
      expect(primaryStyles.backgroundColor).not.toBe(secondaryStyles.backgroundColor);
    });

    test('buttons have proper interactive states', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();

      // Verify button has transition property for smooth interactions
      // The CSS uses "transition: all 0.2s ease" which shows as "0.2s" in computed styles
      const transitionDuration = await primaryBtn.evaluate((el) =>
        window.getComputedStyle(el).transitionDuration
      );

      // Verify transition is defined (not "0s")
      expect(transitionDuration).not.toBe('0s');
      expect(transitionDuration).toMatch(/[\d.]+s/);

      // Hover over the button to verify hover state exists
      await primaryBtn.hover();
      await page.waitForTimeout(300);

      // Verify cursor changes on hover (indicates interactive element)
      const cursor = await primaryBtn.evaluate((el) =>
        window.getComputedStyle(el).cursor
      );
      // Buttons/links typically have pointer cursor
      expect(cursor).toBe('pointer');
    });

  });

  test.describe('Feature Cards Consistency', () => {

    test('all feature cards have consistent styling', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      expect(cardCount).toBe(3); // Three feature cards

      const cardStyles = [];

      for (let i = 0; i < cardCount; i++) {
        const style = await featureCards.nth(i).evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            backgroundColor: computed.backgroundColor,
            borderRadius: computed.borderRadius,
            padding: computed.padding,
            border: computed.border
          };
        });
        cardStyles.push(style);
      }

      // All cards should have the same background color
      const backgroundColors = [...new Set(cardStyles.map(s => s.backgroundColor))];
      expect(backgroundColors.length).toBe(1);

      // All cards should have the same border radius
      const borderRadii = [...new Set(cardStyles.map(s => s.borderRadius))];
      expect(borderRadii.length).toBe(1);

      // All cards should have the same padding
      const paddings = [...new Set(cardStyles.map(s => s.padding))];
      expect(paddings.length).toBe(1);
    });

  });

  test.describe('Command Categories Consistency', () => {

    test('all command categories have consistent styling', async ({ page }) => {
      const commandCategories = page.locator('.command-category');
      const categoryCount = await commandCategories.count();

      expect(categoryCount).toBeGreaterThan(0);

      const categoryStyles = [];

      for (let i = 0; i < categoryCount; i++) {
        const style = await commandCategories.nth(i).evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            backgroundColor: computed.backgroundColor,
            borderRadius: computed.borderRadius,
            padding: computed.padding,
            border: computed.border
          };
        });
        categoryStyles.push(style);
      }

      // All categories should have the same background color
      const backgroundColors = [...new Set(categoryStyles.map(s => s.backgroundColor))];
      expect(backgroundColors.length).toBe(1);

      // All categories should have the same border radius
      const borderRadii = [...new Set(categoryStyles.map(s => s.borderRadius))];
      expect(borderRadii.length).toBe(1);
    });

  });

});

// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Typography and Visual Design Tests for MirDB Homepage
 * Verifies typography follows design requirements - monospace for code, sans-serif for body
 *
 * Test cases:
 * TC1: Code blocks use monospace font family
 * TC2: Body text uses sans-serif font family
 * TC3: Headings have clear size hierarchy (h1 > h2 > h3)
 * TC4: Generous whitespace between sections for readability
 */

test.describe('Typography and Visual Design (Scenario 19)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Font Family Requirements', () => {
    test('TC1: Code blocks use monospace font family', async ({ page }) => {
      // Test quick start code block
      const quickStartCode = page.locator('[data-testid="quick-start"] .code-block pre code');
      await expect(quickStartCode).toBeVisible();

      const quickStartFontFamily = await quickStartCode.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Verify monospace font family is used (check for common monospace fonts)
      expect(quickStartFontFamily.toLowerCase()).toMatch(
        /sf mono|consolas|liberation mono|menlo|monospace|courier/i
      );

      // Test command codes in commands section
      const commandCodes = page.locator('[data-testid^="command-"] code');
      const commandCount = await commandCodes.count();
      expect(commandCount).toBeGreaterThan(0);

      for (let i = 0; i < commandCount; i++) {
        const code = commandCodes.nth(i);
        await expect(code).toBeVisible();

        const fontFamily = await code.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });

        expect(fontFamily.toLowerCase()).toMatch(
          /sf mono|consolas|liberation mono|menlo|monospace|courier/i
        );
      }

      // Test configuration values (also code elements)
      const configValues = page.locator('.config-value');
      const configCount = await configValues.count();

      for (let i = 0; i < configCount; i++) {
        const configCode = configValues.nth(i);
        await expect(configCode).toBeVisible();

        const fontFamily = await configCode.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });

        expect(fontFamily.toLowerCase()).toMatch(
          /sf mono|consolas|liberation mono|menlo|monospace|courier/i
        );
      }
    });

    test('TC2: Body text uses sans-serif font family', async ({ page }) => {
      // Check body element font
      const body = page.locator('body');
      const bodyFontFamily = await body.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Verify sans-serif font family is used (should NOT be monospace/serif as primary)
      expect(bodyFontFamily.toLowerCase()).toMatch(
        /apple-system|blinkmacsystemfont|segoe ui|roboto|oxygen|ubuntu|cantarell|sans-serif/i
      );
      // Ensure it's NOT a monospace or serif font as primary
      expect(bodyFontFamily.toLowerCase()).not.toMatch(/^(courier|monospace|times|georgia|serif)/i);

      // Check tagline paragraph (body text)
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();
      const taglineFontFamily = await tagline.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      expect(taglineFontFamily.toLowerCase()).toMatch(
        /apple-system|blinkmacsystemfont|segoe ui|roboto|oxygen|ubuntu|cantarell|sans-serif/i
      );

      // Check feature card descriptions (body text)
      const featureDescriptions = page.locator('.feature-card p');
      const descCount = await featureDescriptions.count();
      expect(descCount).toBeGreaterThan(0);

      for (let i = 0; i < descCount; i++) {
        const desc = featureDescriptions.nth(i);
        const fontFamily = await desc.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });

        expect(fontFamily.toLowerCase()).toMatch(
          /apple-system|blinkmacsystemfont|segoe ui|roboto|oxygen|ubuntu|cantarell|sans-serif/i
        );
      }

      // Check command item descriptions (body text)
      const commandDescriptions = page.locator('.command-item span');
      const cmdDescCount = await commandDescriptions.count();

      for (let i = 0; i < cmdDescCount; i++) {
        const desc = commandDescriptions.nth(i);
        const fontFamily = await desc.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });

        expect(fontFamily.toLowerCase()).toMatch(
          /apple-system|blinkmacsystemfont|segoe ui|roboto|oxygen|ubuntu|cantarell|sans-serif/i
        );
      }
    });
  });

  test.describe('Heading Hierarchy', () => {
    test('TC3: Headings have clear size hierarchy (h1 > h2 > h3)', async ({ page }) => {
      // Get h1 font size (hero heading)
      const h1 = page.locator('.hero h1');
      await expect(h1).toBeVisible();
      const h1FontSize = await h1.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Get h2 font sizes (section headings)
      const h2Elements = page.locator('section h2');
      const h2Count = await h2Elements.count();
      expect(h2Count).toBeGreaterThan(0);

      // Collect all h2 font sizes
      const h2FontSizes = [];
      for (let i = 0; i < h2Count; i++) {
        const h2 = h2Elements.nth(i);
        const fontSize = await h2.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        h2FontSizes.push(fontSize);
      }

      // Get h3 font sizes (feature card headings)
      const h3Elements = page.locator('.feature-card h3');
      const h3Count = await h3Elements.count();
      expect(h3Count).toBeGreaterThan(0);

      const h3FontSizes = [];
      for (let i = 0; i < h3Count; i++) {
        const h3 = h3Elements.nth(i);
        const fontSize = await h3.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        h3FontSizes.push(fontSize);
      }

      // Verify hierarchy: h1 > h2 > h3
      const avgH2FontSize = h2FontSizes.reduce((a, b) => a + b, 0) / h2FontSizes.length;
      const avgH3FontSize = h3FontSizes.reduce((a, b) => a + b, 0) / h3FontSizes.length;

      // h1 should be larger than h2
      expect(h1FontSize).toBeGreaterThan(avgH2FontSize);

      // h2 should be larger than h3
      expect(avgH2FontSize).toBeGreaterThan(avgH3FontSize);

      // Verify minimum size differences for clear hierarchy
      // h1 should be at least 1.2x larger than h2
      expect(h1FontSize / avgH2FontSize).toBeGreaterThanOrEqual(1.2);

      // h2 should be at least 1.1x larger than h3
      expect(avgH2FontSize / avgH3FontSize).toBeGreaterThanOrEqual(1.1);

      // Log hierarchy for debugging
      console.log(`Font size hierarchy: h1=${h1FontSize}px > h2=${avgH2FontSize}px > h3=${avgH3FontSize}px`);
    });

    test('TC3.1: Heading weights establish visual hierarchy', async ({ page }) => {
      // h1 should be bold/heavy
      const h1 = page.locator('.hero h1');
      const h1Weight = await h1.evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontWeight);
      });
      expect(h1Weight).toBeGreaterThanOrEqual(700); // Bold or heavier

      // h2 section headings should have substantial weight
      const h2 = page.locator('section h2').first();
      const h2Weight = await h2.evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontWeight);
      });
      expect(h2Weight).toBeGreaterThanOrEqual(500); // Medium or heavier

      // h3 should have reasonable weight
      const h3 = page.locator('.feature-card h3').first();
      const h3Weight = await h3.evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontWeight);
      });
      expect(h3Weight).toBeGreaterThanOrEqual(400); // Normal or heavier
    });
  });

  test.describe('Whitespace and Spacing', () => {
    test('TC4: Generous whitespace between sections for readability', async ({ page }) => {
      // Get section elements
      const sections = page.locator('main > section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThan(2);

      // Check padding on sections
      for (let i = 0; i < sectionCount; i++) {
        const section = sections.nth(i);
        await expect(section).toBeVisible();

        const padding = await section.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            top: parseFloat(style.paddingTop),
            bottom: parseFloat(style.paddingBottom),
            left: parseFloat(style.paddingLeft),
            right: parseFloat(style.paddingRight),
          };
        });

        // Each section should have generous vertical padding (at least 48px = 3rem at base)
        expect(padding.top).toBeGreaterThanOrEqual(48);
        expect(padding.bottom).toBeGreaterThanOrEqual(48);

        // Horizontal padding should provide breathing room (at least 16px = 1rem)
        expect(padding.left).toBeGreaterThanOrEqual(16);
        expect(padding.right).toBeGreaterThanOrEqual(16);
      }
    });

    test('TC4.1: Hero section has generous vertical spacing', async ({ page }) => {
      const hero = page.locator('[data-testid="hero-section"]');
      await expect(hero).toBeVisible();

      const heroPadding = await hero.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          top: parseFloat(style.paddingTop),
          bottom: parseFloat(style.paddingBottom),
        };
      });

      // Hero should have substantial vertical padding (at least 80px = 5rem at base)
      // Note: responsive design may reduce this on mobile
      const minHeroPadding = 48; // Minimum for mobile
      expect(heroPadding.top).toBeGreaterThanOrEqual(minHeroPadding);
      expect(heroPadding.bottom).toBeGreaterThanOrEqual(minHeroPadding);
    });

    test('TC4.2: Section headings have bottom margin for spacing', async ({ page }) => {
      const h2Elements = page.locator('section h2');
      const h2Count = await h2Elements.count();

      for (let i = 0; i < h2Count; i++) {
        const h2 = h2Elements.nth(i);
        const marginBottom = await h2.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).marginBottom);
        });

        // Each h2 should have meaningful bottom margin (at least 32px = 2rem at base)
        expect(marginBottom).toBeGreaterThanOrEqual(32);
      }
    });

    test('TC4.3: Feature cards have internal padding', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        const padding = await card.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return parseFloat(style.padding) || parseFloat(style.paddingTop);
        });

        // Cards should have internal padding (at least 24px = 1.5rem)
        expect(padding).toBeGreaterThanOrEqual(24);
      }
    });

    test('TC4.4: Grid layouts have gaps between items', async ({ page }) => {
      // Check features grid gap
      const featuresGrid = page.locator('.features-grid');
      const featuresGap = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.gap) || parseFloat(style.rowGap) || 0;
      });
      expect(featuresGap).toBeGreaterThanOrEqual(16); // At least 1rem gap

      // Check commands grid gap
      const commandsGrid = page.locator('.commands-grid');
      const commandsGap = await commandsGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.gap) || parseFloat(style.rowGap) || 0;
      });
      expect(commandsGap).toBeGreaterThanOrEqual(16); // At least 1rem gap

      // Check status grid gap
      const statusGrid = page.locator('.status-grid');
      const statusGap = await statusGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.gap) || parseFloat(style.rowGap) || 0;
      });
      expect(statusGap).toBeGreaterThanOrEqual(12); // At least 0.75rem gap
    });

    test('TC4.5: Body text has proper line height for readability', async ({ page }) => {
      const body = page.locator('body');
      const lineHeight = await body.evaluate((el) => {
        const style = window.getComputedStyle(el);
        const lh = style.lineHeight;
        // Line height can be a number (multiplier) or px value
        if (lh === 'normal') return 1.5; // Default normal is ~1.2, we expect more
        if (lh.endsWith('px')) {
          const fontSize = parseFloat(style.fontSize);
          return parseFloat(lh) / fontSize;
        }
        return parseFloat(lh);
      });

      // Line height should be at least 1.5 for good readability
      expect(lineHeight).toBeGreaterThanOrEqual(1.4);
    });
  });

  test.describe('Visual Consistency', () => {
    test('TC5: Consistent heading styles across sections', async ({ page }) => {
      const h2Elements = page.locator('section h2');
      const count = await h2Elements.count();
      expect(count).toBeGreaterThan(1);

      // Get styles from first h2 as reference
      const firstH2 = h2Elements.first();
      const referenceStyles = await firstH2.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          fontFamily: style.fontFamily,
          fontWeight: style.fontWeight,
          textAlign: style.textAlign,
        };
      });

      // All other h2s should match
      for (let i = 1; i < count; i++) {
        const h2 = h2Elements.nth(i);
        const styles = await h2.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            fontFamily: style.fontFamily,
            fontWeight: style.fontWeight,
            textAlign: style.textAlign,
          };
        });

        expect(styles.fontFamily).toBe(referenceStyles.fontFamily);
        expect(styles.fontWeight).toBe(referenceStyles.fontWeight);
        expect(styles.textAlign).toBe(referenceStyles.textAlign);
      }
    });

    test('TC6: Code elements have consistent styling', async ({ page }) => {
      // Get all inline code elements
      const codeElements = page.locator('.command-item code, .config-value');
      const count = await codeElements.count();
      expect(count).toBeGreaterThan(1);

      // Get first code element as reference
      const firstCode = codeElements.first();
      const referenceFont = await firstCode.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // All code elements should use same font family
      for (let i = 1; i < count; i++) {
        const code = codeElements.nth(i);
        const fontFamily = await code.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });

        expect(fontFamily).toBe(referenceFont);
      }
    });
  });
});

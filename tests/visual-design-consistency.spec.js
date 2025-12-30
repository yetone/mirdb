// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Visual Design Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Code blocks use monospace font', () => {
    test('all pre elements use monospace font-family', async ({ page }) => {
      const preElements = await page.locator('pre').all();
      expect(preElements.length).toBeGreaterThan(0);

      for (const pre of preElements) {
        const fontFamily = await pre.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });
        // Check that font-family contains monospace-type fonts
        const isMonospace =
          fontFamily.toLowerCase().includes('mono') ||
          fontFamily.toLowerCase().includes('monospace') ||
          fontFamily.toLowerCase().includes('courier') ||
          fontFamily.toLowerCase().includes('consolas');
        expect(isMonospace).toBe(true);
      }
    });

    test('all code elements use monospace font-family', async ({ page }) => {
      const codeElements = await page.locator('code').all();
      expect(codeElements.length).toBeGreaterThan(0);

      for (const code of codeElements) {
        const fontFamily = await code.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });
        // Check that font-family contains monospace-type fonts
        const isMonospace =
          fontFamily.toLowerCase().includes('mono') ||
          fontFamily.toLowerCase().includes('monospace') ||
          fontFamily.toLowerCase().includes('courier') ||
          fontFamily.toLowerCase().includes('consolas');
        expect(isMonospace).toBe(true);
      }
    });

    test('command table first column uses monospace font', async ({ page }) => {
      const commandCells = await page.locator('.commands-table td:first-child').all();
      expect(commandCells.length).toBeGreaterThan(0);

      for (const cell of commandCells) {
        const fontFamily = await cell.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });
        const isMonospace =
          fontFamily.toLowerCase().includes('mono') ||
          fontFamily.toLowerCase().includes('monospace') ||
          fontFamily.toLowerCase().includes('courier') ||
          fontFamily.toLowerCase().includes('consolas');
        expect(isMonospace).toBe(true);
      }
    });
  });

  test.describe('Test Case 2: Typography hierarchy', () => {
    test('h1 has largest font size', async ({ page }) => {
      const h1FontSize = await page.locator('h1').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      const h2FontSize = await page.locator('h2').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(h1FontSize).toBeGreaterThan(h2FontSize);
    });

    test('h2 has larger font size than h3', async ({ page }) => {
      const h2FontSize = await page.locator('h2').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      const h3FontSize = await page.locator('h3').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(h2FontSize).toBeGreaterThan(h3FontSize);
    });

    test('h3 has larger font size than body text', async ({ page }) => {
      const h3FontSize = await page.locator('h3').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      const bodyFontSize = await page.locator('body').evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(h3FontSize).toBeGreaterThan(bodyFontSize);
    });

    test('headings have consistent font-weight hierarchy', async ({ page }) => {
      const h1Weight = await page.locator('h1').first().evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontWeight);
      });
      const h2Weight = await page.locator('h2').first().evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontWeight);
      });
      const h3Weight = await page.locator('h3').first().evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontWeight);
      });

      // All headings should have bold or semi-bold weight (>=600)
      expect(h1Weight).toBeGreaterThanOrEqual(600);
      expect(h2Weight).toBeGreaterThanOrEqual(600);
      expect(h3Weight).toBeGreaterThanOrEqual(600);
    });

    test('all h2 elements have consistent styling', async ({ page }) => {
      const h2Elements = await page.locator('h2').all();
      expect(h2Elements.length).toBeGreaterThan(1);

      const firstH2Style = await h2Elements[0].evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          fontSize: styles.fontSize,
          fontWeight: styles.fontWeight,
          fontFamily: styles.fontFamily,
        };
      });

      for (let i = 1; i < h2Elements.length; i++) {
        const currentH2Style = await h2Elements[i].evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            fontSize: styles.fontSize,
            fontWeight: styles.fontWeight,
            fontFamily: styles.fontFamily,
          };
        });
        expect(currentH2Style.fontSize).toBe(firstH2Style.fontSize);
        expect(currentH2Style.fontWeight).toBe(firstH2Style.fontWeight);
        expect(currentH2Style.fontFamily).toBe(firstH2Style.fontFamily);
      }
    });
  });

  test.describe('Test Case 3: Button styling consistency', () => {
    test('all buttons have consistent border-radius', async ({ page }) => {
      const buttons = await page.locator('.btn').all();
      expect(buttons.length).toBeGreaterThan(1);

      const firstBtnRadius = await buttons[0].evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });

      for (const btn of buttons) {
        const borderRadius = await btn.evaluate((el) => {
          return window.getComputedStyle(el).borderRadius;
        });
        expect(borderRadius).toBe(firstBtnRadius);
      }
    });

    test('all buttons have consistent padding', async ({ page }) => {
      const buttons = await page.locator('.btn').all();
      expect(buttons.length).toBeGreaterThan(1);

      const firstBtnPadding = await buttons[0].evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });

      for (const btn of buttons) {
        const padding = await btn.evaluate((el) => {
          return window.getComputedStyle(el).padding;
        });
        expect(padding).toBe(firstBtnPadding);
      }
    });

    test('all buttons have consistent font-weight', async ({ page }) => {
      const buttons = await page.locator('.btn').all();
      expect(buttons.length).toBeGreaterThan(1);

      const firstBtnFontWeight = await buttons[0].evaluate((el) => {
        return window.getComputedStyle(el).fontWeight;
      });

      for (const btn of buttons) {
        const fontWeight = await btn.evaluate((el) => {
          return window.getComputedStyle(el).fontWeight;
        });
        expect(fontWeight).toBe(firstBtnFontWeight);
      }
    });

    test('primary and secondary buttons use consistent color scheme', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      const secondaryBtn = page.locator('.btn-secondary').first();

      // Primary button should have colored background
      const primaryBgColor = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(primaryBgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(primaryBgColor).not.toBe('transparent');

      // Secondary button should have different styling
      const secondaryBgColor = await secondaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Secondary should be different from primary
      expect(secondaryBgColor).not.toBe(primaryBgColor);
    });
  });

  test.describe('Test Case 4: Section spacing consistency', () => {
    test('main sections have consistent vertical padding', async ({ page }) => {
      const sections = await page.locator('section[class*="-section"]').all();
      expect(sections.length).toBeGreaterThan(2);

      const sectionPaddings = [];
      for (const section of sections) {
        const padding = await section.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            paddingTop: styles.paddingTop,
            paddingBottom: styles.paddingBottom,
          };
        });
        sectionPaddings.push(padding);
      }

      // Check that non-hero sections have consistent padding
      // Hero section may have different padding, so we check other sections
      const nonHeroSections = sectionPaddings.slice(1);
      const firstNonHeroPadding = nonHeroSections[0];

      for (const padding of nonHeroSections) {
        expect(padding.paddingTop).toBe(firstNonHeroPadding.paddingTop);
        expect(padding.paddingBottom).toBe(firstNonHeroPadding.paddingBottom);
      }
    });

    test('feature cards have consistent padding', async ({ page }) => {
      const featureCards = await page.locator('.feature-card').all();
      expect(featureCards.length).toBeGreaterThan(1);

      const firstCardPadding = await featureCards[0].evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });

      for (const card of featureCards) {
        const padding = await card.evaluate((el) => {
          return window.getComputedStyle(el).padding;
        });
        expect(padding).toBe(firstCardPadding);
      }
    });

    test('section containers have consistent max-width', async ({ page }) => {
      const containers = await page.locator('.section-container').all();
      expect(containers.length).toBeGreaterThan(1);

      const firstContainerMaxWidth = await containers[0].evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      for (const container of containers) {
        const maxWidth = await container.evaluate((el) => {
          return window.getComputedStyle(el).maxWidth;
        });
        expect(maxWidth).toBe(firstContainerMaxWidth);
      }
    });

    test('steps have consistent margin-bottom spacing', async ({ page }) => {
      const steps = await page.locator('.step').all();
      expect(steps.length).toBeGreaterThan(1);

      // Get all but the last step (last may have different margin)
      const stepsToCheck = steps.slice(0, -1);
      const firstStepMargin = await stepsToCheck[0].evaluate((el) => {
        return window.getComputedStyle(el).marginBottom;
      });

      for (const step of stepsToCheck) {
        const marginBottom = await step.evaluate((el) => {
          return window.getComputedStyle(el).marginBottom;
        });
        expect(marginBottom).toBe(firstStepMargin);
      }
    });
  });

  test.describe('Test Case 5: Color palette consistency', () => {
    test('uses CSS custom properties for consistent colors', async ({ page }) => {
      // Verify that CSS custom properties are defined
      const rootStyles = await page.evaluate(() => {
        const root = document.documentElement;
        const styles = getComputedStyle(root);
        return {
          primaryColor: styles.getPropertyValue('--primary-color').trim(),
          textColor: styles.getPropertyValue('--text-color').trim(),
          textMuted: styles.getPropertyValue('--text-muted').trim(),
          bgColor: styles.getPropertyValue('--bg-color').trim(),
          borderColor: styles.getPropertyValue('--border-color').trim(),
        };
      });

      // Check that custom properties are defined and not empty
      expect(rootStyles.primaryColor).toBeTruthy();
      expect(rootStyles.textColor).toBeTruthy();
      expect(rootStyles.textMuted).toBeTruthy();
      expect(rootStyles.bgColor).toBeTruthy();
      expect(rootStyles.borderColor).toBeTruthy();
    });

    test('primary buttons use primary color from palette', async ({ page }) => {
      const primaryBtnBgColor = await page.locator('.btn-primary').first().evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      const primaryColor = await page.evaluate(() => {
        const root = document.documentElement;
        return getComputedStyle(root).getPropertyValue('--primary-color').trim();
      });

      // Convert hex to RGB for comparison
      const hexToRgb = (hex) => {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result
          ? `rgb(${parseInt(result[1], 16)}, ${parseInt(result[2], 16)}, ${parseInt(result[3], 16)})`
          : null;
      };

      const expectedRgb = hexToRgb(primaryColor);
      expect(primaryBtnBgColor).toBe(expectedRgb);
    });

    test('all nav links use same text color', async ({ page }) => {
      const navLinks = await page.locator('.nav-link').all();
      expect(navLinks.length).toBeGreaterThan(1);

      const firstLinkColor = await navLinks[0].evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      for (const link of navLinks) {
        const color = await link.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        expect(color).toBe(firstLinkColor);
      }
    });

    test('feature cards have consistent border color', async ({ page }) => {
      const featureCards = await page.locator('.feature-card').all();
      expect(featureCards.length).toBeGreaterThan(1);

      const firstCardBorderColor = await featureCards[0].evaluate((el) => {
        return window.getComputedStyle(el).borderColor;
      });

      for (const card of featureCards) {
        const borderColor = await card.evaluate((el) => {
          return window.getComputedStyle(el).borderColor;
        });
        expect(borderColor).toBe(firstCardBorderColor);
      }
    });

    test('code blocks use consistent background color', async ({ page }) => {
      const preElements = await page.locator('pre').all();
      expect(preElements.length).toBeGreaterThan(1);

      const firstPreBgColor = await preElements[0].evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      for (const pre of preElements) {
        const bgColor = await pre.evaluate((el) => {
          return window.getComputedStyle(el).backgroundColor;
        });
        expect(bgColor).toBe(firstPreBgColor);
      }
    });

    test('muted text elements use consistent color', async ({ page }) => {
      // Check tagline and hero description use the muted text color
      const taglineColor = await page.locator('.tagline').evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      const heroDescColor = await page.locator('.hero-description').evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      expect(taglineColor).toBe(heroDescColor);
    });

    test('footer uses consistent dark theme colors', async ({ page }) => {
      const footerBgColor = await page.locator('.footer').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      const footerTextColor = await page.locator('.footer').evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Footer should have dark background
      const codeBgColor = await page.evaluate(() => {
        const root = document.documentElement;
        return getComputedStyle(root).getPropertyValue('--code-bg').trim();
      });

      // Convert hex to RGB for comparison
      const hexToRgb = (hex) => {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result
          ? `rgb(${parseInt(result[1], 16)}, ${parseInt(result[2], 16)}, ${parseInt(result[3], 16)})`
          : null;
      };

      const expectedFooterBg = hexToRgb(codeBgColor);
      expect(footerBgColor).toBe(expectedFooterBg);
    });
  });
});

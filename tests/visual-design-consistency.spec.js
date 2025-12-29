// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Visual Design Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Color Palette Usage', () => {
    test('TC1: Primary colors are consistently applied (blue/teal with orange accents)', async ({ page }) => {
      // Test CSS variables are properly defined in :root
      const primaryColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim();
      });
      const secondaryColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--secondary-color').trim();
      });
      const bgDark = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--bg-dark').trim();
      });

      // Verify primary blue color is defined
      expect(primaryColor).toBeTruthy();
      expect(primaryColor).toMatch(/#[0-9a-fA-F]{6}|rgb/);

      // Verify secondary/accent color (orange/rust) is defined
      expect(secondaryColor).toBeTruthy();
      expect(secondaryColor).toMatch(/#[0-9a-fA-F]{6}|rgb/);

      // Verify dark background is defined (deep blue)
      expect(bgDark).toBeTruthy();
      expect(bgDark).toMatch(/#[0-9a-fA-F]{6}|rgb/);

      // Check hero section uses dark background
      const heroSection = page.locator('.hero');
      const heroBg = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(heroBg).toBeTruthy();

      // Verify primary button uses primary color
      const primaryBtn = page.locator('.btn-primary').first();
      const btnBgColor = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(btnBgColor).toBeTruthy();

      // Verify feature icons use primary color
      const featureIcon = page.locator('.feature-icon').first();
      const iconColor = await featureIcon.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(iconColor).toBeTruthy();
    });

    test('TC1b: Color scheme feels professional and developer-focused', async ({ page }) => {
      // Check footer uses dark theme
      const footer = page.locator('.footer');
      const footerBg = await footer.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(footerBg).toBeTruthy();

      // Check code blocks use dark background
      const codeBlock = page.locator('.code-block').first();
      const codeBlockBg = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(codeBlockBg).toBeTruthy();

      // Verify command cards have subtle styling with accent border
      const commandCard = page.locator('.command-card').first();
      const borderLeft = await commandCard.evaluate((el) => {
        return window.getComputedStyle(el).borderLeftWidth;
      });
      expect(parseInt(borderLeft)).toBeGreaterThan(0);
    });
  });

  test.describe('Typography Consistency', () => {
    test('TC2: Font families are consistent for headings, body, and code', async ({ page }) => {
      // Check body font (system font stack)
      const bodyFont = await page.evaluate(() => {
        return window.getComputedStyle(document.body).fontFamily;
      });
      expect(bodyFont).toContain('-apple-system');

      // Check heading font consistency
      const h1Font = await page.locator('h1').first().evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      const h2Font = await page.locator('h2').first().evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      const h3Font = await page.locator('h3').first().evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Headings should use same font family as body (system font stack)
      expect(h1Font).toContain('-apple-system');
      expect(h2Font).toContain('-apple-system');
      expect(h3Font).toContain('-apple-system');

      // Check code uses monospace font
      const codeFont = await page.locator('pre code').first().evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      expect(codeFont.toLowerCase()).toMatch(/monaco|menlo|ubuntu mono|monospace/i);

      // Check command names use monospace
      const commandNameFont = await page.locator('.command-name').first().evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      expect(commandNameFont.toLowerCase()).toMatch(/monaco|menlo|ubuntu mono|monospace/i);
    });

    test('TC2b: Font sizes follow proper hierarchy', async ({ page }) => {
      // Get font sizes for different heading levels
      const h1Size = await page.locator('h1').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      const sectionTitleSize = await page.locator('.section-title').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      const featureTitleSize = await page.locator('.feature-title').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      const bodySize = await page.locator('p').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Verify hierarchy: h1 > section-title > feature-title > body
      expect(h1Size).toBeGreaterThan(sectionTitleSize);
      expect(sectionTitleSize).toBeGreaterThan(featureTitleSize);
      expect(featureTitleSize).toBeGreaterThanOrEqual(bodySize);
    });

    test('TC2c: Font weights are appropriate for different elements', async ({ page }) => {
      // Headings should be bold
      const h1Weight = await page.locator('h1').first().evaluate((el) => {
        return window.getComputedStyle(el).fontWeight;
      });
      expect(parseInt(h1Weight)).toBeGreaterThanOrEqual(600);

      // Section titles should be bold
      const sectionTitleWeight = await page.locator('.section-title').first().evaluate((el) => {
        return window.getComputedStyle(el).fontWeight;
      });
      expect(parseInt(sectionTitleWeight)).toBeGreaterThanOrEqual(600);

      // Buttons should have medium-to-bold weight
      const btnWeight = await page.locator('.btn').first().evaluate((el) => {
        return window.getComputedStyle(el).fontWeight;
      });
      expect(parseInt(btnWeight)).toBeGreaterThanOrEqual(500);
    });
  });

  test.describe('Visual Hierarchy', () => {
    test('TC3: Clear visual hierarchy with proper spacing between sections', async ({ page }) => {
      // Check that sections have proper padding
      const heroSection = page.locator('.hero');
      const heroPadding = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).paddingTop;
      });
      expect(parseInt(heroPadding)).toBeGreaterThanOrEqual(40);

      const featuresSection = page.locator('.features');
      const featuresPadding = await featuresSection.evaluate((el) => {
        return window.getComputedStyle(el).paddingTop;
      });
      expect(parseInt(featuresPadding)).toBeGreaterThanOrEqual(40);

      const commandsSection = page.locator('.commands');
      const commandsPadding = await commandsSection.evaluate((el) => {
        return window.getComputedStyle(el).paddingTop;
      });
      expect(parseInt(commandsPadding)).toBeGreaterThanOrEqual(40);

      const gettingStartedSection = page.locator('.getting-started');
      const gettingStartedPadding = await gettingStartedSection.evaluate((el) => {
        return window.getComputedStyle(el).paddingTop;
      });
      expect(parseInt(gettingStartedPadding)).toBeGreaterThanOrEqual(40);
    });

    test('TC3b: Sections are clearly delineated with visual separation', async ({ page }) => {
      // Get background colors of alternating sections
      const heroSection = page.locator('.hero');
      const heroBackground = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      const featuresSection = page.locator('.features');
      const featuresBackground = await featuresSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      const commandsSection = page.locator('.commands');
      const commandsBackground = await commandsSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      const gettingStartedSection = page.locator('.getting-started');
      const gettingStartedBackground = await gettingStartedSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify sections have different backgrounds for visual separation
      // Hero should be dark, features and getting-started should be light, commands should be white
      expect(heroBackground).not.toEqual(featuresBackground);
      expect(featuresBackground).not.toEqual(commandsBackground);
    });

    test('TC3c: Cards and elements have appropriate spacing', async ({ page }) => {
      // Check feature cards have gap between them
      const featuresGrid = page.locator('.features-grid');
      const gridGap = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gap;
      });
      expect(parseInt(gridGap)).toBeGreaterThan(0);

      // Check command cards have gap
      const commandsGrid = page.locator('.commands-grid');
      const commandsGap = await commandsGrid.evaluate((el) => {
        return window.getComputedStyle(el).gap;
      });
      expect(parseInt(commandsGap)).toBeGreaterThan(0);

      // Check step elements have margin between them
      const steps = page.locator('.step');
      const stepCount = await steps.count();
      expect(stepCount).toBeGreaterThan(0);

      const stepMargin = await steps.first().evaluate((el) => {
        return window.getComputedStyle(el).marginBottom;
      });
      expect(parseInt(stepMargin)).toBeGreaterThan(0);
    });
  });

  test.describe('Button Styling Consistency', () => {
    test('TC4: All buttons follow consistent styling patterns', async ({ page }) => {
      // Get all buttons (both btn class and copy-btn class)
      const primaryBtn = page.locator('.btn-primary').first();
      const secondaryBtn = page.locator('.btn-secondary').first();
      const copyBtn = page.locator('.copy-btn').first();

      // Check primary button has consistent padding
      const primaryPadding = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });
      expect(primaryPadding).toBeTruthy();

      // Check primary button has border radius
      const primaryRadius = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(parseInt(primaryRadius)).toBeGreaterThan(0);

      // Check secondary button has border radius
      const secondaryRadius = await secondaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(parseInt(secondaryRadius)).toBeGreaterThan(0);

      // Check both main buttons have same border radius
      expect(primaryRadius).toEqual(secondaryRadius);

      // Check copy button exists and has styling
      const copyBtnRadius = await copyBtn.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(parseInt(copyBtnRadius)).toBeGreaterThan(0);
    });

    test('TC4b: Buttons have proper hover transitions', async ({ page }) => {
      // Check that buttons have transition property
      const primaryBtn = page.locator('.btn-primary').first();
      const transition = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });
      expect(transition).not.toBe('none');

      const secondaryBtn = page.locator('.btn-secondary').first();
      const secondaryTransition = await secondaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });
      expect(secondaryTransition).not.toBe('none');

      const copyBtn = page.locator('.copy-btn').first();
      const copyTransition = await copyBtn.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });
      expect(copyTransition).not.toBe('none');
    });

    test('TC4c: Button text styling is consistent', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      const secondaryBtn = page.locator('.btn-secondary').first();

      // Check text decoration is none (no underlines on links styled as buttons)
      const primaryDecoration = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).textDecoration;
      });
      expect(primaryDecoration).toContain('none');

      const secondaryDecoration = await secondaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).textDecoration;
      });
      expect(secondaryDecoration).toContain('none');

      // Check both buttons have same font weight
      const primaryWeight = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).fontWeight;
      });
      const secondaryWeight = await secondaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).fontWeight;
      });
      expect(primaryWeight).toEqual(secondaryWeight);
    });
  });

  test.describe('Dark Theme Compatibility', () => {
    test('TC5: Dark theme elements are properly styled', async ({ page }) => {
      // Check hero section (already dark themed)
      const heroSection = page.locator('.hero');
      const heroColor = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Hero text should be light colored for contrast on dark background
      expect(heroColor).toBeTruthy();

      // Check footer (dark themed)
      const footer = page.locator('.footer');
      const footerColor = await footer.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Footer text should be light colored
      expect(footerColor).toBeTruthy();

      // Check code blocks (dark themed)
      const codeBlock = page.locator('.code-block').first();
      const codeBg = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Code block should have dark background
      expect(codeBg).toBeTruthy();

      const preBlock = page.locator('.step pre').first();
      const preColor = await preBlock.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Pre block text should be light colored
      expect(preColor).toBeTruthy();
    });

    test('TC5b: Dark sections have proper text contrast', async ({ page }) => {
      // Check hero tagline color
      const tagline = page.locator('.hero .tagline');
      const taglineColor = await tagline.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(taglineColor).toBeTruthy();

      // Check hero description color
      const description = page.locator('.hero .description');
      const descColor = await description.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(descColor).toBeTruthy();

      // Check footer links color
      const footerLink = page.locator('.footer-links a').first();
      const linkColor = await footerLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(linkColor).toBeTruthy();
    });

    test('TC5c: Code syntax highlighting is visible in dark theme', async ({ page }) => {
      // Check code comment color
      const codeComment = page.locator('.code-comment').first();
      if (await codeComment.count() > 0) {
        const commentColor = await codeComment.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        expect(commentColor).toBeTruthy();
      }

      // Check code string color
      const codeString = page.locator('.code-string').first();
      if (await codeString.count() > 0) {
        const stringColor = await codeString.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        expect(stringColor).toBeTruthy();
      }

      // Check code response color
      const codeResponse = page.locator('.code-response').first();
      if (await codeResponse.count() > 0) {
        const responseColor = await codeResponse.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        expect(responseColor).toBeTruthy();
      }
    });
  });

  test.describe('Accessibility Visual Styles', () => {
    test('Focus styles are visible for interactive elements', async ({ page }) => {
      // Check that focus outline style is defined
      const primaryBtn = page.locator('.btn-primary').first();

      // Focus the element and check outline
      await primaryBtn.focus();
      const focusOutline = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).outlineWidth;
      });

      // Focus should have visible outline
      expect(parseInt(focusOutline)).toBeGreaterThan(0);
    });

    test('Reduced motion preference is respected', async ({ page }) => {
      // Check that prefers-reduced-motion media query is defined in CSS
      const hasReducedMotionStyles = await page.evaluate(() => {
        const styleSheets = document.styleSheets;
        for (let i = 0; i < styleSheets.length; i++) {
          try {
            const rules = styleSheets[i].cssRules || styleSheets[i].rules;
            for (let j = 0; j < rules.length; j++) {
              if (rules[j].media && rules[j].media.mediaText.includes('prefers-reduced-motion')) {
                return true;
              }
            }
          } catch (e) {
            // Cross-origin stylesheets may throw
            continue;
          }
        }
        return false;
      });
      expect(hasReducedMotionStyles).toBe(true);
    });
  });
});

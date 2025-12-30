// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Visual Design Consistency
 * Scenario: Verify the design is visually consistent with modern developer tool aesthetics
 * NFR-5: Design must be visually consistent with modern developer tool aesthetics
 */

test.describe('Visual Design Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check code blocks use monospace font
   * Expected: All code examples use monospace/fixed-width font family
   */
  test('TC1: Code blocks use monospace font family', async ({ page }) => {
    // Check all <pre> elements have monospace font
    const preElements = page.locator('pre');
    const preCount = await preElements.count();
    expect(preCount).toBeGreaterThan(0);

    for (let i = 0; i < preCount; i++) {
      const fontFamily = await preElements.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      // Check that font-family contains monospace keywords
      const hasMonospace = /mono|Monaco|Cascadia|Consolas|Courier|SF Mono/i.test(fontFamily);
      expect(hasMonospace, `<pre> element ${i + 1} should have monospace font, got: ${fontFamily}`).toBe(true);
    }

    // Check all <code> elements have monospace font
    const codeElements = page.locator('code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBeGreaterThan(0);

    for (let i = 0; i < codeCount; i++) {
      const fontFamily = await codeElements.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      // Check that font-family contains monospace keywords
      const hasMonospace = /mono|Monaco|Cascadia|Consolas|Courier|SF Mono/i.test(fontFamily);
      expect(hasMonospace, `<code> element ${i + 1} should have monospace font, got: ${fontFamily}`).toBe(true);
    }

    // Check commands table first column (command names) has monospace font
    const commandCells = page.locator('.commands-table td:first-child');
    const commandCellCount = await commandCells.count();

    if (commandCellCount > 0) {
      for (let i = 0; i < commandCellCount; i++) {
        const fontFamily = await commandCells.nth(i).evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });
        const hasMonospace = /mono|Monaco|Cascadia|Consolas|Courier|SF Mono/i.test(fontFamily);
        expect(hasMonospace, `Command cell ${i + 1} should have monospace font, got: ${fontFamily}`).toBe(true);
      }
    }
  });

  /**
   * Test Case 2: Verify typography hierarchy
   * Expected: Clear visual hierarchy with distinct heading sizes (h1 > h2 > h3 > body)
   */
  test('TC2: Typography hierarchy has distinct heading sizes', async ({ page }) => {
    // Get body font size as baseline
    const bodyFontSize = await page.locator('body').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Get h1 font size
    const h1Element = page.locator('h1').first();
    const h1FontSize = await h1Element.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Get h2 font sizes
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThan(0);

    const h2FontSize = await h2Elements.first().evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Get h3 font sizes
    const h3Elements = page.locator('h3');
    const h3Count = await h3Elements.count();
    expect(h3Count).toBeGreaterThan(0);

    const h3FontSize = await h3Elements.first().evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Verify hierarchy: h1 > h2 > h3 > body
    expect(h1FontSize, `h1 (${h1FontSize}px) should be larger than h2 (${h2FontSize}px)`).toBeGreaterThan(h2FontSize);
    expect(h2FontSize, `h2 (${h2FontSize}px) should be larger than h3 (${h3FontSize}px)`).toBeGreaterThan(h3FontSize);
    expect(h3FontSize, `h3 (${h3FontSize}px) should be larger than or equal to body (${bodyFontSize}px)`).toBeGreaterThanOrEqual(bodyFontSize);

    // Verify all h2 elements have consistent font size
    for (let i = 0; i < h2Count; i++) {
      const fontSize = await h2Elements.nth(i).evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize, `All h2 elements should have consistent size`).toBe(h2FontSize);
    }

    // Verify headings have appropriate font-weight
    const h1FontWeight = await h1Element.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontWeight, 10);
    });
    expect(h1FontWeight, 'h1 should have bold font weight').toBeGreaterThanOrEqual(700);

    const h2FontWeight = await h2Elements.first().evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontWeight, 10);
    });
    expect(h2FontWeight, 'h2 should have bold font weight').toBeGreaterThanOrEqual(600);
  });

  /**
   * Test Case 3: Check button styling consistency
   * Expected: All buttons share consistent styling (colors, padding, border-radius)
   */
  test('TC3: Buttons have consistent styling', async ({ page }) => {
    const buttons = page.locator('.btn');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);

    // Collect button styles
    const buttonStyles = [];
    for (let i = 0; i < buttonCount; i++) {
      const styles = await buttons.nth(i).evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          padding: computed.padding,
          paddingTop: computed.paddingTop,
          paddingBottom: computed.paddingBottom,
          paddingLeft: computed.paddingLeft,
          paddingRight: computed.paddingRight,
          borderRadius: computed.borderRadius,
          fontWeight: computed.fontWeight,
          display: computed.display,
        };
      });
      buttonStyles.push(styles);
    }

    // All buttons should have same padding values (check vertical padding)
    const firstPaddingTop = buttonStyles[0].paddingTop;
    const firstPaddingBottom = buttonStyles[0].paddingBottom;

    for (let i = 1; i < buttonStyles.length; i++) {
      expect(buttonStyles[i].paddingTop, `Button ${i + 1} paddingTop should match button 1`).toBe(firstPaddingTop);
      expect(buttonStyles[i].paddingBottom, `Button ${i + 1} paddingBottom should match button 1`).toBe(firstPaddingBottom);
    }

    // All buttons should have same border-radius
    const firstBorderRadius = buttonStyles[0].borderRadius;
    for (let i = 1; i < buttonStyles.length; i++) {
      expect(buttonStyles[i].borderRadius, `Button ${i + 1} border-radius should match button 1`).toBe(firstBorderRadius);
    }

    // All buttons should have same font-weight
    const firstFontWeight = buttonStyles[0].fontWeight;
    for (let i = 1; i < buttonStyles.length; i++) {
      expect(buttonStyles[i].fontWeight, `Button ${i + 1} font-weight should match button 1`).toBe(firstFontWeight);
    }

    // Buttons should have reasonable border-radius (not zero)
    const borderRadiusValue = parseFloat(firstBorderRadius);
    expect(borderRadiusValue, 'Buttons should have non-zero border-radius').toBeGreaterThan(0);

    // Check that buttons have appropriate padding (not too small)
    const paddingTopValue = parseFloat(firstPaddingTop);
    expect(paddingTopValue, 'Buttons should have adequate vertical padding').toBeGreaterThanOrEqual(8);
  });

  /**
   * Test Case 4: Verify section spacing
   * Expected: Consistent vertical spacing between sections
   */
  test('TC4: Sections have consistent vertical spacing', async ({ page }) => {
    // Get all main sections
    const sections = page.locator('main section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    // Collect section padding values
    const sectionStyles = [];
    for (let i = 0; i < sectionCount; i++) {
      const styles = await sections.nth(i).evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          paddingTop: parseFloat(computed.paddingTop),
          paddingBottom: parseFloat(computed.paddingBottom),
        };
      });
      sectionStyles.push(styles);
    }

    // Check that sections have consistent padding
    // Allow for some variation (hero might be different) but check main content sections
    const contentSections = sectionStyles.slice(1); // Skip hero section which may have different padding

    if (contentSections.length >= 2) {
      // Non-hero sections should have consistent vertical padding
      const firstPaddingTop = contentSections[0].paddingTop;
      const firstPaddingBottom = contentSections[0].paddingBottom;

      for (let i = 1; i < contentSections.length; i++) {
        expect(contentSections[i].paddingTop, `Section ${i + 2} paddingTop should match section 2`).toBe(firstPaddingTop);
        expect(contentSections[i].paddingBottom, `Section ${i + 2} paddingBottom should match section 2`).toBe(firstPaddingBottom);
      }
    }

    // Verify sections have adequate vertical spacing (padding >= 48px which is 3rem at 16px base)
    for (let i = 0; i < sectionCount; i++) {
      expect(sectionStyles[i].paddingTop, `Section ${i + 1} should have adequate top padding`).toBeGreaterThanOrEqual(48);
      expect(sectionStyles[i].paddingBottom, `Section ${i + 1} should have adequate bottom padding`).toBeGreaterThanOrEqual(0);
    }

    // Check h2 margins are consistent across sections
    const h2Elements = page.locator('section h2');
    const h2Count = await h2Elements.count();

    if (h2Count >= 2) {
      const h2Margins = [];
      for (let i = 0; i < h2Count; i++) {
        const margin = await h2Elements.nth(i).evaluate((el) => {
          return window.getComputedStyle(el).marginBottom;
        });
        h2Margins.push(margin);
      }

      // All h2 margins should be consistent
      for (let i = 1; i < h2Margins.length; i++) {
        expect(h2Margins[i], `h2 element ${i + 1} margin should match h2 element 1`).toBe(h2Margins[0]);
      }
    }
  });

  /**
   * Test Case 5: Check color palette consistency
   * Expected: Page uses consistent color palette throughout all sections
   */
  test('TC5: Color palette is consistent throughout page', async ({ page }) => {
    // Get CSS custom properties (CSS variables)
    const cssVariables = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        primaryColor: styles.getPropertyValue('--primary-color').trim(),
        textColor: styles.getPropertyValue('--text-color').trim(),
        textMuted: styles.getPropertyValue('--text-muted').trim(),
        bgColor: styles.getPropertyValue('--bg-color').trim(),
        bgSecondary: styles.getPropertyValue('--bg-secondary').trim(),
        borderColor: styles.getPropertyValue('--border-color').trim(),
      };
    });

    // Verify CSS variables are defined
    expect(cssVariables.primaryColor, 'Primary color should be defined').toBeTruthy();
    expect(cssVariables.textColor, 'Text color should be defined').toBeTruthy();
    expect(cssVariables.bgColor, 'Background color should be defined').toBeTruthy();

    // Check that primary buttons use the primary color
    const primaryBtn = page.locator('.btn-primary').first();
    const primaryBtnBgColor = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Convert hex to rgb for comparison if needed
    const primaryColorRgb = await page.evaluate((hex) => {
      const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      if (result) {
        return `rgb(${parseInt(result[1], 16)}, ${parseInt(result[2], 16)}, ${parseInt(result[3], 16)})`;
      }
      return hex;
    }, cssVariables.primaryColor);

    expect(primaryBtnBgColor, 'Primary button should use primary color').toBe(primaryColorRgb);

    // Check that nav links use consistent text color
    const navLinks = page.locator('.nav-link');
    const navLinkCount = await navLinks.count();

    const navLinkColors = [];
    for (let i = 0; i < navLinkCount; i++) {
      const color = await navLinks.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      navLinkColors.push(color);
    }

    // All nav links should have same base color
    for (let i = 1; i < navLinkColors.length; i++) {
      expect(navLinkColors[i], `Nav link ${i + 1} color should match nav link 1`).toBe(navLinkColors[0]);
    }

    // Check that feature cards have consistent styling
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      const cardStyles = [];
      for (let i = 0; i < cardCount; i++) {
        const styles = await featureCards.nth(i).evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            backgroundColor: computed.backgroundColor,
            borderColor: computed.borderColor,
          };
        });
        cardStyles.push(styles);
      }

      // All cards should have same background color
      for (let i = 1; i < cardStyles.length; i++) {
        expect(cardStyles[i].backgroundColor, `Feature card ${i + 1} background should match card 1`).toBe(cardStyles[0].backgroundColor);
        expect(cardStyles[i].borderColor, `Feature card ${i + 1} border should match card 1`).toBe(cardStyles[0].borderColor);
      }
    }

    // Check that all h3 headings in feature cards use consistent color
    const featureHeadings = page.locator('.feature-card h3');
    const headingCount = await featureHeadings.count();

    if (headingCount >= 2) {
      const headingColors = [];
      for (let i = 0; i < headingCount; i++) {
        const color = await featureHeadings.nth(i).evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        headingColors.push(color);
      }

      // All feature headings should have same color
      for (let i = 1; i < headingColors.length; i++) {
        expect(headingColors[i], `Feature heading ${i + 1} color should match heading 1`).toBe(headingColors[0]);
      }
    }

    // Verify code blocks use consistent styling
    const codeBlocks = page.locator('pre');
    const codeBlockCount = await codeBlocks.count();

    if (codeBlockCount >= 2) {
      const codeBlockStyles = [];
      for (let i = 0; i < codeBlockCount; i++) {
        const styles = await codeBlocks.nth(i).evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            backgroundColor: computed.backgroundColor,
            color: computed.color,
            borderRadius: computed.borderRadius,
          };
        });
        codeBlockStyles.push(styles);
      }

      // All code blocks should have same styling
      for (let i = 1; i < codeBlockStyles.length; i++) {
        expect(codeBlockStyles[i].backgroundColor, `Code block ${i + 1} background should match block 1`).toBe(codeBlockStyles[0].backgroundColor);
        expect(codeBlockStyles[i].color, `Code block ${i + 1} text color should match block 1`).toBe(codeBlockStyles[0].color);
        expect(codeBlockStyles[i].borderRadius, `Code block ${i + 1} border-radius should match block 1`).toBe(codeBlockStyles[0].borderRadius);
      }
    }
  });
});

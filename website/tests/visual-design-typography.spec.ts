import { test, expect } from '@playwright/test';

/**
 * Visual Design and Typography Tests
 *
 * These tests verify consistent visual design and typography throughout the page.
 * Test cases cover:
 * - Heading font consistency
 * - Body text font readability
 * - Code block monospace fonts
 * - Spacing consistency
 * - Button styling consistency
 * - Overall visual polish
 */

test.describe('Visual Design and Typography', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC1: All headings use consistent font family', async ({ page }) => {
    // Check heading font consistency across all heading elements
    // Expected: All headings use consistent font family

    // Get the main heading (h1) font family
    const h1 = page.locator('.hero-section h1');
    const h1FontFamily = await h1.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Get all h2 section titles
    const h2Elements = page.locator('.section-title');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThan(0);

    for (let i = 0; i < h2Count; i++) {
      const h2FontFamily = await h2Elements.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      // All headings should use the same system font stack
      expect(h2FontFamily).toMatch(/-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i);
    }

    // Check h3 elements (feature titles, step titles, etc.)
    const h3Selectors = ['.feature-title', '.step-title', '.explanation-title', '.status-category-title'];
    for (const selector of h3Selectors) {
      const h3Elements = page.locator(selector);
      const count = await h3Elements.count();

      for (let i = 0; i < count; i++) {
        const h3FontFamily = await h3Elements.nth(i).evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });
        // All h3 headings should use the same font family
        expect(h3FontFamily).toMatch(/-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i);
      }
    }

    // Verify h1 also uses system font stack
    expect(h1FontFamily).toMatch(/-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i);
  });

  test('TC2: Body text uses readable, web-safe font', async ({ page }) => {
    // Check body text font for readability
    // Expected: Body text uses readable, web-safe font

    // Check body font family
    const bodyFontFamily = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontFamily;
    });

    // Should use system font stack for optimal readability
    expect(bodyFontFamily).toMatch(/-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i);

    // Check body font size is readable (at least 16px)
    const bodyFontSize = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontSize;
    });
    expect(parseInt(bodyFontSize)).toBeGreaterThanOrEqual(16);

    // Check line height for readability (should be at least 1.5)
    const lineHeight = await page.evaluate(() => {
      return window.getComputedStyle(document.body).lineHeight;
    });
    // Line height should be set (not "normal")
    expect(lineHeight).not.toBe('normal');

    // Check paragraph text readability
    const paragraphs = page.locator('.feature-description, .step-description, .section-subtitle');
    const pCount = await paragraphs.count();
    expect(pCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(pCount, 5); i++) {
      const pFontFamily = await paragraphs.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      const pFontSize = await paragraphs.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      const pLineHeight = await paragraphs.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).lineHeight;
      });

      // Font family should be web-safe
      expect(pFontFamily).toMatch(/-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i);
      // Font size should be readable (at least 14px)
      expect(parseInt(pFontSize)).toBeGreaterThanOrEqual(14);
    }
  });

  test('TC3: Code blocks use monospace font family', async ({ page }) => {
    // Check code font for code blocks
    // Expected: Code blocks use monospace font family

    // Navigate to quick start section where code blocks are
    await page.locator('#quickstart').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);

    // Check all code blocks
    const codeBlocks = page.locator('.code-block');
    const blockCount = await codeBlocks.count();
    expect(blockCount).toBe(4);

    for (let i = 0; i < blockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const fontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should use monospace font family
      expect(fontFamily.toLowerCase()).toMatch(/monaco|menlo|ubuntu mono|consolas|monospace/i);
    }

    // Check inline code elements in command list
    const inlineCode = page.locator('.command-list code');
    const inlineCount = await inlineCode.count();
    expect(inlineCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(inlineCount, 5); i++) {
      const fontFamily = await inlineCode.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Inline code should also use monospace font
      expect(fontFamily.toLowerCase()).toMatch(/monaco|menlo|ubuntu mono|consolas|monospace/i);
    }
  });

  test('TC4: Consistent spacing between sections and elements', async ({ page }) => {
    // Check spacing consistency between sections and elements
    // Expected: Consistent spacing between sections and elements

    // Check section padding consistency
    const sections = [
      { selector: '.features-section', name: 'Features' },
      { selector: '.architecture-section', name: 'Architecture' },
      { selector: '.quick-start-section', name: 'Quick Start' },
      { selector: '.project-status-section', name: 'Project Status' }
    ];

    const sectionPaddings: string[] = [];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element).toBeVisible();

      const padding = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          top: styles.paddingTop,
          bottom: styles.paddingBottom,
          left: styles.paddingLeft,
          right: styles.paddingRight
        };
      });

      // Vertical padding should be consistent (5rem = 80px on desktop)
      sectionPaddings.push(padding.top);
    }

    // Verify all sections have similar vertical padding
    const firstPadding = parseInt(sectionPaddings[0]);
    for (const padding of sectionPaddings) {
      // Allow some tolerance for responsive adjustments
      expect(Math.abs(parseInt(padding) - firstPadding)).toBeLessThanOrEqual(16);
    }

    // Check feature cards have consistent padding
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(7);

    let firstCardPadding: string | null = null;
    for (let i = 0; i < cardCount; i++) {
      const cardPadding = await featureCards.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });

      if (firstCardPadding === null) {
        firstCardPadding = cardPadding;
      } else {
        expect(cardPadding).toBe(firstCardPadding);
      }
    }

    // Check margin between section title and subtitle is consistent
    const sectionTitles = page.locator('.section-title');
    const titleCount = await sectionTitles.count();

    for (let i = 0; i < titleCount; i++) {
      const marginBottom = await sectionTitles.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).marginBottom;
      });
      // All section titles should have the same margin-bottom (1rem = 16px)
      expect(marginBottom).toBe('16px');
    }
  });

  test('TC5: All buttons follow consistent styling pattern', async ({ page }) => {
    // Check button styling consistency
    // Expected: All buttons follow consistent styling pattern

    // Check CTA buttons in hero section
    const ctaPrimary = page.locator('.cta-primary');
    const ctaSecondary = page.locator('.cta-secondary');

    // Verify primary button styles
    const primaryStyles = await ctaPrimary.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius,
        padding: styles.padding,
        fontSize: styles.fontSize,
        fontWeight: styles.fontWeight,
        display: styles.display
      };
    });

    // Primary button should have white background
    expect(primaryStyles.backgroundColor).toBe('rgb(255, 255, 255)');
    // Should have consistent border radius (8px)
    expect(primaryStyles.borderRadius).toBe('8px');
    // Should use flexbox display
    expect(primaryStyles.display).toMatch(/flex/);
    // Font weight should be bold (600+)
    expect(parseInt(primaryStyles.fontWeight)).toBeGreaterThanOrEqual(600);

    // Verify secondary button styles
    const secondaryStyles = await ctaSecondary.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius,
        borderWidth: styles.borderWidth,
        borderStyle: styles.borderStyle,
        fontSize: styles.fontSize,
        fontWeight: styles.fontWeight,
        display: styles.display
      };
    });

    // Secondary button should have transparent background
    expect(secondaryStyles.backgroundColor).toMatch(/rgba\(0, 0, 0, 0\)|transparent/);
    // Should have consistent border radius
    expect(secondaryStyles.borderRadius).toBe('8px');
    // Should have visible border
    expect(secondaryStyles.borderWidth).toBe('2px');
    expect(secondaryStyles.borderStyle).toBe('solid');
    // Font weight should be bold
    expect(parseInt(secondaryStyles.fontWeight)).toBeGreaterThanOrEqual(600);
    // Font size should match primary button
    expect(secondaryStyles.fontSize).toBe(primaryStyles.fontSize);

    // Verify both buttons have same padding pattern
    const primaryPadding = await ctaPrimary.evaluate((el) => {
      return window.getComputedStyle(el).padding;
    });
    const secondaryPadding = await ctaSecondary.evaluate((el) => {
      return window.getComputedStyle(el).padding;
    });
    expect(primaryPadding).toBe(secondaryPadding);

    // Check that buttons have transition effects defined
    const primaryTransition = await ctaPrimary.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(primaryTransition).not.toBe('none');
    expect(primaryTransition).toContain('0.2s');
  });

  test('TC6: No visual artifacts, misalignments, or inconsistencies (manual review aided by automated checks)', async ({ page }) => {
    // Perform automated checks for common visual issues
    // Expected: No visual artifacts, misalignments, or inconsistencies

    // Check for proper box-sizing across the page
    const boxSizingCheck = await page.evaluate(() => {
      const allElements = document.querySelectorAll('*');
      let inconsistentCount = 0;
      for (const el of allElements) {
        const boxSizing = window.getComputedStyle(el).boxSizing;
        if (boxSizing !== 'border-box') {
          inconsistentCount++;
        }
      }
      return inconsistentCount;
    });
    // All elements should use border-box (CSS reset applied)
    expect(boxSizingCheck).toBe(0);

    // Check for proper text color contrast on key elements
    const heroTextColor = await page.locator('.hero-section h1').evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Hero text should be white on gradient background
    expect(heroTextColor).toBe('rgb(255, 255, 255)');

    // Check that section titles have proper text color
    const sectionTitleColor = await page.locator('.section-title').first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Should use the defined text color (#1f2937 = rgb(31, 41, 55))
    expect(sectionTitleColor).toBe('rgb(31, 41, 55)');

    // Check feature cards have proper border and background
    const featureCard = page.locator('.feature-card').first();
    const cardStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderWidth: styles.borderWidth,
        borderStyle: styles.borderStyle,
        borderRadius: styles.borderRadius
      };
    });

    expect(cardStyles.backgroundColor).toBe('rgb(255, 255, 255)');
    expect(cardStyles.borderWidth).toBe('1px');
    expect(cardStyles.borderStyle).toBe('solid');
    expect(cardStyles.borderRadius).toBe('12px');

    // Check grid alignment in features section
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify SVG diagram is properly sized and visible
    const svgDiagram = page.locator('.lsm-diagram');
    await page.locator('#architecture').scrollIntoViewIfNeeded();
    await expect(svgDiagram).toBeVisible();

    const svgBox = await svgDiagram.boundingBox();
    expect(svgBox).not.toBeNull();
    expect(svgBox!.width).toBeGreaterThan(100);
    expect(svgBox!.height).toBeGreaterThan(100);

    // Check that there are no overflow issues
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      const hasHorizontalOverflow = body.scrollWidth > window.innerWidth + 5; // 5px tolerance
      return hasHorizontalOverflow;
    });
    expect(bodyOverflow).toBe(false);

    // Check status items have proper alignment
    const statusItems = page.locator('.status-item');
    const statusCount = await statusItems.count();
    expect(statusCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(statusCount, 5); i++) {
      const display = await statusItems.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      const alignItems = await statusItems.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).alignItems;
      });

      expect(display).toBe('flex');
      expect(alignItems).toBe('center');
    }

    // Check code block styling consistency
    const codeBlocks = page.locator('.code-block');
    const blockCount = await codeBlocks.count();

    let firstBlockBg: string | null = null;
    let firstBlockRadius: string | null = null;

    for (let i = 0; i < blockCount; i++) {
      const styles = await codeBlocks.nth(i).evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          backgroundColor: computed.backgroundColor,
          borderRadius: computed.borderRadius
        };
      });

      if (firstBlockBg === null) {
        firstBlockBg = styles.backgroundColor;
        firstBlockRadius = styles.borderRadius;
      } else {
        expect(styles.backgroundColor).toBe(firstBlockBg);
        expect(styles.borderRadius).toBe(firstBlockRadius);
      }
    }
  });
});

test.describe('Visual Design - Color Scheme Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('CSS custom properties define consistent color palette', async ({ page }) => {
    // Verify CSS variables are properly defined
    const rootVariables = await page.evaluate(() => {
      const rootStyles = getComputedStyle(document.documentElement);
      return {
        primaryColor: rootStyles.getPropertyValue('--primary-color').trim(),
        primaryHover: rootStyles.getPropertyValue('--primary-hover').trim(),
        secondaryColor: rootStyles.getPropertyValue('--secondary-color').trim(),
        textColor: rootStyles.getPropertyValue('--text-color').trim(),
        textLight: rootStyles.getPropertyValue('--text-light').trim(),
        background: rootStyles.getPropertyValue('--background').trim()
      };
    });

    // Verify all CSS variables are defined
    expect(rootVariables.primaryColor).toBe('#2563eb');
    expect(rootVariables.primaryHover).toBe('#1d4ed8');
    expect(rootVariables.secondaryColor).toBe('#1f2937');
    expect(rootVariables.textColor).toBe('#1f2937');
    expect(rootVariables.textLight).toBe('#6b7280');
    expect(rootVariables.background).toBe('#ffffff');
  });

  test('Color scheme is applied consistently throughout page', async ({ page }) => {
    // Check primary color is used consistently
    const primaryColorElements = await page.evaluate(() => {
      const elements = [];

      // Check explanation title borders
      const explanationTitles = document.querySelectorAll('.explanation-title');
      for (const el of explanationTitles) {
        const borderColor = window.getComputedStyle(el).borderBottomColor;
        elements.push({ element: 'explanation-title', property: 'border-color', value: borderColor });
      }

      // Check command category borders
      const commandCategories = document.querySelectorAll('.command-category h4');
      for (const el of commandCategories) {
        const borderColor = window.getComputedStyle(el).borderBottomColor;
        elements.push({ element: 'command-category', property: 'border-color', value: borderColor });
      }

      return elements;
    });

    // All primary-colored elements should use the same blue (#2563eb = rgb(37, 99, 235))
    for (const item of primaryColorElements) {
      expect(item.value).toBe('rgb(37, 99, 235)');
    }

    // Check secondary/footer color
    const footerBg = await page.locator('footer').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Footer should use secondary color (#1f2937 = rgb(31, 41, 55))
    expect(footerBg).toBe('rgb(31, 41, 55)');

    // Check completed features use consistent green
    const completedIcon = page.locator('.status-item[data-status="completed"] .status-icon').first();
    const completedBg = await completedIcon.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should use light green (#d1fae5 = rgb(209, 250, 229))
    expect(completedBg).toBe('rgb(209, 250, 229)');

    // Check planned features use consistent yellow/amber
    const plannedIcon = page.locator('.status-item[data-status="planned"] .status-icon').first();
    const plannedBg = await plannedIcon.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should use light yellow (#fef3c7 = rgb(254, 243, 199))
    expect(plannedBg).toBe('rgb(254, 243, 199)');
  });
});

test.describe('Visual Design - Visual Hierarchy', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Visual hierarchy is clear with proper font sizing progression', async ({ page }) => {
    // Get font sizes for different heading levels
    const h1Size = await page.locator('.hero-section h1').evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });

    const h2Size = await page.locator('.section-title').first().evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });

    const h3Size = await page.locator('.feature-title').first().evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });

    const bodySize = await page.evaluate(() => {
      return parseInt(window.getComputedStyle(document.body).fontSize);
    });

    // Verify proper hierarchy: h1 > h2 > h3 > body
    expect(h1Size).toBeGreaterThan(h2Size);
    expect(h2Size).toBeGreaterThan(h3Size);
    expect(h3Size).toBeGreaterThan(bodySize);

    // Verify significant difference between levels (at least 4px)
    expect(h1Size - h2Size).toBeGreaterThanOrEqual(4);
    expect(h2Size - h3Size).toBeGreaterThanOrEqual(4);
  });

  test('Important elements stand out with proper visual weight', async ({ page }) => {
    // Check h1 has bold weight
    const h1Weight = await page.locator('.hero-section h1').evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontWeight);
    });
    expect(h1Weight).toBeGreaterThanOrEqual(700);

    // Check section titles have bold weight
    const h2Weight = await page.locator('.section-title').first().evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontWeight);
    });
    expect(h2Weight).toBeGreaterThanOrEqual(700);

    // Check feature titles have semi-bold or bold weight
    const h3Weight = await page.locator('.feature-title').first().evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontWeight);
    });
    expect(h3Weight).toBeGreaterThanOrEqual(600);

    // Check CTA buttons stand out
    const ctaButtonBox = await page.locator('.cta-primary').boundingBox();
    expect(ctaButtonBox).not.toBeNull();
    expect(ctaButtonBox!.width).toBeGreaterThan(120);
    expect(ctaButtonBox!.height).toBeGreaterThan(40);
  });
});

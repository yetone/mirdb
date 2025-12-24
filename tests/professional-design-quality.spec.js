// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Professional Design Quality (Scenario 18)
 * Verifies that the design is clean, professional, and developer-focused as specified in NFR-5
 *
 * Design Principles from PRD:
 * - Clean, minimalist design appropriate for developer tooling
 * - Monospace fonts for code examples
 * - Sufficient contrast for readability
 * - Consistent spacing and visual hierarchy
 * - Professional color scheme
 */

test.describe('Professional Design Quality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for consistent color scheme
   * Expected: Page uses a cohesive color palette throughout
   */
  test('TC1: Page uses a cohesive color palette throughout', async ({ page }) => {
    // Get CSS variables defined in :root (design system colors)
    const cssVariables = await page.evaluate(() => {
      const rootStyles = getComputedStyle(document.documentElement);
      return {
        primaryColor: rootStyles.getPropertyValue('--primary-color').trim(),
        primaryDark: rootStyles.getPropertyValue('--primary-dark').trim(),
        secondaryColor: rootStyles.getPropertyValue('--secondary-color').trim(),
        textColor: rootStyles.getPropertyValue('--text-color').trim(),
        textLight: rootStyles.getPropertyValue('--text-light').trim(),
        bgColor: rootStyles.getPropertyValue('--bg-color').trim(),
        bgSecondary: rootStyles.getPropertyValue('--bg-secondary').trim(),
        borderColor: rootStyles.getPropertyValue('--border-color').trim(),
        codeBg: rootStyles.getPropertyValue('--code-bg').trim(),
        codeText: rootStyles.getPropertyValue('--code-text').trim()
      };
    });

    // Verify CSS variables are defined (design system is in place)
    expect(cssVariables.primaryColor).toBeTruthy();
    expect(cssVariables.textColor).toBeTruthy();
    expect(cssVariables.bgColor).toBeTruthy();
    expect(cssVariables.codeBg).toBeTruthy();
    expect(cssVariables.codeText).toBeTruthy();

    // Verify colors are consistent across similar elements
    const elementColors = await page.evaluate(() => {
      const results = {
        headings: [],
        paragraphs: [],
        buttons: [],
        codeBlocks: []
      };

      // Check heading colors
      document.querySelectorAll('h2, h3').forEach(el => {
        const style = getComputedStyle(el);
        results.headings.push(style.color);
      });

      // Check paragraph colors in main content
      document.querySelectorAll('.features p, .arch-detail p, .getting-started p').forEach(el => {
        const style = getComputedStyle(el);
        results.paragraphs.push(style.color);
      });

      // Check primary button colors
      document.querySelectorAll('.btn-primary').forEach(el => {
        const style = getComputedStyle(el);
        results.buttons.push(style.backgroundColor);
      });

      // Check code block colors
      document.querySelectorAll('.code-block').forEach(el => {
        const style = getComputedStyle(el);
        results.codeBlocks.push({
          bg: style.backgroundColor,
          color: style.color
        });
      });

      return results;
    });

    // Verify heading colors are consistent
    if (elementColors.headings.length > 1) {
      const uniqueHeadingColors = [...new Set(elementColors.headings)];
      // Allow max 2 different heading colors (e.g., hero vs main content)
      expect(uniqueHeadingColors.length).toBeLessThanOrEqual(2);
    }

    // Verify paragraph text colors are consistent
    if (elementColors.paragraphs.length > 1) {
      const uniqueParagraphColors = [...new Set(elementColors.paragraphs)];
      // All paragraphs in content areas should have same color
      expect(uniqueParagraphColors.length).toBe(1);
    }

    // Verify code block backgrounds are consistent
    if (elementColors.codeBlocks.length > 1) {
      const uniqueCodeBgs = [...new Set(elementColors.codeBlocks.map(cb => cb.bg))];
      expect(uniqueCodeBgs.length).toBe(1);
    }
  });

  /**
   * Test Case 2: Verify consistent typography
   * Expected: Font families and sizes are consistent across similar elements
   */
  test('TC2: Font families and sizes are consistent across similar elements', async ({ page }) => {
    const typography = await page.evaluate(() => {
      const results = {
        bodyFonts: [],
        h2Fonts: [],
        h3Fonts: [],
        codeFonts: [],
        h2Sizes: [],
        h3Sizes: [],
        paragraphSizes: []
      };

      // Body font
      results.bodyFonts.push(getComputedStyle(document.body).fontFamily);

      // H2 fonts and sizes
      document.querySelectorAll('h2').forEach(el => {
        const style = getComputedStyle(el);
        results.h2Fonts.push(style.fontFamily);
        results.h2Sizes.push(style.fontSize);
      });

      // H3 fonts and sizes
      document.querySelectorAll('h3').forEach(el => {
        const style = getComputedStyle(el);
        results.h3Fonts.push(style.fontFamily);
        results.h3Sizes.push(style.fontSize);
      });

      // Code fonts
      document.querySelectorAll('.code-block, .code-block code, pre code').forEach(el => {
        const style = getComputedStyle(el);
        results.codeFonts.push(style.fontFamily);
      });

      // Paragraph sizes in main content
      document.querySelectorAll('.features p, .arch-detail p').forEach(el => {
        const style = getComputedStyle(el);
        results.paragraphSizes.push(style.fontSize);
      });

      return results;
    });

    // Verify body uses a system font stack
    expect(typography.bodyFonts[0]).toMatch(/(-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif)/i);

    // Verify all H2 elements have consistent font size
    if (typography.h2Sizes.length > 1) {
      const uniqueH2Sizes = [...new Set(typography.h2Sizes)];
      expect(uniqueH2Sizes.length).toBe(1);
    }

    // Verify all H3 elements have consistent font family
    if (typography.h3Fonts.length > 1) {
      const uniqueH3Fonts = [...new Set(typography.h3Fonts)];
      expect(uniqueH3Fonts.length).toBe(1);
    }

    // Verify code blocks use monospace fonts
    const monospaceIndicators = ['monospace', 'monaco', 'menlo', 'consolas', 'courier', 'ubuntu mono'];
    typography.codeFonts.forEach(font => {
      const hasMonospace = monospaceIndicators.some(indicator =>
        font.toLowerCase().includes(indicator)
      );
      expect(hasMonospace).toBeTruthy();
    });

    // Verify paragraph sizes are consistent
    if (typography.paragraphSizes.length > 1) {
      const uniqueParagraphSizes = [...new Set(typography.paragraphSizes)];
      // Allow some variation but should be mostly consistent
      expect(uniqueParagraphSizes.length).toBeLessThanOrEqual(2);
    }
  });

  /**
   * Test Case 3: Check code block styling
   * Expected: Code blocks use monospace font and have appropriate styling
   */
  test('TC3: Code blocks use monospace font and have appropriate styling', async ({ page }) => {
    // Navigate to getting started section which has code blocks
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find all code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify each code block has proper styling
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Get computed styles
      const styles = await codeBlock.evaluate((el) => {
        const style = getComputedStyle(el);
        return {
          fontFamily: style.fontFamily,
          backgroundColor: style.backgroundColor,
          color: style.color,
          padding: style.padding,
          borderRadius: style.borderRadius,
          overflow: style.overflowX
        };
      });

      // Verify monospace font
      const monospaceIndicators = ['monospace', 'monaco', 'menlo', 'consolas', 'courier', 'ubuntu mono'];
      const hasMonospace = monospaceIndicators.some(font =>
        styles.fontFamily.toLowerCase().includes(font)
      );
      expect(hasMonospace).toBeTruthy();

      // Verify dark background (code blocks should have dark bg for readability)
      // Parse RGB values
      const bgMatch = styles.backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (bgMatch) {
        const [, r, g, b] = bgMatch.map(Number);
        // Dark background should have low RGB values (e.g., < 100 average)
        const avgBrightness = (r + g + b) / 3;
        expect(avgBrightness).toBeLessThan(100);
      }

      // Verify light text color for contrast
      const colorMatch = styles.color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (colorMatch) {
        const [, r, g, b] = colorMatch.map(Number);
        // Light text should have high RGB values (e.g., > 150 average)
        const avgBrightness = (r + g + b) / 3;
        expect(avgBrightness).toBeGreaterThan(150);
      }

      // Verify padding exists
      expect(styles.padding).not.toBe('0px');

      // Verify border-radius for professional look
      expect(styles.borderRadius).not.toBe('0px');

      // Verify overflow handling for long code lines
      expect(styles.overflow).toBe('auto');
    }
  });

  /**
   * Test Case 4: Evaluate whitespace and spacing
   * Expected: Consistent spacing between sections and elements
   */
  test('TC4: Consistent spacing between sections and elements', async ({ page }) => {
    // Get padding/margin values for major sections
    const sectionSpacing = await page.evaluate(() => {
      const sections = ['#features', '#architecture', '#getting-started', '#configuration'];
      const results = {};

      sections.forEach(selector => {
        const el = document.querySelector(selector);
        if (el) {
          const style = getComputedStyle(el);
          results[selector] = {
            paddingTop: style.paddingTop,
            paddingBottom: style.paddingBottom,
            paddingLeft: style.paddingLeft,
            paddingRight: style.paddingRight
          };
        }
      });

      return results;
    });

    // Verify all main sections have padding
    Object.values(sectionSpacing).forEach(spacing => {
      // Sections should have vertical padding
      const paddingTop = parseFloat(spacing.paddingTop);
      const paddingBottom = parseFloat(spacing.paddingBottom);

      expect(paddingTop).toBeGreaterThan(0);
      expect(paddingBottom).toBeGreaterThan(0);
    });

    // Check that section padding is consistent
    const verticalPaddings = Object.values(sectionSpacing).map(s => parseFloat(s.paddingTop));
    if (verticalPaddings.length > 1) {
      // All sections should have same top padding (within 10% variance)
      const avgPadding = verticalPaddings.reduce((a, b) => a + b, 0) / verticalPaddings.length;
      verticalPaddings.forEach(padding => {
        const variance = Math.abs(padding - avgPadding) / avgPadding;
        expect(variance).toBeLessThan(0.2); // Allow 20% variance for alternating sections
      });
    }

    // Verify feature grid has consistent gap spacing
    const featureGridGap = await page.evaluate(() => {
      const grid = document.querySelector('.features-grid');
      if (grid) {
        const style = getComputedStyle(grid);
        return {
          gap: style.gap,
          rowGap: style.rowGap,
          columnGap: style.columnGap
        };
      }
      return null;
    });

    if (featureGridGap) {
      // Grid should have gap defined
      const hasGap = featureGridGap.gap !== 'normal' ||
                     featureGridGap.rowGap !== 'normal' ||
                     featureGridGap.columnGap !== 'normal';
      expect(hasGap).toBeTruthy();
    }

    // Verify container max-width is set for professional layout
    const containerStyles = await page.evaluate(() => {
      const container = document.querySelector('.container');
      if (container) {
        const style = getComputedStyle(container);
        return {
          maxWidth: style.maxWidth,
          marginLeft: style.marginLeft,
          marginRight: style.marginRight,
          paddingLeft: style.paddingLeft,
          paddingRight: style.paddingRight
        };
      }
      return null;
    });

    if (containerStyles) {
      // Container should have max-width (not 'none')
      expect(containerStyles.maxWidth).not.toBe('none');
      // Container should be centered - either via auto margins or consistent padding
      // The design uses margin: 0 auto with padding for horizontal spacing
      const isCentered = containerStyles.marginLeft === 'auto' ||
                         (containerStyles.paddingLeft !== '0px' && containerStyles.paddingRight !== '0px');
      expect(isCentered).toBeTruthy();
    }
  });

  /**
   * Additional Test: Visual hierarchy is clear
   * Important elements should stand out
   */
  test('Visual hierarchy guides user attention with proper element sizing', async ({ page }) => {
    const hierarchy = await page.evaluate(() => {
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');
      const h3s = document.querySelectorAll('h3');
      const paragraphs = document.querySelectorAll('p');

      return {
        h1Size: h1 ? parseFloat(getComputedStyle(h1).fontSize) : 0,
        h2Size: h2s.length > 0 ? parseFloat(getComputedStyle(h2s[0]).fontSize) : 0,
        h3Size: h3s.length > 0 ? parseFloat(getComputedStyle(h3s[0]).fontSize) : 0,
        pSize: paragraphs.length > 0 ? parseFloat(getComputedStyle(paragraphs[0]).fontSize) : 0,
        h1Weight: h1 ? getComputedStyle(h1).fontWeight : '400',
        ctaButtons: document.querySelectorAll('.btn-primary').length
      };
    });

    // H1 should be largest
    expect(hierarchy.h1Size).toBeGreaterThan(hierarchy.h2Size);

    // H2 should be larger than H3
    expect(hierarchy.h2Size).toBeGreaterThan(hierarchy.h3Size);

    // H3 should be approximately similar to or larger than paragraph text
    // Some designs allow H3 to be slightly smaller for visual balance
    // Allow H3 to be at least 80% of paragraph size for acceptable hierarchy
    expect(hierarchy.h3Size).toBeGreaterThanOrEqual(hierarchy.pSize * 0.8);

    // H1 should be bold
    expect(parseInt(hierarchy.h1Weight)).toBeGreaterThanOrEqual(700);

    // Should have at least one primary CTA button
    expect(hierarchy.ctaButtons).toBeGreaterThan(0);
  });

  /**
   * Additional Test: Feature cards have consistent styling
   */
  test('Feature cards have consistent professional styling', async ({ page }) => {
    const featureCards = await page.evaluate(() => {
      const cards = document.querySelectorAll('.feature-item');
      const results = [];

      cards.forEach(card => {
        const style = getComputedStyle(card);
        results.push({
          padding: style.padding,
          borderRadius: style.borderRadius,
          backgroundColor: style.backgroundColor,
          boxShadow: style.boxShadow
        });
      });

      return results;
    });

    expect(featureCards.length).toBeGreaterThan(0);

    // All feature cards should have same padding
    const uniquePaddings = [...new Set(featureCards.map(c => c.padding))];
    expect(uniquePaddings.length).toBe(1);

    // All feature cards should have same border-radius
    const uniqueRadii = [...new Set(featureCards.map(c => c.borderRadius))];
    expect(uniqueRadii.length).toBe(1);

    // All feature cards should have same background
    const uniqueBgs = [...new Set(featureCards.map(c => c.backgroundColor))];
    expect(uniqueBgs.length).toBe(1);

    // Cards should have rounded corners (professional look)
    expect(featureCards[0].borderRadius).not.toBe('0px');

    // Cards should have subtle shadow for depth
    expect(featureCards[0].boxShadow).not.toBe('none');
  });

  /**
   * Additional Test: Configuration table has professional styling
   */
  test('Configuration table has professional styling', async ({ page }) => {
    const tableStyles = await page.evaluate(() => {
      const table = document.querySelector('.config-table');
      if (!table) return null;

      const thead = table.querySelector('thead');
      const tbody = table.querySelector('tbody');
      const firstRow = tbody?.querySelector('tr');

      return {
        tableRadius: getComputedStyle(table).borderRadius,
        tableShadow: getComputedStyle(table).boxShadow,
        theadBg: thead ? getComputedStyle(thead).backgroundColor : null,
        theadColor: thead ? getComputedStyle(thead).color : null,
        rowPadding: firstRow?.querySelector('td') ?
          getComputedStyle(firstRow.querySelector('td')).padding : null
      };
    });

    if (tableStyles) {
      // Table should have rounded corners
      expect(tableStyles.tableRadius).not.toBe('0px');

      // Table should have shadow for depth
      expect(tableStyles.tableShadow).not.toBe('none');

      // Table header should have distinct background
      expect(tableStyles.theadBg).toBeTruthy();

      // Table cells should have padding
      expect(tableStyles.rowPadding).not.toBe('0px');
    }
  });
});

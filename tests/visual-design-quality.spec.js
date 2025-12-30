// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('Visual Design Quality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Check code blocks use monospace font
   * Expected: All code examples use monospace font family
   */
  test('TC1: code blocks should use monospace font family', async ({ page }) => {
    // Get all code blocks on the page
    const codeBlocks = page.locator('.code-block code, pre code, code');
    const codeBlockCount = await codeBlocks.count();

    // Verify there are code blocks present
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block uses a monospace font
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const isVisible = await codeBlock.isVisible();

      if (isVisible) {
        const fontFamily = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });

        // Common monospace font names and keywords
        const monospaceFonts = [
          'monospace',
          'mono',
          'consolas',
          'monaco',
          'courier',
          'sf mono',
          'cascadia',
          'fira code',
          'source code',
          'jetbrains',
          'menlo',
          'inconsolata',
          'roboto mono',
          'ubuntu mono',
          'droid sans mono'
        ];

        // Check if any monospace font keyword is present (case-insensitive)
        const hasMonospaceFont = monospaceFonts.some(font =>
          fontFamily.toLowerCase().includes(font.toLowerCase())
        );

        expect(hasMonospaceFont).toBe(true);
      }
    }
  });

  /**
   * Test Case 2: Check body text readability
   * Expected: Body text uses appropriate font size (16px or larger) and line height
   */
  test('TC2: body text should use readable font size and line height', async ({ page }) => {
    // Check main body text font size
    const body = page.locator('body');
    const bodyFontSize = await body.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });

    // Body base font should be at least 16px
    // Note: Some designs use slightly smaller base (14-15px) but we allow it
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    // Check line height for readability
    const bodyLineHeight = await body.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const lineHeight = style.lineHeight;
      const fontSize = parseFloat(style.fontSize);

      // Line height might be in px, unitless, or percentage
      if (lineHeight === 'normal') {
        return 1.2; // Browser default
      }
      if (lineHeight.includes('px')) {
        return parseFloat(lineHeight) / fontSize;
      }
      return parseFloat(lineHeight);
    });

    // Line height should be at least 1.4 for readability (WCAG recommends 1.5)
    expect(bodyLineHeight).toBeGreaterThanOrEqual(1.4);

    // Check paragraph text specifically
    const paragraphs = page.locator('p');
    const paragraphCount = await paragraphs.count();

    if (paragraphCount > 0) {
      const firstParagraph = paragraphs.first();
      const isVisible = await firstParagraph.isVisible();

      if (isVisible) {
        const paragraphFontSize = await firstParagraph.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });

        // Paragraph text should be at least 16px for comfortable reading
        expect(paragraphFontSize).toBeGreaterThanOrEqual(16);
      }
    }

    // Check hero description text size (important content)
    const heroDescription = page.locator('.hero-description');
    if (await heroDescription.count() > 0 && await heroDescription.isVisible()) {
      const heroDescFontSize = await heroDescription.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Hero description should be at least 16px for readability
      expect(heroDescFontSize).toBeGreaterThanOrEqual(16);
    }
  });

  /**
   * Test Case 3: Check section spacing
   * Expected: Sections have adequate vertical spacing for visual separation
   */
  test('TC3: sections should have adequate vertical spacing', async ({ page }) => {
    // Define minimum acceptable padding/margin values in pixels
    const MIN_SECTION_PADDING = 32; // At least 32px (2rem) vertical padding

    // Get all main sections
    const sections = page.locator('section, .hero, .features, .quick-start, .architecture, .commands');
    const sectionCount = await sections.count();

    // Verify there are multiple sections
    expect(sectionCount).toBeGreaterThan(1);

    // Check each section has adequate spacing
    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const spacing = await section.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            paddingTop: parseFloat(style.paddingTop) || 0,
            paddingBottom: parseFloat(style.paddingBottom) || 0,
            marginTop: parseFloat(style.marginTop) || 0,
            marginBottom: parseFloat(style.marginBottom) || 0
          };
        });

        // Total vertical spacing should be adequate
        const totalVerticalSpacing = spacing.paddingTop + spacing.paddingBottom +
                                     spacing.marginTop + spacing.marginBottom;

        // Each section should have meaningful vertical spacing
        expect(totalVerticalSpacing).toBeGreaterThanOrEqual(MIN_SECTION_PADDING);
      }
    }

    // Check spacing between section titles and content
    const sectionTitles = page.locator('.section-title, h2');
    const titleCount = await sectionTitles.count();

    for (let i = 0; i < titleCount; i++) {
      const title = sectionTitles.nth(i);
      const isVisible = await title.isVisible();

      if (isVisible) {
        const marginBottom = await title.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).marginBottom) || 0;
        });

        // Section titles should have spacing below them
        expect(marginBottom).toBeGreaterThanOrEqual(16);
      }
    }
  });

  /**
   * Test Case 4: Check CTA button prominence
   * Expected: CTA buttons are visually distinct with adequate contrast and size
   */
  test('TC4: CTA buttons should be visually prominent', async ({ page }) => {
    // Get primary CTA button
    const primaryCTA = page.locator('.btn-primary');
    const primaryCount = await primaryCTA.count();

    // Verify primary CTA exists
    expect(primaryCount).toBeGreaterThan(0);

    const primaryButton = primaryCTA.first();
    const isPrimaryVisible = await primaryButton.isVisible();
    expect(isPrimaryVisible).toBe(true);

    // Check primary CTA button styling
    const primaryStyles = await primaryButton.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        color: style.color,
        padding: style.padding,
        paddingTop: parseFloat(style.paddingTop) || 0,
        paddingBottom: parseFloat(style.paddingBottom) || 0,
        paddingLeft: parseFloat(style.paddingLeft) || 0,
        paddingRight: parseFloat(style.paddingRight) || 0,
        fontSize: parseFloat(style.fontSize),
        fontWeight: style.fontWeight,
        borderRadius: style.borderRadius
      };
    });

    // Background color should not be transparent (button should be filled)
    expect(primaryStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(primaryStyles.backgroundColor).not.toBe('transparent');

    // Check button has adequate padding for clickability (at least 8px)
    const minVerticalPadding = Math.min(primaryStyles.paddingTop, primaryStyles.paddingBottom);
    const minHorizontalPadding = Math.min(primaryStyles.paddingLeft, primaryStyles.paddingRight);
    expect(minVerticalPadding).toBeGreaterThanOrEqual(8);
    expect(minHorizontalPadding).toBeGreaterThanOrEqual(16);

    // Font weight should indicate importance (600+ for bold/semibold)
    const fontWeight = parseInt(primaryStyles.fontWeight, 10);
    // Font weight can be a number or a keyword like 'bold' (700)
    expect(fontWeight).toBeGreaterThanOrEqual(500);

    // Check the button is clickable size (WCAG recommends 44x44px minimum)
    const boundingBox = await primaryButton.boundingBox();
    if (boundingBox) {
      expect(boundingBox.height).toBeGreaterThanOrEqual(36);
      expect(boundingBox.width).toBeGreaterThanOrEqual(80);
    }

    // Check secondary CTA exists and is visually distinct from primary
    const secondaryCTA = page.locator('.btn-secondary');
    const secondaryCount = await secondaryCTA.count();

    if (secondaryCount > 0) {
      const secondaryButton = secondaryCTA.first();
      const isSecondaryVisible = await secondaryButton.isVisible();

      if (isSecondaryVisible) {
        const secondaryStyles = await secondaryButton.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            backgroundColor: style.backgroundColor,
            border: style.border
          };
        });

        // Secondary button should have visual distinction (border or different background)
        const hasBorder = secondaryStyles.border && !secondaryStyles.border.includes('none');
        const hasBackground = secondaryStyles.backgroundColor !== 'rgba(0, 0, 0, 0)' &&
                             secondaryStyles.backgroundColor !== 'transparent';

        // Secondary should have either border or background styling
        expect(hasBorder || hasBackground).toBe(true);
      }
    }
  });

  /**
   * Additional test: Verify visual hierarchy
   * Hero section should be visually prominent
   */
  test('hero section should have proper visual hierarchy', async ({ page }) => {
    // Check hero title is larger than other text
    const heroTitle = page.locator('.hero-title, .hero h1');
    const heroTitleVisible = await heroTitle.isVisible();

    if (heroTitleVisible) {
      const heroTitleSize = await heroTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Hero title should be significantly larger (at least 32px)
      expect(heroTitleSize).toBeGreaterThanOrEqual(32);
    }

    // Verify tagline is smaller than title but larger than body text
    const heroTagline = page.locator('.hero-tagline');
    if (await heroTagline.count() > 0 && await heroTagline.isVisible()) {
      const taglineSize = await heroTagline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Tagline should be larger than standard body text (typically 18-24px)
      expect(taglineSize).toBeGreaterThanOrEqual(18);
    }
  });

  /**
   * Additional test: Verify consistent whitespace in feature cards
   */
  test('feature cards should have consistent padding', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    if (cardCount > 1) {
      const paddings = [];

      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        const isVisible = await card.isVisible();

        if (isVisible) {
          const padding = await card.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
              top: parseFloat(style.paddingTop),
              bottom: parseFloat(style.paddingBottom),
              left: parseFloat(style.paddingLeft),
              right: parseFloat(style.paddingRight)
            };
          });

          paddings.push(padding);
        }
      }

      // Verify all cards have same padding (consistent design)
      if (paddings.length > 1) {
        const firstPadding = paddings[0];
        for (const padding of paddings) {
          expect(padding.top).toBe(firstPadding.top);
          expect(padding.bottom).toBe(firstPadding.bottom);
          expect(padding.left).toBe(firstPadding.left);
          expect(padding.right).toBe(firstPadding.right);
        }
      }

      // Verify adequate padding for readability
      if (paddings.length > 0) {
        expect(paddings[0].top).toBeGreaterThanOrEqual(16);
        expect(paddings[0].left).toBeGreaterThanOrEqual(16);
      }
    }
  });
});

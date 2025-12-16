// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Theme and Styling Consistency
 *
 * These tests verify consistent theming and styling throughout the page:
 * - Test Case 1: Primary brand color is used consistently for CTAs and highlights
 * - Test Case 2: Same font family is used for body text throughout
 * - Test Case 3: All buttons follow consistent style patterns
 * - Test Case 4: Margins and padding follow consistent spacing scale
 */

test.describe('Theme and Styling Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Primary brand color is used consistently for CTAs and highlights', async ({ page }) => {
    // Define the expected primary color (from CSS variables)
    // --color-primary: #e05d44 translates to rgb(224, 93, 68)
    const expectedPrimaryColor = 'rgb(224, 93, 68)';
    const expectedPrimaryColorHex = '#e05d44';

    // Test 1: Primary CTA button uses primary color
    const primaryBtn = page.getByTestId('cta-get-started');
    await expect(primaryBtn).toBeVisible();

    const primaryBtnStyles = await primaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        borderColor: style.borderColor
      };
    });

    expect(
      primaryBtnStyles.backgroundColor,
      'Primary CTA button should use primary brand color as background'
    ).toBe(expectedPrimaryColor);

    expect(
      primaryBtnStyles.borderColor,
      'Primary CTA button should use primary brand color for border'
    ).toBe(expectedPrimaryColor);

    // Test 2: Feature icons use primary color
    const featureIcons = page.locator('.feature-icon');
    const iconCount = await featureIcons.count();
    expect(iconCount, 'Should have feature icons').toBeGreaterThan(0);

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      const iconColor = await icon.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      expect(
        iconColor,
        `Feature icon ${i + 1} should use primary brand color`
      ).toBe(expectedPrimaryColor);
    }

    // Test 3: Skip link uses primary color as background
    const skipLink = page.getByTestId('skip-link');
    await skipLink.focus();

    const skipLinkStyles = await skipLink.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    expect(
      skipLinkStyles,
      'Skip link should use primary brand color as background'
    ).toBe(expectedPrimaryColor);

    // Test 4: Focus indicators use primary color
    const navLink = page.locator('.nav-links a').first();
    await navLink.focus();

    const focusOutlineColor = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).outlineColor;
    });

    expect(
      focusOutlineColor,
      'Focus indicators should use primary brand color'
    ).toBe(expectedPrimaryColor);

    // Test 5: Explanation list border uses primary color
    const explanationItems = page.locator('.explanation-list li');
    const explanationItemCount = await explanationItems.count();
    expect(explanationItemCount, 'Should have explanation list items').toBeGreaterThan(0);

    const firstItemBorder = await explanationItems.first().evaluate((el) => {
      return window.getComputedStyle(el).borderLeftColor;
    });

    expect(
      firstItemBorder,
      'Explanation list items should use primary brand color for left border'
    ).toBe(expectedPrimaryColor);
  });

  test('Test Case 2: Same font family is used for body text throughout', async ({ page }) => {
    // Define the expected font family (from CSS variables)
    // --font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif
    const expectedFontFamilyPattern = /-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i;

    // Test body font family
    const bodyFont = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(
      bodyFont,
      'Body should use the defined font family'
    ).toMatch(expectedFontFamilyPattern);

    // Test hero tagline font family
    const taglineFont = await page.getByTestId('hero-tagline').evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(
      taglineFont,
      'Hero tagline should use the same font family as body'
    ).toBe(bodyFont);

    // Test feature descriptions font family
    const featureDescriptions = page.locator('.feature-description');
    const descCount = await featureDescriptions.count();
    expect(descCount, 'Should have feature descriptions').toBeGreaterThan(0);

    for (let i = 0; i < Math.min(descCount, 4); i++) {
      const descFont = await featureDescriptions.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      expect(
        descFont,
        `Feature description ${i + 1} should use the same font family as body`
      ).toBe(bodyFont);
    }

    // Test architecture description font family
    const archDescFont = await page.getByTestId('architecture-description').evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(
      archDescFont,
      'Architecture description should use the same font family as body'
    ).toBe(bodyFont);

    // Test footer font family
    const footerFont = await page.locator('.footer').evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(
      footerFont,
      'Footer should use the same font family as body'
    ).toBe(bodyFont);

    // Test navigation links font family
    const navLinkFont = await page.locator('.nav-links a').first().evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(
      navLinkFont,
      'Navigation links should use the same font family as body'
    ).toBe(bodyFont);

    // Test explanation list font family
    const explanationFont = await page.locator('.explanation-list li').first().evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(
      explanationFont,
      'Explanation list should use the same font family as body'
    ).toBe(bodyFont);
  });

  test('Test Case 3: All buttons follow consistent style patterns', async ({ page }) => {
    // Define expected button styles (from CSS)
    const expectedBorderRadius = '8px';
    const expectedFontWeight = '600';

    // Test primary CTA button
    const primaryBtn = page.getByTestId('cta-get-started');
    const primaryBtnStyles = await primaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        borderRadius: style.borderRadius,
        fontWeight: style.fontWeight,
        display: style.display,
        cursor: style.cursor,
        textDecoration: style.textDecoration
      };
    });

    expect(primaryBtnStyles.borderRadius, 'Primary button should have consistent border radius').toBe(expectedBorderRadius);
    expect(primaryBtnStyles.fontWeight, 'Primary button should have consistent font weight').toBe(expectedFontWeight);
    expect(primaryBtnStyles.cursor, 'Primary button should have pointer cursor').toBe('pointer');
    expect(primaryBtnStyles.textDecoration, 'Primary button should have no text decoration').toMatch(/none/);

    // Test secondary CTA button
    const secondaryBtn = page.getByTestId('cta-documentation');
    const secondaryBtnStyles = await secondaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        borderRadius: style.borderRadius,
        fontWeight: style.fontWeight,
        display: style.display,
        cursor: style.cursor,
        textDecoration: style.textDecoration
      };
    });

    expect(secondaryBtnStyles.borderRadius, 'Secondary button should have same border radius as primary').toBe(expectedBorderRadius);
    expect(secondaryBtnStyles.fontWeight, 'Secondary button should have same font weight as primary').toBe(expectedFontWeight);
    expect(secondaryBtnStyles.cursor, 'Secondary button should have pointer cursor').toBe('pointer');
    expect(secondaryBtnStyles.textDecoration, 'Secondary button should have no text decoration').toMatch(/none/);

    // Test copy buttons follow similar patterns
    const copyButtons = page.locator('.copy-button');
    const copyBtnCount = await copyButtons.count();
    expect(copyBtnCount, 'Should have copy buttons').toBeGreaterThan(0);

    for (let i = 0; i < copyBtnCount; i++) {
      const copyBtnStyles = await copyButtons.nth(i).evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          cursor: style.cursor,
          borderRadius: style.borderRadius
        };
      });

      expect(
        copyBtnStyles.cursor,
        `Copy button ${i + 1} should have pointer cursor`
      ).toBe('pointer');

      expect(
        copyBtnStyles.borderRadius,
        `Copy button ${i + 1} should have rounded corners`
      ).not.toBe('0px');
    }

    // Test button hover states are defined (verify transitions exist)
    const primaryBtnTransition = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });

    expect(
      primaryBtnTransition,
      'Primary button should have transition for hover effects'
    ).not.toBe('none');

    const secondaryBtnTransition = await secondaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });

    expect(
      secondaryBtnTransition,
      'Secondary button should have transition for hover effects'
    ).not.toBe('none');

    // Test both buttons have consistent display and alignment (flex-based)
    expect(
      primaryBtnStyles.display,
      'Primary button should use flex-based display'
    ).toMatch(/flex/);

    expect(
      secondaryBtnStyles.display,
      'Secondary button should use flex-based display'
    ).toMatch(/flex/);
  });

  test('Test Case 4: Margins and padding follow consistent spacing scale', async ({ page }) => {
    // Define expected spacing values based on CSS variables
    // --spacing-xs: 0.5rem (8px), --spacing-sm: 1rem (16px), --spacing-md: 1.5rem (24px)
    // --spacing-lg: 2rem (32px), --spacing-xl: 3rem (48px), --spacing-2xl: 4rem (64px)
    const spacingScale = {
      xs: 8,   // 0.5rem
      sm: 16,  // 1rem
      md: 24,  // 1.5rem
      lg: 32,  // 2rem
      xl: 48,  // 3rem
      '2xl': 64 // 4rem
    };

    // Helper function to parse pixel value
    const parsePixels = (value) => {
      const match = value.match(/(\d+(?:\.\d+)?)/);
      return match ? parseFloat(match[1]) : 0;
    };

    // Helper function to check if value is in spacing scale (with tolerance)
    const isInSpacingScale = (pixelValue, tolerance = 2) => {
      return Object.values(spacingScale).some(
        (scaleValue) => Math.abs(pixelValue - scaleValue) <= tolerance
      );
    };

    // Test section padding consistency
    const sections = ['features', 'architecture', 'code-example'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}, [data-testid="${sectionId}-section"]`).first();
      const isVisible = await section.isVisible();

      if (isVisible) {
        const sectionPadding = await section.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            paddingTop: style.paddingTop,
            paddingBottom: style.paddingBottom,
            paddingLeft: style.paddingLeft,
            paddingRight: style.paddingRight
          };
        });

        const paddingTop = parsePixels(sectionPadding.paddingTop);
        const paddingBottom = parsePixels(sectionPadding.paddingBottom);

        // Sections should have consistent vertical padding
        expect(
          Math.abs(paddingTop - paddingBottom),
          `Section ${sectionId} should have equal top and bottom padding`
        ).toBeLessThanOrEqual(2);

        // Padding should be from the spacing scale
        expect(
          isInSpacingScale(paddingTop),
          `Section ${sectionId} top padding (${paddingTop}px) should follow spacing scale`
        ).toBe(true);
      }
    }

    // Test feature cards have consistent spacing
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount, 'Should have feature cards').toBeGreaterThan(0);

    let previousPadding = null;
    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const cardPadding = await featureCards.nth(i).evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.padding;
      });

      if (previousPadding !== null) {
        expect(
          cardPadding,
          `Feature card ${i + 1} should have same padding as previous cards`
        ).toBe(previousPadding);
      }
      previousPadding = cardPadding;
    }

    // Test feature grid gap follows spacing scale
    const featuresGrid = page.locator('.features-grid');
    const gridGap = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gap;
    });

    const gapValue = parsePixels(gridGap);
    expect(
      isInSpacingScale(gapValue),
      `Features grid gap (${gapValue}px) should follow spacing scale`
    ).toBe(true);

    // Test button padding consistency
    const primaryBtn = page.getByTestId('cta-get-started');
    const secondaryBtn = page.getByTestId('cta-documentation');

    const primaryPadding = await primaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        paddingTop: style.paddingTop,
        paddingRight: style.paddingRight,
        paddingBottom: style.paddingBottom,
        paddingLeft: style.paddingLeft
      };
    });

    const secondaryPadding = await secondaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        paddingTop: style.paddingTop,
        paddingRight: style.paddingRight,
        paddingBottom: style.paddingBottom,
        paddingLeft: style.paddingLeft
      };
    });

    expect(
      primaryPadding,
      'Primary and secondary buttons should have consistent padding'
    ).toEqual(secondaryPadding);

    // Test navigation padding
    const navPadding = await page.locator('.nav').evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.padding;
    });

    const navPaddingValues = navPadding.split(' ').map(parsePixels);
    navPaddingValues.forEach((value, index) => {
      expect(
        isInSpacingScale(value),
        `Nav padding value ${index + 1} (${value}px) should follow spacing scale`
      ).toBe(true);
    });

    // Test container max-width consistency
    const containers = [
      '.features-container',
      '.architecture-container',
      '.code-example-container',
      '.footer-container'
    ];

    let previousMaxWidth = null;
    for (const containerSelector of containers) {
      const container = page.locator(containerSelector);
      const isVisible = await container.isVisible();

      if (isVisible) {
        const maxWidth = await container.evaluate((el) => {
          return window.getComputedStyle(el).maxWidth;
        });

        if (previousMaxWidth !== null) {
          expect(
            maxWidth,
            `${containerSelector} should have consistent max-width with other containers`
          ).toBe(previousMaxWidth);
        }
        previousMaxWidth = maxWidth;
      }
    }
  });

  test('Color variables are consistently applied across elements', async ({ page }) => {
    // Define expected colors from CSS variables
    const expectedColors = {
      background: 'rgb(13, 17, 23)',      // --color-background: #0d1117
      surface: 'rgb(22, 27, 34)',          // --color-surface: #161b22
      text: 'rgb(201, 209, 217)',          // --color-text: #c9d1d9
      textMuted: 'rgb(139, 148, 158)',     // --color-text-muted: #8b949e
      border: 'rgb(48, 54, 61)'            // --color-border: #30363d
    };

    // Test body background color
    const bodyBg = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(bodyBg, 'Body background should use background color variable').toBe(expectedColors.background);

    // Test body text color
    const bodyColor = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(bodyColor, 'Body text should use text color variable').toBe(expectedColors.text);

    // Test features section background (surface color)
    const featuresBg = await page.locator('.features').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(featuresBg, 'Features section should use surface color variable').toBe(expectedColors.surface);

    // Test muted text color (tagline)
    const taglineColor = await page.getByTestId('hero-tagline').evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(taglineColor, 'Tagline should use muted text color variable').toBe(expectedColors.textMuted);

    // Test border color on header
    const headerBorder = await page.locator('.header').evaluate((el) => {
      return window.getComputedStyle(el).borderBottomColor;
    });
    expect(headerBorder, 'Header border should use border color variable').toBe(expectedColors.border);

    // Test feature card border color
    const cardBorder = await page.locator('.feature-card').first().evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });
    expect(cardBorder, 'Feature card border should use border color variable').toBe(expectedColors.border);

    // Test footer background (surface color)
    const footerBg = await page.locator('.footer').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(footerBg, 'Footer should use surface color variable').toBe(expectedColors.surface);
  });

  test('Typography scale is consistently applied', async ({ page }) => {
    // Define expected font sizes from CSS variables
    // --font-size-base: 16px, --font-size-lg: 1.25rem (20px), --font-size-xl: 1.5rem (24px)
    // --font-size-2xl: 2rem (32px), --font-size-3xl: 3rem (48px)

    // Test hero title uses largest font size
    const heroTitleSize = await page.getByTestId('product-name').evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const heroTitlePx = parseFloat(heroTitleSize);
    expect(heroTitlePx, 'Hero title should use large font size (3xl scale)').toBeGreaterThanOrEqual(40);

    // Test section titles use consistent size
    const sectionTitles = page.locator('.features-title, .architecture-title, .code-example-title');
    const titleCount = await sectionTitles.count();

    let previousTitleSize = null;
    for (let i = 0; i < titleCount; i++) {
      const titleSize = await sectionTitles.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });

      if (previousTitleSize !== null) {
        expect(
          titleSize,
          `Section title ${i + 1} should have consistent font size with other section titles`
        ).toBe(previousTitleSize);
      }
      previousTitleSize = titleSize;
    }

    // Test feature card titles use consistent size
    const cardTitles = page.locator('.feature-card-title');
    const cardTitleCount = await cardTitles.count();

    let previousCardTitleSize = null;
    for (let i = 0; i < Math.min(cardTitleCount, 4); i++) {
      const cardTitleSize = await cardTitles.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });

      if (previousCardTitleSize !== null) {
        expect(
          cardTitleSize,
          `Feature card title ${i + 1} should have consistent font size`
        ).toBe(previousCardTitleSize);
      }
      previousCardTitleSize = cardTitleSize;
    }

    // Test body text uses base font size
    const bodyFontSize = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(bodyFontSize, 'Body should use base font size (16px)').toBe('16px');
  });

  test('Border radius is consistently applied', async ({ page }) => {
    // Define expected border radius from CSS variable
    // --border-radius: 8px
    const expectedBorderRadius = '8px';

    // Test feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < Math.min(cardCount, 4); i++) {
      const borderRadius = await featureCards.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(
        borderRadius,
        `Feature card ${i + 1} should have consistent border radius`
      ).toBe(expectedBorderRadius);
    }

    // Test buttons
    const buttons = page.locator('.btn');
    const btnCount = await buttons.count();

    for (let i = 0; i < btnCount; i++) {
      const borderRadius = await buttons.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(
        borderRadius,
        `Button ${i + 1} should have consistent border radius`
      ).toBe(expectedBorderRadius);
    }

    // Test code block wrappers
    const codeBlockWrappers = page.locator('.code-block-wrapper');
    const wrapperCount = await codeBlockWrappers.count();

    for (let i = 0; i < wrapperCount; i++) {
      const borderRadius = await codeBlockWrappers.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(
        borderRadius,
        `Code block wrapper ${i + 1} should have consistent border radius`
      ).toBe(expectedBorderRadius);
    }

    // Test architecture diagram container
    const archDiagram = page.locator('.architecture-diagram');
    const archBorderRadius = await archDiagram.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });
    expect(
      archBorderRadius,
      'Architecture diagram should have consistent border radius'
    ).toBe(expectedBorderRadius);

    // Test architecture explanation container
    const archExplanation = page.locator('.architecture-explanation');
    const explBorderRadius = await archExplanation.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });
    expect(
      explBorderRadius,
      'Architecture explanation should have consistent border radius'
    ).toBe(expectedBorderRadius);
  });
});

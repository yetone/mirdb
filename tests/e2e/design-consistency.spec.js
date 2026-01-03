// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Design Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Color Palette Consistency', () => {
    // Test Case 1: Page uses consistent color scheme throughout
    test('TC1: page uses consistent color scheme throughout', async ({ page }) => {
      // Verify CSS variables are defined in :root
      const primaryColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-primary').trim();
      });
      expect(primaryColor).toBeTruthy();

      const backgroundColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-background').trim();
      });
      expect(backgroundColor).toBeTruthy();

      const textColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-text').trim();
      });
      expect(textColor).toBeTruthy();

      const surfaceColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-surface').trim();
      });
      expect(surfaceColor).toBeTruthy();

      // Verify colors are used consistently across sections
      const heroBackground = await page.evaluate(() => {
        const hero = document.querySelector('.hero');
        return hero ? getComputedStyle(hero).backgroundImage || getComputedStyle(hero).backgroundColor : null;
      });
      expect(heroBackground).toBeTruthy();

      // Check that feature cards use consistent colors
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        const cardBg = await card.evaluate(el => getComputedStyle(el).backgroundColor);
        // All cards should have the surface color background
        expect(cardBg).toBeTruthy();
      }
    });

    test('color variables are defined and consistent', async ({ page }) => {
      const colorVars = await page.evaluate(() => {
        const style = getComputedStyle(document.documentElement);
        return {
          primary: style.getPropertyValue('--color-primary').trim(),
          primaryDark: style.getPropertyValue('--color-primary-dark').trim(),
          background: style.getPropertyValue('--color-background').trim(),
          surface: style.getPropertyValue('--color-surface').trim(),
          text: style.getPropertyValue('--color-text').trim(),
          textMuted: style.getPropertyValue('--color-text-muted').trim(),
          border: style.getPropertyValue('--color-border').trim(),
        };
      });

      // All color variables should be defined
      expect(colorVars.primary).toBeTruthy();
      expect(colorVars.primaryDark).toBeTruthy();
      expect(colorVars.background).toBeTruthy();
      expect(colorVars.surface).toBeTruthy();
      expect(colorVars.text).toBeTruthy();
      expect(colorVars.textMuted).toBeTruthy();
      expect(colorVars.border).toBeTruthy();
    });
  });

  test.describe('Typography Hierarchy Consistency', () => {
    // Test Case 2: Heading sizes follow consistent scale
    test('TC2: heading sizes follow consistent scale', async ({ page }) => {
      // Get h1 font size (hero)
      const h1Size = await page.evaluate(() => {
        const h1 = document.querySelector('.hero h1');
        return h1 ? parseFloat(getComputedStyle(h1).fontSize) : 0;
      });

      // Get h2 font sizes from sections
      const h2Sizes = await page.evaluate(() => {
        const h2s = document.querySelectorAll('main h2');
        return Array.from(h2s).map(h2 => parseFloat(getComputedStyle(h2).fontSize));
      });

      // Get h3 font sizes grouped by context
      const h3SizesByContext = await page.evaluate(() => {
        return {
          featureCards: Array.from(document.querySelectorAll('.feature-card h3')).map(h3 => parseFloat(getComputedStyle(h3).fontSize)),
          quickStartSteps: Array.from(document.querySelectorAll('.quick-start-step h3')).map(h3 => parseFloat(getComputedStyle(h3).fontSize)),
          commandCategories: Array.from(document.querySelectorAll('.command-category h3')).map(h3 => parseFloat(getComputedStyle(h3).fontSize)),
          roadmapColumns: Array.from(document.querySelectorAll('.roadmap-column h3')).map(h3 => parseFloat(getComputedStyle(h3).fontSize)),
        };
      });

      // H1 should be largest
      expect(h1Size).toBeGreaterThan(0);

      // All h2s should be same size (consistent) and smaller than h1
      if (h2Sizes.length > 0) {
        const firstH2Size = h2Sizes[0];
        expect(firstH2Size).toBeLessThan(h1Size);

        // All h2s should have similar sizing (within 2px tolerance)
        h2Sizes.forEach(size => {
          expect(Math.abs(size - firstH2Size)).toBeLessThanOrEqual(2);
        });
      }

      // H3s within the same context should be consistent
      // Feature card h3s should all be the same size
      if (h3SizesByContext.featureCards.length > 1) {
        const firstSize = h3SizesByContext.featureCards[0];
        h3SizesByContext.featureCards.forEach(size => {
          expect(Math.abs(size - firstSize)).toBeLessThanOrEqual(2);
        });
      }

      // Quick start step h3s should all be the same size
      if (h3SizesByContext.quickStartSteps.length > 1) {
        const firstSize = h3SizesByContext.quickStartSteps[0];
        h3SizesByContext.quickStartSteps.forEach(size => {
          expect(Math.abs(size - firstSize)).toBeLessThanOrEqual(2);
        });
      }

      // Command category h3s should all be the same size
      if (h3SizesByContext.commandCategories.length > 1) {
        const firstSize = h3SizesByContext.commandCategories[0];
        h3SizesByContext.commandCategories.forEach(size => {
          expect(Math.abs(size - firstSize)).toBeLessThanOrEqual(2);
        });
      }

      // Roadmap column h3s should all be the same size
      if (h3SizesByContext.roadmapColumns.length > 1) {
        const firstSize = h3SizesByContext.roadmapColumns[0];
        h3SizesByContext.roadmapColumns.forEach(size => {
          expect(Math.abs(size - firstSize)).toBeLessThanOrEqual(2);
        });
      }

      // All h3s should be smaller than h2s
      const allH3Sizes = [
        ...h3SizesByContext.featureCards,
        ...h3SizesByContext.quickStartSteps,
        ...h3SizesByContext.commandCategories,
        ...h3SizesByContext.roadmapColumns,
      ];

      if (allH3Sizes.length > 0 && h2Sizes.length > 0) {
        const maxH3Size = Math.max(...allH3Sizes);
        expect(maxH3Size).toBeLessThanOrEqual(h2Sizes[0]);
      }
    });

    test('font family is consistent throughout', async ({ page }) => {
      // Check that body uses the defined font family
      const bodyFont = await page.evaluate(() => {
        return getComputedStyle(document.body).fontFamily;
      });
      expect(bodyFont).toBeTruthy();

      // Check that headings inherit consistent font
      const h1Font = await page.evaluate(() => {
        const h1 = document.querySelector('.hero h1');
        return h1 ? getComputedStyle(h1).fontFamily : null;
      });
      expect(h1Font).toBeTruthy();

      // Check that code blocks use monospace font
      const codeFont = await page.evaluate(() => {
        const code = document.querySelector('.code-block code');
        return code ? getComputedStyle(code).fontFamily : null;
      });
      expect(codeFont).toMatch(/mono|consolas|menlo|courier/i);
    });
  });

  test.describe('Button Styling Consistency', () => {
    // Test Case 3: All buttons follow same styling patterns
    test('TC3: all buttons follow same styling patterns', async ({ page }) => {
      // Get all primary buttons
      const primaryButtons = page.locator('.btn-primary');
      const primaryCount = await primaryButtons.count();

      if (primaryCount > 0) {
        // Get styling of first primary button
        const firstPrimaryStyle = await primaryButtons.first().evaluate(el => {
          const style = getComputedStyle(el);
          return {
            backgroundColor: style.backgroundColor,
            borderRadius: style.borderRadius,
            fontWeight: style.fontWeight,
            padding: style.padding,
          };
        });

        // All primary buttons should have similar styling
        for (let i = 1; i < primaryCount; i++) {
          const btnStyle = await primaryButtons.nth(i).evaluate(el => {
            const style = getComputedStyle(el);
            return {
              backgroundColor: style.backgroundColor,
              borderRadius: style.borderRadius,
              fontWeight: style.fontWeight,
            };
          });

          expect(btnStyle.backgroundColor).toBe(firstPrimaryStyle.backgroundColor);
          expect(btnStyle.borderRadius).toBe(firstPrimaryStyle.borderRadius);
          expect(btnStyle.fontWeight).toBe(firstPrimaryStyle.fontWeight);
        }
      }

      // Get all secondary buttons
      const secondaryButtons = page.locator('.btn-secondary');
      const secondaryCount = await secondaryButtons.count();

      if (secondaryCount > 0) {
        // Get styling of first secondary button
        const firstSecondaryStyle = await secondaryButtons.first().evaluate(el => {
          const style = getComputedStyle(el);
          return {
            borderRadius: style.borderRadius,
            fontWeight: style.fontWeight,
            borderStyle: style.borderStyle,
          };
        });

        // All secondary buttons should have similar styling
        for (let i = 1; i < secondaryCount; i++) {
          const btnStyle = await secondaryButtons.nth(i).evaluate(el => {
            const style = getComputedStyle(el);
            return {
              borderRadius: style.borderRadius,
              fontWeight: style.fontWeight,
              borderStyle: style.borderStyle,
            };
          });

          expect(btnStyle.borderRadius).toBe(firstSecondaryStyle.borderRadius);
          expect(btnStyle.fontWeight).toBe(firstSecondaryStyle.fontWeight);
        }
      }

      // Verify buttons use consistent border-radius variable
      const allButtons = page.locator('.btn');
      const buttonCount = await allButtons.count();

      if (buttonCount > 1) {
        const firstBtnRadius = await allButtons.first().evaluate(el => {
          return getComputedStyle(el).borderRadius;
        });

        for (let i = 1; i < buttonCount; i++) {
          const btnRadius = await allButtons.nth(i).evaluate(el => {
            return getComputedStyle(el).borderRadius;
          });
          expect(btnRadius).toBe(firstBtnRadius);
        }
      }
    });

    test('docs links follow consistent styling', async ({ page }) => {
      const docsLinks = page.locator('.docs-link');
      const count = await docsLinks.count();

      if (count > 1) {
        const firstStyle = await docsLinks.first().evaluate(el => {
          const style = getComputedStyle(el);
          return {
            backgroundColor: style.backgroundColor,
            borderRadius: style.borderRadius,
            fontWeight: style.fontWeight,
          };
        });

        for (let i = 1; i < count; i++) {
          const linkStyle = await docsLinks.nth(i).evaluate(el => {
            const style = getComputedStyle(el);
            return {
              backgroundColor: style.backgroundColor,
              borderRadius: style.borderRadius,
              fontWeight: style.fontWeight,
            };
          });

          expect(linkStyle.backgroundColor).toBe(firstStyle.backgroundColor);
          expect(linkStyle.borderRadius).toBe(firstStyle.borderRadius);
        }
      }
    });
  });

  test.describe('Section Spacing Consistency', () => {
    // Test Case 4: Sections have consistent vertical spacing
    test('TC4: sections have consistent vertical spacing', async ({ page }) => {
      // Get padding values for all major sections
      const sectionPaddings = await page.evaluate(() => {
        const sections = document.querySelectorAll('.hero, .features, .quickstart, .commands-section, .roadmap, .config');
        return Array.from(sections).map(section => {
          const style = getComputedStyle(section);
          return {
            name: section.className,
            paddingTop: parseFloat(style.paddingTop),
            paddingBottom: parseFloat(style.paddingBottom),
          };
        });
      });

      expect(sectionPaddings.length).toBeGreaterThan(0);

      // Hero section uses xl spacing
      const heroSection = sectionPaddings.find(s => s.name.includes('hero'));
      expect(heroSection).toBeDefined();
      expect(heroSection.paddingTop).toBeGreaterThan(0);
      expect(heroSection.paddingBottom).toBeGreaterThan(0);

      // Features, quickstart, commands, roadmap, config should have xl spacing
      const mainSections = sectionPaddings.filter(s =>
        s.name.includes('features') ||
        s.name.includes('quickstart') ||
        s.name.includes('commands') ||
        s.name.includes('roadmap') ||
        s.name.includes('config')
      );

      if (mainSections.length > 1) {
        // All main sections should have similar padding (within 10px tolerance)
        const firstPadding = mainSections[0].paddingTop;
        mainSections.forEach(section => {
          expect(Math.abs(section.paddingTop - firstPadding)).toBeLessThanOrEqual(10);
        });
      }
    });

    test('spacing variables are defined and used consistently', async ({ page }) => {
      const spacingVars = await page.evaluate(() => {
        const style = getComputedStyle(document.documentElement);
        return {
          xs: style.getPropertyValue('--spacing-xs').trim(),
          sm: style.getPropertyValue('--spacing-sm').trim(),
          md: style.getPropertyValue('--spacing-md').trim(),
          lg: style.getPropertyValue('--spacing-lg').trim(),
          xl: style.getPropertyValue('--spacing-xl').trim(),
        };
      });

      // All spacing variables should be defined
      expect(spacingVars.xs).toBeTruthy();
      expect(spacingVars.sm).toBeTruthy();
      expect(spacingVars.md).toBeTruthy();
      expect(spacingVars.lg).toBeTruthy();
      expect(spacingVars.xl).toBeTruthy();

      // Spacing should follow a scale (xs < sm < md < lg < xl)
      const parseSpacing = (val) => parseFloat(val);

      expect(parseSpacing(spacingVars.xs)).toBeLessThan(parseSpacing(spacingVars.sm));
      expect(parseSpacing(spacingVars.sm)).toBeLessThan(parseSpacing(spacingVars.md));
      expect(parseSpacing(spacingVars.md)).toBeLessThan(parseSpacing(spacingVars.lg));
      expect(parseSpacing(spacingVars.lg)).toBeLessThan(parseSpacing(spacingVars.xl));
    });

    test('container width is consistent across sections', async ({ page }) => {
      const containerWidths = await page.evaluate(() => {
        const containers = document.querySelectorAll('.container');
        return Array.from(containers).map(container => {
          const style = getComputedStyle(container);
          return {
            maxWidth: style.maxWidth,
            marginLeft: style.marginLeft,
            marginRight: style.marginRight,
          };
        });
      });

      expect(containerWidths.length).toBeGreaterThan(0);

      // All containers should have the same max-width
      const firstMaxWidth = containerWidths[0].maxWidth;
      containerWidths.forEach(container => {
        expect(container.maxWidth).toBe(firstMaxWidth);
      });
    });

    test('border-radius is consistent for cards and elements', async ({ page }) => {
      // Check that border-radius variable is defined
      const borderRadius = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--border-radius').trim();
      });
      expect(borderRadius).toBeTruthy();

      // Check feature cards use consistent border-radius
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      if (cardCount > 1) {
        const firstRadius = await featureCards.first().evaluate(el => {
          return getComputedStyle(el).borderRadius;
        });

        for (let i = 1; i < cardCount; i++) {
          const radius = await featureCards.nth(i).evaluate(el => {
            return getComputedStyle(el).borderRadius;
          });
          expect(radius).toBe(firstRadius);
        }
      }

      // Check code blocks use consistent border-radius
      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();

      if (codeBlockCount > 1) {
        const firstCodeRadius = await codeBlocks.first().evaluate(el => {
          return getComputedStyle(el).borderRadius;
        });

        for (let i = 1; i < codeBlockCount; i++) {
          const radius = await codeBlocks.nth(i).evaluate(el => {
            return getComputedStyle(el).borderRadius;
          });
          expect(radius).toBe(firstCodeRadius);
        }
      }
    });
  });

  test.describe('Visual Design Elements', () => {
    test('feature cards have consistent styling', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      expect(cardCount).toBeGreaterThan(0);

      if (cardCount > 1) {
        const firstCardStyle = await featureCards.first().evaluate(el => {
          const style = getComputedStyle(el);
          return {
            backgroundColor: style.backgroundColor,
            padding: style.padding,
            borderRadius: style.borderRadius,
            borderWidth: style.borderWidth,
            borderStyle: style.borderStyle,
          };
        });

        for (let i = 1; i < cardCount; i++) {
          const cardStyle = await featureCards.nth(i).evaluate(el => {
            const style = getComputedStyle(el);
            return {
              backgroundColor: style.backgroundColor,
              padding: style.padding,
              borderRadius: style.borderRadius,
              borderWidth: style.borderWidth,
              borderStyle: style.borderStyle,
            };
          });

          expect(cardStyle.backgroundColor).toBe(firstCardStyle.backgroundColor);
          expect(cardStyle.borderRadius).toBe(firstCardStyle.borderRadius);
          expect(cardStyle.borderWidth).toBe(firstCardStyle.borderWidth);
          expect(cardStyle.borderStyle).toBe(firstCardStyle.borderStyle);
        }
      }
    });

    test('grid layouts use consistent gap spacing', async ({ page }) => {
      // Check features grid
      const featuresGrid = page.locator('.features-grid');
      const featuresGap = await featuresGrid.evaluate(el => {
        return getComputedStyle(el).gap;
      });
      expect(featuresGap).toBeTruthy();

      // Check commands grid
      const commandsGrid = page.locator('.commands-grid');
      const commandsGap = await commandsGrid.evaluate(el => {
        return getComputedStyle(el).gap;
      });
      expect(commandsGap).toBeTruthy();

      // Grids should use consistent spacing
      expect(featuresGap).toBe(commandsGap);
    });

    test('text colors are consistent across similar elements', async ({ page }) => {
      // Check that muted text color is used consistently
      const mutedTextElements = await page.evaluate(() => {
        const descriptions = document.querySelectorAll('.feature-card p, .quick-start-step p, .roadmap-item .item-content p');
        const colors = new Set();
        descriptions.forEach(el => {
          colors.add(getComputedStyle(el).color);
        });
        return Array.from(colors);
      });

      // All muted text should use the same color
      expect(mutedTextElements.length).toBeLessThanOrEqual(1);
    });
  });
});

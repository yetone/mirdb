const { test, expect } = require('@playwright/test');

test.describe('Visual Design Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Body text uses consistent sans-serif font throughout', async ({ page }) => {
    // Get the body font family
    const bodyFontFamily = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Verify it's a sans-serif font stack
    const isSansSerif = bodyFontFamily.includes('sans-serif') ||
      bodyFontFamily.includes('system-ui') ||
      bodyFontFamily.includes('-apple-system') ||
      bodyFontFamily.includes('BlinkMacSystemFont') ||
      bodyFontFamily.includes('Segoe UI') ||
      bodyFontFamily.includes('Roboto');
    expect(isSansSerif).toBe(true);

    // Check that multiple text elements use consistent font
    const textElements = [
      '.hero .tagline',
      '.section-subtitle',
      '.feature-card p',
      '.footer-links a'
    ];

    for (const selector of textElements) {
      const element = page.locator(selector).first();
      if (await element.count() > 0) {
        const fontFamily = await element.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });
        // All body text should use the same sans-serif font family or inherit from body
        const usesSansSerif = fontFamily.includes('sans-serif') ||
          fontFamily.includes('system-ui') ||
          fontFamily.includes('-apple-system') ||
          fontFamily.includes('BlinkMacSystemFont') ||
          fontFamily.includes('Segoe UI') ||
          fontFamily.includes('Roboto');
        expect(usesSansSerif).toBe(true);
      }
    }
  });

  test('TC2: All code examples use monospace font family', async ({ page }) => {
    // Check code blocks use monospace font
    const codeBlock = page.locator('.code-block pre');
    if (await codeBlock.count() > 0) {
      const codeFontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      const isMonospace = codeFontFamily.includes('Monaco') ||
        codeFontFamily.includes('Menlo') ||
        codeFontFamily.includes('monospace') ||
        codeFontFamily.includes('Ubuntu Mono') ||
        codeFontFamily.includes('Consolas') ||
        codeFontFamily.includes('Courier');
      expect(isMonospace).toBe(true);
    }

    // Check command items use monospace font
    const commandItem = page.locator('.command-item').first();
    if (await commandItem.count() > 0) {
      const commandFontFamily = await commandItem.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      const isMonospace = commandFontFamily.includes('Monaco') ||
        commandFontFamily.includes('Menlo') ||
        commandFontFamily.includes('monospace') ||
        commandFontFamily.includes('Ubuntu Mono') ||
        commandFontFamily.includes('Consolas') ||
        commandFontFamily.includes('Courier');
      expect(isMonospace).toBe(true);
    }

    // Check config values use monospace font
    const configValue = page.locator('.config-value').first();
    if (await configValue.count() > 0) {
      const configFontFamily = await configValue.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      const isMonospace = configFontFamily.includes('Monaco') ||
        configFontFamily.includes('Menlo') ||
        configFontFamily.includes('monospace') ||
        configFontFamily.includes('Ubuntu Mono') ||
        configFontFamily.includes('Consolas') ||
        configFontFamily.includes('Courier');
      expect(isMonospace).toBe(true);
    }
  });

  test('TC3: Headings are visually larger than body text with clear hierarchy', async ({ page }) => {
    // Get h1 font size (hero title)
    const h1FontSize = await page.locator('.hero h1').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Get section title (h2) font size
    const h2FontSize = await page.locator('.section-title').first().evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Get feature card heading (h3) font size
    const h3FontSize = await page.locator('.feature-card h3').first().evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Get body text font size
    const bodyFontSize = await page.locator('.feature-card p').first().evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Verify hierarchy: h1 > h2 > h3 > body text
    expect(h1FontSize).toBeGreaterThan(h2FontSize);
    expect(h2FontSize).toBeGreaterThan(h3FontSize);
    expect(h3FontSize).toBeGreaterThan(bodyFontSize);

    // Verify headings are larger than typical body text (16px)
    expect(h1FontSize).toBeGreaterThanOrEqual(40); // 2.5rem or higher
    expect(h2FontSize).toBeGreaterThanOrEqual(24); // 1.5rem or higher
    expect(h3FontSize).toBeGreaterThanOrEqual(18); // 1.125rem or higher

    // Verify headings have appropriate font weight
    const h1FontWeight = await page.locator('.hero h1').evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontWeight);
    });
    const h2FontWeight = await page.locator('.section-title').first().evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontWeight);
    });

    // Headings should be bold (>=600)
    expect(h1FontWeight).toBeGreaterThanOrEqual(600);
    expect(h2FontWeight).toBeGreaterThanOrEqual(600);
  });

  test('TC4: Sections have adequate padding/margin for visual separation', async ({ page }) => {
    // Check hero section padding
    const heroSection = page.locator('.hero');
    const heroPadding = await heroSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(heroPadding.paddingTop).toBeGreaterThanOrEqual(40);
    expect(heroPadding.paddingBottom).toBeGreaterThanOrEqual(40);

    // Check features section padding
    const featuresSection = page.locator('.features');
    const featuresPadding = await featuresSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(featuresPadding.paddingTop).toBeGreaterThanOrEqual(60);
    expect(featuresPadding.paddingBottom).toBeGreaterThanOrEqual(60);

    // Check quickstart section padding
    const quickstartSection = page.locator('.quickstart');
    const quickstartPadding = await quickstartSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(quickstartPadding.paddingTop).toBeGreaterThanOrEqual(60);
    expect(quickstartPadding.paddingBottom).toBeGreaterThanOrEqual(60);

    // Check commands section padding
    const commandsSection = page.locator('.commands');
    const commandsPadding = await commandsSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(commandsPadding.paddingTop).toBeGreaterThanOrEqual(60);
    expect(commandsPadding.paddingBottom).toBeGreaterThanOrEqual(60);

    // Check configuration section padding
    const configSection = page.locator('.configuration');
    const configPadding = await configSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(configPadding.paddingTop).toBeGreaterThanOrEqual(60);
    expect(configPadding.paddingBottom).toBeGreaterThanOrEqual(60);

    // Check footer padding
    const footer = page.locator('footer');
    const footerPadding = await footer.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(footerPadding.paddingTop).toBeGreaterThanOrEqual(24);
    expect(footerPadding.paddingBottom).toBeGreaterThanOrEqual(24);
  });

  test('Color scheme is consistent and appropriate for developer tools', async ({ page }) => {
    // Check background color is dark (appropriate for developer tools)
    const bgColor = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Dark theme background should have low RGB values
    const bgMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (bgMatch) {
      const [, r, g, b] = bgMatch.map(Number);
      // Dark background means low RGB values (< 100)
      expect(r).toBeLessThan(100);
      expect(g).toBeLessThan(100);
      expect(b).toBeLessThan(100);
    }

    // Check text color is light (readable on dark background)
    const textColor = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const textMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (textMatch) {
      const [, r, g, b] = textMatch.map(Number);
      // Light text should have high RGB values (> 150)
      expect(r).toBeGreaterThan(150);
      expect(g).toBeGreaterThan(150);
      expect(b).toBeGreaterThan(150);
    }
  });

  test('Visual elements have consistent border radius and styling', async ({ page }) => {
    // Check feature cards have consistent border radius
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    let firstBorderRadius = null;
    for (let i = 0; i < cardCount; i++) {
      const borderRadius = await featureCards.nth(i).evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      if (firstBorderRadius === null) {
        firstBorderRadius = borderRadius;
      } else {
        expect(borderRadius).toBe(firstBorderRadius);
      }
    }

    // Check buttons have consistent styling
    const buttons = page.locator('.btn');
    const buttonCount = await buttons.count();
    if (buttonCount >= 2) {
      const btn1Radius = await buttons.nth(0).evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      const btn2Radius = await buttons.nth(1).evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(btn1Radius).toBe(btn2Radius);
    }
  });
});

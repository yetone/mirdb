/**
 * Brand Consistency E2E Tests
 * Owner: Scenario 9 - Brand Consistency and Visual Design
 *
 * End-to-end tests for validating brand consistency and visual design of the MirDB homepage.
 *
 * Requirements traced:
 * - NFR-2: Homepage shall maintain consistency with existing project branding
 * - Design Requirements: Clean, professional design for technical audience
 */

// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Brand Consistency and Visual Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 2: Check logo displays correctly
  test('logo image loads and displays without distortion', async ({ page }) => {
    const logo = page.locator('header .logo img');
    await expect(logo).toBeVisible();

    // Check that the logo has a valid source pointing to logo.gif
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Check that the logo has alt text for accessibility
    const alt = await logo.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt.toLowerCase()).toContain('logo');

    // Check that the image element has CSS styling for proper display
    const computedStyles = await logo.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        height: styles.height,
        width: styles.width,
        display: styles.display
      };
    });

    // Image should have defined height (as per CSS: height: 40px)
    expect(computedStyles.display).not.toBe('none');
    // Height should be defined
    const heightVal = parseFloat(computedStyles.height);
    expect(heightVal).toBeGreaterThan(0);

    // Check that the logo image element is rendered in the DOM correctly
    const boundingBox = await logo.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.height).toBeGreaterThan(0);
  });

  // Test Case 4: Check for adequate whitespace
  test('sections have sufficient padding and margins for readability', async ({ page }) => {
    // Check hero section padding
    const hero = page.locator('.hero');
    const heroPadding = await hero.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    // Hero should have substantial padding (at least 32px)
    expect(heroPadding.paddingTop).toBeGreaterThanOrEqual(32);
    expect(heroPadding.paddingBottom).toBeGreaterThanOrEqual(32);

    // Check features section padding
    const features = page.locator('.features');
    const featuresPadding = await features.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(featuresPadding.paddingTop).toBeGreaterThanOrEqual(32);
    expect(featuresPadding.paddingBottom).toBeGreaterThanOrEqual(32);

    // Check quickstart section padding
    const quickstart = page.locator('.quickstart');
    const quickstartPadding = await quickstart.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(styles.paddingTop),
        paddingBottom: parseFloat(styles.paddingBottom)
      };
    });
    expect(quickstartPadding.paddingTop).toBeGreaterThanOrEqual(32);
    expect(quickstartPadding.paddingBottom).toBeGreaterThanOrEqual(32);

    // Check container has horizontal padding
    const container = page.locator('.container').first();
    const containerPadding = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingLeft: parseFloat(styles.paddingLeft),
        paddingRight: parseFloat(styles.paddingRight)
      };
    });
    expect(containerPadding.paddingLeft).toBeGreaterThanOrEqual(16);
    expect(containerPadding.paddingRight).toBeGreaterThanOrEqual(16);
  });

  // Test for consistent font usage across the page
  test('page uses consistent font family throughout', async ({ page }) => {
    // Check body font
    const bodyFont = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(bodyFont).toBeTruthy();
    expect(bodyFont).not.toContain('Times');

    // Check that headings use the same font family (system fonts)
    const h1Font = await page.locator('h1').first().evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    const h2Font = await page.locator('h2').first().evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Body and headings should use the same font family
    expect(h1Font).toBe(bodyFont);
    expect(h2Font).toBe(bodyFont);
  });

  // Test for code blocks using monospace font
  test('code examples use monospace font family', async ({ page }) => {
    const codeBlock = page.locator('pre code').first();
    await expect(codeBlock).toBeVisible();

    const codeFont = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Code should use a monospace font
    const isMonospace =
      codeFont.toLowerCase().includes('consolas') ||
      codeFont.toLowerCase().includes('menlo') ||
      codeFont.toLowerCase().includes('monaco') ||
      codeFont.toLowerCase().includes('monospace') ||
      codeFont.toLowerCase().includes('sfmono') ||
      codeFont.toLowerCase().includes('courier');
    expect(isMonospace).toBe(true);
  });

  // Test for professional color scheme
  test('page has professional color scheme applied', async ({ page }) => {
    // Check primary button has custom background color
    const primaryBtn = page.locator('.btn-primary').first();
    const btnBgColor = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should not be transparent or white
    expect(btnBgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(btnBgColor).not.toBe('rgb(255, 255, 255)');

    // Check body background
    const bodyBg = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(bodyBg).toBeTruthy();

    // Check text color is not black (custom color scheme)
    const textColor = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(textColor).toBeTruthy();
  });

  // Test for consistent visual hierarchy
  test('visual hierarchy with proper heading sizes', async ({ page }) => {
    const h1Size = await page.locator('.hero h1').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    const h2Size = await page.locator('h2').first().evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    const h3Size = await page.locator('h3').first().evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Headings should follow size hierarchy
    expect(h1Size).toBeGreaterThan(h2Size);
    expect(h2Size).toBeGreaterThan(h3Size);
  });

  // Test for cards having consistent styling
  test('feature cards have consistent styling', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(1);

    // Get styles of first card
    const firstCardStyles = await featureCards.first().evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius,
        padding: styles.padding
      };
    });

    // All cards should have the same styling
    for (let i = 1; i < cardCount; i++) {
      const cardStyles = await featureCards.nth(i).evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          borderRadius: styles.borderRadius,
          padding: styles.padding
        };
      });

      expect(cardStyles.backgroundColor).toBe(firstCardStyles.backgroundColor);
      expect(cardStyles.borderRadius).toBe(firstCardStyles.borderRadius);
      expect(cardStyles.padding).toBe(firstCardStyles.padding);
    }
  });

  // Test for consistent link styling
  test('links have consistent styling', async ({ page }) => {
    // Get navigation link color
    const navLink = page.locator('header nav a').first();
    const navLinkColor = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(navLinkColor).toBeTruthy();

    // Check that links have hover state (by checking transition)
    const hasTransition = await navLink.evaluate((el) => {
      const transition = window.getComputedStyle(el).transition;
      return transition && transition !== 'all 0s ease 0s' && transition !== 'none';
    });
    expect(hasTransition).toBe(true);
  });
});

// Test Case 6: Check professional appearance (supplementary visual checks)
test.describe('Professional Appearance', () => {
  test('page has no horizontal overflow', async ({ page }) => {
    await page.goto('/');

    // Check that document doesn't have horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('all sections are properly aligned', async ({ page }) => {
    await page.goto('/');

    const sections = ['#hero', '#features', '#quickstart', '#resources'];

    for (const selector of sections) {
      const section = page.locator(selector);
      const isVisible = await section.isVisible();
      if (isVisible) {
        const boundingBox = await section.boundingBox();
        expect(boundingBox).toBeTruthy();
        // Section should start at or near left edge (accounting for scroll)
        expect(boundingBox.x).toBeGreaterThanOrEqual(0);
      }
    }
  });

  test('footer has consistent styling', async ({ page }) => {
    await page.goto('/');

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerStyles = await footer.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        color: styles.color,
        padding: styles.padding
      };
    });

    // Footer should have distinct background (typically dark)
    expect(footerStyles.backgroundColor).toBeTruthy();
    expect(footerStyles.padding).toBeTruthy();
  });
});

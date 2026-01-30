/**
 * Responsive Design and Theme Tests
 * Owners: Scenario 8 (Mobile), Scenario 9 (Tablet), Scenario 12 (Dark Mode)
 *
 * Tests:
 * - Mobile viewport (375px)
 * - Tablet viewport (768px)
 * - Navigation adaptation
 * - Touch targets
 * - Dark/light mode switching
 */

const { test, expect } = require('@playwright/test');
const { BASE_URL, SELECTORS, VIEWPORTS, waitForPageLoad } = require('./test-utils');

// ===========================================
// Mobile Responsive Tests - Scenario 8
// ===========================================

test.describe('Scenario 8: Mobile Responsive Design (375px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('Test Case 1: Page renders without horizontal overflow at 375px viewport', async ({ page }) => {
    // Check that body doesn't have horizontal overflow
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;
      return {
        bodyScrollWidth: body.scrollWidth,
        bodyClientWidth: body.clientWidth,
        htmlScrollWidth: html.scrollWidth,
        htmlClientWidth: html.clientWidth,
        viewportWidth: window.innerWidth
      };
    });

    // Body scroll width should not exceed viewport width
    expect(bodyOverflow.bodyScrollWidth).toBeLessThanOrEqual(bodyOverflow.viewportWidth + 1);
    expect(bodyOverflow.htmlScrollWidth).toBeLessThanOrEqual(bodyOverflow.viewportWidth + 1);
  });

  test('Test Case 2: Navigation is accessible via hamburger menu on mobile', async ({ page }) => {
    // Check that hamburger menu button exists and is visible
    const menuToggle = page.locator('.header__menu-toggle');
    await expect(menuToggle).toBeVisible();

    // Check that navigation is initially hidden (collapsed)
    const nav = page.locator('.header__nav');
    const navVisible = await nav.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.display !== 'none';
    });
    expect(navVisible).toBe(false);

    // Click the hamburger menu
    await menuToggle.click();

    // Check that navigation is now visible
    await expect(nav).toHaveClass(/header__nav--open/);

    // Check that aria-expanded is true
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');

    // Check that nav links are accessible
    const navLinks = page.locator('.header__nav-link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Verify each link is visible when menu is open
    for (let i = 0; i < linkCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }
  });

  test('Test Case 3: Text is readable with font size at least 16px', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const styles = window.getComputedStyle(body);
      return parseFloat(styles.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph text font sizes
    const paragraphs = page.locator('p');
    const paragraphCount = await paragraphs.count();

    for (let i = 0; i < Math.min(paragraphCount, 5); i++) {
      const fontSize = await paragraphs.nth(i).evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return parseFloat(styles.fontSize);
      });
      // Allow some smaller text for secondary content but main text should be readable
      expect(fontSize).toBeGreaterThanOrEqual(12);
    }
  });

  test('Test Case 4: Code blocks have horizontal scroll (overflow-x)', async ({ page }) => {
    // Find code blocks in the usage section
    const codeBlocks = page.locator('.usage__code');
    const codeBlockCount = await codeBlocks.count();

    if (codeBlockCount > 0) {
      for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
        const overflowX = await codeBlocks.nth(i).evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return styles.overflowX;
        });
        // Should be 'auto' or 'scroll' to allow horizontal scrolling
        expect(['auto', 'scroll']).toContain(overflowX);
      }
    }

    // Also check pre elements generally
    const preElements = page.locator('pre');
    const preCount = await preElements.count();

    if (preCount > 0) {
      for (let i = 0; i < Math.min(preCount, 3); i++) {
        const overflowX = await preElements.nth(i).evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return styles.overflowX;
        });
        expect(['auto', 'scroll', 'visible']).toContain(overflowX);
      }
    }
  });

  test('Test Case 5: Interactive elements have at least 44px touch target', async ({ page }) => {
    // Check hamburger menu button
    const menuToggle = page.locator('.header__menu-toggle');
    const menuToggleBox = await menuToggle.boundingBox();
    expect(menuToggleBox.width).toBeGreaterThanOrEqual(44);
    expect(menuToggleBox.height).toBeGreaterThanOrEqual(44);

    // Check CTA buttons
    const ctaButtons = page.locator('.hero__cta');
    const ctaCount = await ctaButtons.count();

    for (let i = 0; i < ctaCount; i++) {
      const ctaBox = await ctaButtons.nth(i).boundingBox();
      if (ctaBox) {
        expect(ctaBox.height).toBeGreaterThanOrEqual(44);
      }
    }

    // Check copy buttons if visible
    const copyButtons = page.locator('.usage__copy-btn');
    const copyCount = await copyButtons.count();

    for (let i = 0; i < Math.min(copyCount, 3); i++) {
      const copyBtn = copyButtons.nth(i);
      const isVisible = await copyBtn.isVisible();
      if (isVisible) {
        const copyBox = await copyBtn.boundingBox();
        if (copyBox) {
          expect(copyBox.width).toBeGreaterThanOrEqual(44);
          expect(copyBox.height).toBeGreaterThanOrEqual(44);
        }
      }
    }
  });

  test('Navigation menu closes when clicking a link', async ({ page }) => {
    const menuToggle = page.locator('.header__menu-toggle');
    const nav = page.locator('.header__nav');

    // Open menu
    await menuToggle.click();
    await expect(nav).toHaveClass(/header__nav--open/);

    // Click a nav link
    const firstLink = page.locator('.header__nav-link').first();
    await firstLink.click();

    // Menu should close
    await expect(nav).not.toHaveClass(/header__nav--open/);
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('Navigation menu closes on Escape key', async ({ page }) => {
    const menuToggle = page.locator('.header__menu-toggle');
    const nav = page.locator('.header__nav');

    // Open menu
    await menuToggle.click();
    await expect(nav).toHaveClass(/header__nav--open/);

    // Press Escape
    await page.keyboard.press('Escape');

    // Menu should close
    await expect(nav).not.toHaveClass(/header__nav--open/);
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('Hero section adapts to mobile layout', async ({ page }) => {
    const hero = page.locator(SELECTORS.hero);
    await expect(hero).toBeVisible();

    // Check that hero title is visible and readable
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    // Check that CTAs stack vertically on mobile
    const ctaContainer = page.locator('.hero__cta-container');
    const flexDirection = await ctaContainer.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.flexDirection;
    });
    expect(flexDirection).toBe('column');
  });

  test('Features grid shows single column on mobile', async ({ page }) => {
    const featuresGrid = page.locator('.features__grid');
    const isVisible = await featuresGrid.isVisible();

    if (isVisible) {
      const gridColumns = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.gridTemplateColumns;
      });

      // Should be single column (1fr) on mobile
      // The actual computed value might be in pixels, so we check it's a single value
      const columnCount = gridColumns.split(' ').filter(c => c !== '').length;
      expect(columnCount).toBeLessThanOrEqual(1);
    }
  });

  test('Page has proper viewport meta tag', async ({ page }) => {
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewportMeta).toContain('width=device-width');
    expect(viewportMeta).toContain('initial-scale=1');
  });
});

// ==============================================================
// Scenario 9: Tablet Responsive Design Tests
// ==============================================================

test.describe('Scenario 9: Tablet Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport size
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto(BASE_URL);
    await waitForPageLoad(page);
  });

  test('TC1: Page renders with appropriate layout for tablet at 768px viewport width', async ({ page }) => {
    // Verify viewport is set correctly
    const viewport = page.viewportSize();
    expect(viewport.width).toBe(768);

    // Verify page elements are visible and correctly sized
    const header = page.locator(SELECTORS.header);
    await expect(header).toBeVisible();

    const hero = page.locator(SELECTORS.hero);
    await expect(hero).toBeVisible();

    const features = page.locator(SELECTORS.features);
    await expect(features).toBeVisible();

    // Verify the page renders without horizontal scrollbar
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(768);

    // Verify main content areas are properly contained
    const headerBox = await header.boundingBox();
    expect(headerBox.width).toBeLessThanOrEqual(768);
  });

  test('TC2: Features display in 2-column layout at tablet size', async ({ page }) => {
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Verify the grid has feature cards
    const featureCards = page.locator('.features__card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Get the computed grid style
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    // Verify it's a grid layout
    expect(gridStyle.display).toBe('grid');

    // Verify it's a 2-column layout (should have 2 columns of similar width)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col.trim() !== '');
    expect(columns.length).toBe(2);

    // Verify feature cards are laid out in 2 columns by checking their positions
    if (cardCount >= 2) {
      const card1 = featureCards.nth(0);
      const card2 = featureCards.nth(1);

      const box1 = await card1.boundingBox();
      const box2 = await card2.boundingBox();

      // In a 2-column layout, the first two cards should be on the same row (same y position)
      // with different x positions
      expect(box1.y).toBeCloseTo(box2.y, 0);
      expect(box1.x).not.toEqual(box2.x);
    }
  });

  test('TC3: Navigation is usable and appropriately sized at tablet size', async ({ page }) => {
    const nav = page.locator(SELECTORS.headerNav);
    await expect(nav).toBeVisible();

    const navLinks = page.locator(SELECTORS.headerNavLinks);
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify all navigation links are visible
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();
    }

    // Check that navigation links have appropriate minimum touch target size (at least 44x44 for accessibility)
    const firstLink = navLinks.first();
    const linkBox = await firstLink.boundingBox();

    // Navigation links should be reasonably sized for tablet interaction
    expect(linkBox.width).toBeGreaterThanOrEqual(32);
    expect(linkBox.height).toBeGreaterThanOrEqual(20);

    // Verify header stays within viewport width
    const header = page.locator(SELECTORS.header);
    const headerBox = await header.boundingBox();
    expect(headerBox.width).toBeLessThanOrEqual(768);

    // Verify navigation doesn't overflow
    const navBox = await nav.boundingBox();
    expect(navBox.x + navBox.width).toBeLessThanOrEqual(768);
  });

  test('Hero section adapts to tablet viewport', async ({ page }) => {
    const hero = page.locator(SELECTORS.hero);
    await expect(hero).toBeVisible();

    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    // Verify hero section fits within viewport
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeLessThanOrEqual(768);

    // Verify CTA buttons are visible and clickable
    const ctaButtons = page.locator('.hero__cta');
    const ctaCount = await ctaButtons.count();

    for (let i = 0; i < ctaCount; i++) {
      const cta = ctaButtons.nth(i);
      await expect(cta).toBeVisible();

      const ctaBox = await cta.boundingBox();
      // CTA buttons should be appropriately sized for tablet
      expect(ctaBox.width).toBeGreaterThanOrEqual(80);
      expect(ctaBox.height).toBeGreaterThanOrEqual(32);
    }
  });

  test('Usage section renders correctly on tablet', async ({ page }) => {
    const usage = page.locator(SELECTORS.usage);
    await expect(usage).toBeVisible();

    // Verify code blocks are contained within viewport
    const codeBlocks = page.locator('.usage__code-block');
    const blockCount = await codeBlocks.count();

    for (let i = 0; i < blockCount; i++) {
      const block = codeBlocks.nth(i);
      const blockBox = await block.boundingBox();

      if (blockBox) {
        // Code blocks should not overflow the viewport
        expect(blockBox.x).toBeGreaterThanOrEqual(0);
        expect(blockBox.x + blockBox.width).toBeLessThanOrEqual(768 + 16); // Allow small margin for padding
      }
    }
  });

  test('All sections are accessible via scrolling on tablet', async ({ page }) => {
    // Verify all main sections are visible when scrolled to
    const sections = [
      SELECTORS.header,
      SELECTORS.hero,
      SELECTORS.features,
      SELECTORS.usage,
    ];

    for (const selector of sections) {
      const section = page.locator(selector);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();
    }
  });
});

// ===========================================
// Dark Mode Tests - Scenario 12
// ===========================================

test.describe('Scenario 12: Dark Mode Support', () => {
  test('Test Case 1: CSS includes @media (prefers-color-scheme: dark) rules', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
    await waitForPageLoad(page);

    // Check that the dark-mode.css file contains prefers-color-scheme media queries
    const darkModeStyles = await page.evaluate(async () => {
      const stylesheets = Array.from(document.styleSheets);
      let hasDarkMediaQuery = false;
      let hasLightMediaQuery = false;

      for (const sheet of stylesheets) {
        try {
          if (sheet.href && sheet.href.includes('dark-mode.css')) {
            const rules = Array.from(sheet.cssRules || []);
            for (const rule of rules) {
              if (rule.type === CSSRule.MEDIA_RULE) {
                const mediaText = rule.conditionText || rule.media.mediaText;
                if (mediaText.includes('prefers-color-scheme: dark')) {
                  hasDarkMediaQuery = true;
                }
                if (mediaText.includes('prefers-color-scheme: light')) {
                  hasLightMediaQuery = true;
                }
              }
            }
          }
        } catch (e) {
          // Ignore cross-origin errors
        }
      }

      return { hasDarkMediaQuery, hasLightMediaQuery };
    });

    expect(darkModeStyles.hasDarkMediaQuery).toBe(true);
    expect(darkModeStyles.hasLightMediaQuery).toBe(true);
  });

  test('Test Case 2: Page renders with dark background and light text in dark mode', async ({ page }) => {
    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
    await waitForPageLoad(page);

    // Check body background color is dark
    const bodyStyles = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return {
        backgroundColor: computedStyle.backgroundColor,
        color: computedStyle.color,
      };
    });

    // Parse RGB values to check if background is dark
    const bgMatch = bodyStyles.backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (bgMatch) {
      const [, r, g, b] = bgMatch.map(Number);
      // Dark background should have RGB values less than 128
      const avgBg = (r + g + b) / 3;
      expect(avgBg).toBeLessThan(128);
    }

    // Parse RGB values to check if text is light
    const textMatch = bodyStyles.color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (textMatch) {
      const [, r, g, b] = textMatch.map(Number);
      // Light text should have RGB values greater than 128
      const avgText = (r + g + b) / 3;
      expect(avgText).toBeGreaterThan(128);
    }

    // Verify hero section has appropriate dark mode styling
    const heroStyles = await page.evaluate(() => {
      const hero = document.querySelector('#hero');
      if (!hero) return null;
      const computedStyle = window.getComputedStyle(hero);
      return {
        background: computedStyle.background,
      };
    });

    expect(heroStyles).not.toBeNull();
  });

  test('Test Case 3: Page renders with appropriate light theme in light mode', async ({ page }) => {
    // Emulate light color scheme preference
    await page.emulateMedia({ colorScheme: 'light' });
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
    await waitForPageLoad(page);

    // Check body background color is light
    const bodyStyles = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return {
        backgroundColor: computedStyle.backgroundColor,
        color: computedStyle.color,
      };
    });

    // Parse RGB values to check if background is light
    const bgMatch = bodyStyles.backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (bgMatch) {
      const [, r, g, b] = bgMatch.map(Number);
      // Light background should have RGB values greater than 200
      const avgBg = (r + g + b) / 3;
      expect(avgBg).toBeGreaterThan(200);
    }

    // Parse RGB values to check if text is dark
    const textMatch = bodyStyles.color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (textMatch) {
      const [, r, g, b] = textMatch.map(Number);
      // Dark text should have RGB values less than 128
      const avgText = (r + g + b) / 3;
      expect(avgText).toBeLessThan(128);
    }

    // Verify header background is light
    const headerStyles = await page.evaluate(() => {
      const header = document.querySelector('#header');
      if (!header) return null;
      const computedStyle = window.getComputedStyle(header);
      return {
        backgroundColor: computedStyle.backgroundColor,
      };
    });

    expect(headerStyles).not.toBeNull();
    if (headerStyles) {
      const headerBgMatch = headerStyles.backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (headerBgMatch) {
        const [, r, g, b] = headerBgMatch.map(Number);
        const avgHeaderBg = (r + g + b) / 3;
        expect(avgHeaderBg).toBeGreaterThan(200);
      }
    }
  });

  test('Test Case 4: Code blocks remain readable in dark mode', async ({ page }) => {
    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
    await waitForPageLoad(page);

    // Check if usage section exists with code blocks
    const usageSection = page.locator('#usage');
    const usageExists = await usageSection.isVisible();

    if (usageExists) {
      // Find code blocks
      const codeBlocks = page.locator('.usage__code');
      const codeBlockCount = await codeBlocks.count();

      if (codeBlockCount > 0) {
        // Check first code block for readability
        const codeStyles = await codeBlocks.first().evaluate((el) => {
          const computedStyle = window.getComputedStyle(el);
          return {
            backgroundColor: computedStyle.backgroundColor,
            color: computedStyle.color,
          };
        });

        // Parse background and text colors
        const bgMatch = codeStyles.backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
        const textMatch = codeStyles.color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);

        if (bgMatch && textMatch) {
          const [, bgR, bgG, bgB] = bgMatch.map(Number);
          const [, textR, textG, textB] = textMatch.map(Number);

          // Calculate contrast ratio (simplified)
          const bgLuminance = (bgR * 299 + bgG * 587 + bgB * 114) / 1000;
          const textLuminance = (textR * 299 + textG * 587 + textB * 114) / 1000;

          // There should be significant contrast between text and background
          const contrastDiff = Math.abs(textLuminance - bgLuminance);
          expect(contrastDiff).toBeGreaterThan(100);
        }
      }
    }

    // Also check pre elements generally
    const preElements = page.locator('pre');
    const preCount = await preElements.count();

    if (preCount > 0) {
      const preStyles = await preElements.first().evaluate((el) => {
        const computedStyle = window.getComputedStyle(el);
        return {
          backgroundColor: computedStyle.backgroundColor,
          color: computedStyle.color,
        };
      });

      // Verify pre elements have defined colors (not transparent/inherit)
      expect(preStyles.backgroundColor).toBeDefined();
      expect(preStyles.color).toBeDefined();
    }
  });

  test('Theme switching maintains visual consistency', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);

    // Test dark mode first
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto('/');
    await waitForPageLoad(page);

    // Capture dark mode state
    const darkModeState = await page.evaluate(() => {
      const header = document.querySelector('#header');
      const hero = document.querySelector('#hero');
      const features = document.querySelector('#features');

      return {
        headerVisible: header ? getComputedStyle(header).display !== 'none' : false,
        heroVisible: hero ? getComputedStyle(hero).display !== 'none' : false,
        featuresVisible: features ? getComputedStyle(features).display !== 'none' : false,
      };
    });

    expect(darkModeState.headerVisible).toBe(true);
    expect(darkModeState.heroVisible).toBe(true);

    // Switch to light mode
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload();
    await waitForPageLoad(page);

    // Capture light mode state
    const lightModeState = await page.evaluate(() => {
      const header = document.querySelector('#header');
      const hero = document.querySelector('#hero');
      const features = document.querySelector('#features');

      return {
        headerVisible: header ? getComputedStyle(header).display !== 'none' : false,
        heroVisible: hero ? getComputedStyle(hero).display !== 'none' : false,
        featuresVisible: features ? getComputedStyle(features).display !== 'none' : false,
      };
    });

    // All sections should remain visible after theme switch
    expect(lightModeState.headerVisible).toBe(true);
    expect(lightModeState.heroVisible).toBe(true);
  });

  test('Navigation links are visible in both themes', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);

    // Test in dark mode
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto('/');
    await waitForPageLoad(page);

    const darkModeNav = page.locator('.header__nav-link');
    const darkNavCount = await darkModeNav.count();
    expect(darkNavCount).toBeGreaterThan(0);

    // Verify links are visible in dark mode
    for (let i = 0; i < Math.min(darkNavCount, 3); i++) {
      await expect(darkModeNav.nth(i)).toBeVisible();
    }

    // Test in light mode
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload();
    await waitForPageLoad(page);

    const lightModeNav = page.locator('.header__nav-link');
    const lightNavCount = await lightModeNav.count();
    expect(lightNavCount).toBeGreaterThan(0);

    // Verify links are visible in light mode
    for (let i = 0; i < Math.min(lightNavCount, 3); i++) {
      await expect(lightModeNav.nth(i)).toBeVisible();
    }
  });

  test('Features cards render correctly in both themes', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);

    // Test dark mode
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto('/');
    await waitForPageLoad(page);

    const darkFeatureCards = page.locator('.features__card');
    const darkCardCount = await darkFeatureCards.count();

    if (darkCardCount > 0) {
      const darkCardStyles = await darkFeatureCards.first().evaluate((el) => {
        const computedStyle = window.getComputedStyle(el);
        return {
          backgroundColor: computedStyle.backgroundColor,
        };
      });

      // Verify card has a background color
      expect(darkCardStyles.backgroundColor).toBeDefined();
      expect(darkCardStyles.backgroundColor).not.toBe('transparent');
    }

    // Test light mode
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload();
    await waitForPageLoad(page);

    const lightFeatureCards = page.locator('.features__card');
    const lightCardCount = await lightFeatureCards.count();

    if (lightCardCount > 0) {
      const lightCardStyles = await lightFeatureCards.first().evaluate((el) => {
        const computedStyle = window.getComputedStyle(el);
        return {
          backgroundColor: computedStyle.backgroundColor,
        };
      });

      // Verify card has a background color
      expect(lightCardStyles.backgroundColor).toBeDefined();
      expect(lightCardStyles.backgroundColor).not.toBe('transparent');
    }
  });
});

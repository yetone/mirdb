/**
 * Cross-Browser Compatibility E2E Tests
 *
 * This test suite verifies that the MirDB homepage works correctly across
 * major browsers: Chrome, Firefox, Safari (WebKit), and Edge.
 *
 * Tests cover:
 * - Page load and rendering
 * - CSS features (Flexbox/Grid layouts)
 * - Visual consistency
 * - Interactive elements
 * - No browser-specific issues
 */

const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Cross-Browser Homepage Functionality
 * Tests run on all configured browsers (Chromium, Firefox, WebKit, Edge)
 */
test.describe('Cross-Browser Compatibility Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Homepage loads correctly in the browser
   * Verifies page loads, title is correct, and main sections are visible
   */
  test('TC1: Homepage loads and displays correctly', async ({ page, browserName }) => {
    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const quickstartSection = page.locator('.quickstart-section');
    await expect(quickstartSection).toBeVisible();

    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify hero content renders correctly
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();
    await expect(heroSubtitle).toContainText('Persistent Key-Value Store');

    // Verify navigation is visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();
  });

  /**
   * Test Case 2: CSS Flexbox layouts render correctly
   * Verifies Flexbox-based layouts work across browsers
   */
  test('TC2: Flexbox layouts render correctly', async ({ page, browserName }) => {
    // Test hero CTA buttons (flex container)
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    const ctaDisplay = await heroCta.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(ctaDisplay).toBe('flex');

    // Verify CTA buttons are rendered
    const primaryBtn = page.locator('.hero-cta .btn-primary');
    const secondaryBtn = page.locator('.hero-cta .btn-secondary');
    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Verify button alignment
    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();
    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    // Test navigation links (flex container)
    const navLinks = page.locator('.nav-links');
    const navDisplay = await navLinks.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(navDisplay).toBe('flex');

    // Test footer links (flex container)
    const footerLinks = page.locator('.footer-links');
    const footerDisplay = await footerLinks.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(footerDisplay).toBe('flex');
  });

  /**
   * Test Case 3: CSS Grid layouts render correctly
   * Verifies Grid-based layouts work across browsers
   */
  test('TC3: CSS Grid layouts render correctly', async ({ page, browserName }) => {
    // Test features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const gridDisplay = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify feature cards are rendered
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(6);

    // Verify each feature card has title and description
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      const title = card.locator('.feature-title');
      const description = card.locator('.feature-description');
      const icon = card.locator('.feature-icon');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
      await expect(icon).toBeVisible();
    }
  });

  /**
   * Test Case 4: Interactive elements function correctly
   * Verifies links, buttons, and hover states work
   */
  test('TC4: Interactive elements work correctly', async ({ page, browserName }) => {
    // Test primary CTA button is clickable and has correct href
    const primaryBtn = page.locator('.hero-cta .btn-primary');
    const primaryHref = await primaryBtn.getAttribute('href');
    expect(primaryHref).toBe('#quick-start');

    // Test secondary CTA button links to GitHub
    const secondaryBtn = page.locator('.hero-cta .btn-secondary');
    const secondaryHref = await secondaryBtn.getAttribute('href');
    expect(secondaryHref).toContain('github.com');

    // Test navigation links have correct hrefs
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();

    const githubNavLink = page.locator('.nav-links a[href*="github"]');
    await expect(githubNavLink).toBeVisible();

    // Verify external links have proper attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const externalCount = await externalLinks.count();
    expect(externalCount).toBeGreaterThan(0);

    // Check that external links have noopener noreferrer
    for (let i = 0; i < externalCount; i++) {
      const rel = await externalLinks.nth(i).getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  /**
   * Test Case 5: No horizontal overflow (visual consistency)
   * Verifies content fits within viewport without horizontal scrollbars
   */
  test('TC5: No horizontal overflow at desktop viewport', async ({ page, browserName }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Check for horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify content is contained within viewport
    const hero = page.locator('.hero');
    const heroBox = await hero.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      expect(heroBox.x).toBeGreaterThanOrEqual(0);
    }
  });

  /**
   * Test Case 6: CSS custom properties (variables) work correctly
   * Verifies CSS variables are applied properly
   */
  test('TC6: CSS custom properties render correctly', async ({ page, browserName }) => {
    // Check that CSS variables are applied
    const body = page.locator('body');
    const bgColor = await body.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background should be set (not default transparent/white)
    expect(bgColor).not.toBe('transparent');
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');

    // Check hero title has gradient text (webkit-background-clip)
    const heroTitle = page.locator('.hero-title');
    const heroTitleStyle = await heroTitle.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        background: style.background,
        backgroundImage: style.backgroundImage,
        color: style.color
      };
    });
    // Title should have some styling applied
    expect(heroTitleStyle.backgroundImage).not.toBe('none');

    // Check feature cards have proper border radius
    const featureCard = page.locator('.feature-card').first();
    const borderRadius = await featureCard.evaluate(el => {
      return window.getComputedStyle(el).borderRadius;
    });
    expect(borderRadius).not.toBe('0px');
  });

  /**
   * Test Case 7: Smooth scroll behavior works
   * Verifies smooth scrolling is enabled
   */
  test('TC7: Smooth scroll behavior is enabled', async ({ page, browserName }) => {
    const htmlScrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(htmlScrollBehavior).toBe('smooth');
  });

  /**
   * Test Case 8: Typography renders consistently
   * Verifies fonts and text rendering work correctly
   */
  test('TC8: Typography renders correctly', async ({ page, browserName }) => {
    // Check body font family is applied
    const bodyFontFamily = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontFamily;
    });
    // Font family should be set (system fonts stack)
    expect(bodyFontFamily).toBeTruthy();
    expect(bodyFontFamily.length).toBeGreaterThan(0);

    // Check code blocks use monospace font
    const codeElement = page.locator('.code-block code').first();
    const codeFontFamily = await codeElement.evaluate(el => {
      return window.getComputedStyle(el).fontFamily;
    });
    // Should contain monospace-related font
    expect(codeFontFamily.toLowerCase()).toMatch(/mono|consolas|menlo|courier/i);

    // Check line height for readability
    const lineHeight = await page.evaluate(() => {
      const body = document.body;
      const style = window.getComputedStyle(body);
      return parseFloat(style.lineHeight) / parseFloat(style.fontSize);
    });
    expect(lineHeight).toBeGreaterThanOrEqual(1.4);
  });

  /**
   * Test Case 9: Images and SVG icons render correctly
   * Verifies visual assets are displayed
   */
  test('TC9: SVG icons render correctly', async ({ page, browserName }) => {
    // Check feature icons are rendered (SVG)
    const featureIcons = page.locator('.feature-icon svg');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBe(6);

    // Each icon should be visible
    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      await expect(icon).toBeVisible();

      // Icon should have dimensions
      const iconBox = await icon.boundingBox();
      expect(iconBox).not.toBeNull();
      if (iconBox) {
        expect(iconBox.width).toBeGreaterThan(0);
        expect(iconBox.height).toBeGreaterThan(0);
      }
    }
  });

  /**
   * Test Case 10: Backdrop filter (glassmorphism) works or degrades gracefully
   * Verifies navbar blur effect
   */
  test('TC10: Navbar backdrop filter applied', async ({ page, browserName }) => {
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    const backdropFilter = await navbar.evaluate(el => {
      return window.getComputedStyle(el).backdropFilter;
    });

    // Backdrop filter should be set (blur effect) or gracefully degrade
    // Some browsers may not support it, but it shouldn't break the layout
    if (backdropFilter && backdropFilter !== 'none') {
      expect(backdropFilter).toContain('blur');
    }

    // Navbar should still be visible and functional regardless
    const navbarBox = await navbar.boundingBox();
    expect(navbarBox).not.toBeNull();
    expect(navbarBox.height).toBeGreaterThan(0);
  });

  /**
   * Test Case 11: Hover states work (where applicable)
   * Verifies hover interactions
   */
  test('TC11: Hover states function correctly', async ({ page, browserName }) => {
    // Hover over feature card and verify it's still interactive
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Get initial state
    const initialTransform = await featureCard.evaluate(el => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over the card
    await featureCard.hover();

    // After hover, the card should still be visible
    await expect(featureCard).toBeVisible();

    // Hover over CTA button
    const primaryBtn = page.locator('.hero-cta .btn-primary');
    await primaryBtn.hover();
    await expect(primaryBtn).toBeVisible();
  });

  /**
   * Test Case 12: Z-index stacking works correctly
   * Verifies fixed navbar stays above content
   */
  test('TC12: Fixed navbar has proper z-index stacking', async ({ page, browserName }) => {
    const navbar = page.locator('.navbar');

    // Verify navbar has fixed position
    const position = await navbar.evaluate(el => {
      return window.getComputedStyle(el).position;
    });
    expect(position).toBe('fixed');

    // Verify navbar has z-index
    const zIndex = await navbar.evaluate(el => {
      return parseInt(window.getComputedStyle(el).zIndex);
    });
    expect(zIndex).toBeGreaterThan(0);

    // Scroll down and verify navbar is still visible
    await page.evaluate(() => window.scrollTo(0, 500));
    await expect(navbar).toBeVisible();

    // Navbar should be at top of viewport
    const navbarBox = await navbar.boundingBox();
    expect(navbarBox).not.toBeNull();
    expect(navbarBox.y).toBe(0);
  });

  /**
   * Test Case 13: Meta tags are present (browser-agnostic SEO)
   * Verifies essential meta tags
   */
  test('TC13: Essential meta tags are present', async ({ page, browserName }) => {
    // Check viewport meta tag
    const viewportMeta = page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toHaveAttribute('content', /width=device-width/);

    // Check description meta tag
    const descriptionMeta = page.locator('meta[name="description"]');
    await expect(descriptionMeta).toHaveAttribute('content', /MirDB/);

    // Check charset
    const charsetMeta = page.locator('meta[charset]');
    await expect(charsetMeta).toHaveAttribute('charset', 'UTF-8');

    // Check Open Graph tags
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toBeAttached();

    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toBeAttached();
  });

  /**
   * Test Case 14: Code blocks are styled and readable
   * Verifies code block styling across browsers
   */
  test('TC14: Code blocks render correctly', async ({ page, browserName }) => {
    // Navigate to code blocks
    const codeBlocks = page.locator('.code-block');
    const blockCount = await codeBlocks.count();
    expect(blockCount).toBeGreaterThanOrEqual(1);

    // Verify each code block is styled
    for (let i = 0; i < blockCount; i++) {
      const block = codeBlocks.nth(i);
      await expect(block).toBeVisible();

      // Check code header exists
      const header = block.locator('.code-header');
      await expect(header).toBeVisible();

      // Check pre element has overflow handling
      const pre = block.locator('pre');
      const overflowX = await pre.evaluate(el => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(overflowX).toBe('auto');

      // Check code content is present
      const code = block.locator('code');
      await expect(code).toBeVisible();
      const codeText = await code.textContent();
      expect(codeText.length).toBeGreaterThan(0);
    }
  });

  /**
   * Test Case 15: All sections have proper semantic structure
   * Verifies HTML5 semantic elements render correctly
   */
  test('TC15: Semantic HTML structure is correct', async ({ page, browserName }) => {
    // Verify nav element
    const nav = page.locator('nav.navbar');
    await expect(nav).toBeVisible();

    // Verify header element (hero section)
    const header = page.locator('header.hero');
    await expect(header).toBeVisible();

    // Verify main element
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(2); // Features and Quick Start

    // Verify footer element
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify single h1 element
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);
  });
});

/**
 * Browser-Specific Tests
 * Tests that check for known browser-specific behaviors
 */
test.describe('Browser-Specific Rendering Checks', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('Gradient text renders without issues', async ({ page, browserName }) => {
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    // Get computed styles
    const styles = await heroTitle.evaluate(el => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundClip: computed.backgroundClip || computed.webkitBackgroundClip,
        textFillColor: computed.webkitTextFillColor || computed.color
      };
    });

    // Text should be visible (either as gradient or fallback)
    const titleBox = await heroTitle.boundingBox();
    expect(titleBox).not.toBeNull();
    expect(titleBox.width).toBeGreaterThan(0);
    expect(titleBox.height).toBeGreaterThan(0);
  });

  test('Transitions are smooth on interactive elements', async ({ page, browserName }) => {
    const btn = page.locator('.btn-primary').first();
    await expect(btn).toBeVisible();

    // Check transition property is set
    const transition = await btn.evaluate(el => {
      return window.getComputedStyle(el).transition;
    });
    expect(transition).not.toBe('none');
    expect(transition).toBeTruthy();
  });

  test('Box-sizing is consistently border-box', async ({ page, browserName }) => {
    // Check various elements use border-box
    const elements = [
      '.feature-card',
      '.btn',
      '.nav-container',
      '.code-block'
    ];

    for (const selector of elements) {
      const element = page.locator(selector).first();
      if (await element.count() > 0) {
        const boxSizing = await element.evaluate(el => {
          return window.getComputedStyle(el).boxSizing;
        });
        expect(boxSizing).toBe('border-box');
      }
    }
  });
});

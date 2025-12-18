// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

/**
 * Cross-Browser Compatibility Tests (NFR-5)
 * Verify homepage renders correctly in modern browsers
 * (Chrome, Firefox, Safari/WebKit, Edge - latest 2 versions)
 *
 * These tests run across all configured browser projects in playwright.config.js:
 * - Chromium (Chrome/Edge)
 * - Firefox
 * - WebKit (Safari)
 */

test.describe('Cross-Browser Compatibility - Page Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1-4: Page renders correctly with all features functional
   * Input: Load page in Chrome/Firefox/Safari/Edge
   * Expected: Page renders correctly with all features functional
   */
  test('Page renders and is visible', async ({ page, browserName }) => {
    // Verify page title is correct
    await expect(page).toHaveTitle(/MirDB/);

    // Verify body is visible
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Log browser info for debugging
    console.log(`Running on browser: ${browserName}`);
  });

  test('Header and navigation render correctly', async ({ page }) => {
    // Header should be visible
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Logo should be visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveText('MirDB');

    // Navigation links should exist
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeAttached();

    // Check navigation links
    const featuresLink = navLinks.locator('a[href="#features"]');
    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    const githubLink = navLinks.locator('a[href*="github.com"]');

    await expect(featuresLink).toBeAttached();
    await expect(gettingStartedLink).toBeAttached();
    await expect(githubLink).toBeAttached();
  });

  test('Hero section renders correctly', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check hero elements
    const heroTitle = heroSection.locator('h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    const description = heroSection.locator('.description');
    await expect(description).toBeVisible();

    // Check CTA buttons
    const ctaButtons = heroSection.locator('.cta-buttons .btn');
    await expect(ctaButtons).toHaveCount(2);

    const primaryBtn = heroSection.locator('.btn-primary');
    await expect(primaryBtn).toBeVisible();
    await expect(primaryBtn).toHaveText('Get Started');

    const secondaryBtn = heroSection.locator('.btn-secondary');
    await expect(secondaryBtn).toBeVisible();
    await expect(secondaryBtn).toContainText('View on GitHub');
  });

  test('Features section renders correctly', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Key Features');

    // Check all three feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify each feature card has required elements
    const memcachedCard = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedCard).toBeVisible();
    await expect(memcachedCard.locator('h3')).toContainText('Memcached Protocol');

    const persistentCard = page.locator('[data-testid="feature-persistent"]');
    await expect(persistentCard).toBeVisible();
    await expect(persistentCard.locator('h3')).toContainText('Persistent Storage');

    const lsmCard = page.locator('[data-testid="feature-lsm"]');
    await expect(lsmCard).toBeVisible();
    await expect(lsmCard.locator('h3')).toContainText('LSM Tree');
  });

  test('Getting Started section renders correctly', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check heading
    const heading = gettingStartedSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Getting Started');

    // Check code blocks exist
    const codeBlocks = gettingStartedSection.locator('pre code');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Commands section renders correctly', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Check heading
    const heading = commandsSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Supported Commands');

    // Check command categories
    const commandCategories = commandsSection.locator('.command-category');
    await expect(commandCategories).toHaveCount(4);
  });

  test('Footer renders correctly', async ({ page }) => {
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Check footer content
    const footerContent = footer.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Check GitHub link
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();

    // Check license text
    await expect(footer).toContainText('MIT');

    // Check copyright
    await expect(footer).toContainText('MirDB');
  });
});

test.describe('Cross-Browser Compatibility - CSS Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
    await page.waitForLoadState('domcontentloaded');
  });

  test('CSS Custom Properties are applied correctly', async ({ page }) => {
    // Check that CSS custom properties are working
    const body = page.locator('body');

    // Get computed color of body text
    const textColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Should have a valid color value (not 'inherit' or empty)
    expect(textColor).toBeTruthy();
    expect(textColor).not.toBe('inherit');

    // Check background color
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(bgColor).toBeTruthy();
  });

  test('Flexbox layout works correctly', async ({ page }) => {
    // Check nav uses flexbox
    const nav = page.locator('.nav');
    const navDisplay = await nav.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(navDisplay).toBe('flex');

    // Check CTA buttons container uses flexbox
    const ctaButtons = page.locator('.cta-buttons');
    const ctaDisplay = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(ctaDisplay).toBe('flex');
  });

  test('CSS Grid layout works correctly', async ({ page }) => {
    // Check features grid uses grid layout
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Check commands grid uses grid layout
    const commandsGrid = page.locator('.commands-grid');
    const commandsDisplay = await commandsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(commandsDisplay).toBe('grid');
  });

  test('Border radius renders correctly', async ({ page }) => {
    // Check button has border-radius
    const button = page.locator('.btn-primary').first();
    const borderRadius = await button.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });

    // Should have a non-zero border radius
    expect(borderRadius).not.toBe('0px');
    expect(borderRadius).toBeTruthy();
  });

  test('Box shadow renders correctly on feature cards', async ({ page }) => {
    const featureCard = page.locator('.feature-card').first();

    // Hover over the card
    await featureCard.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Check box-shadow is applied on hover
    const boxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Should have some box-shadow on hover (not 'none')
    expect(boxShadow).not.toBe('none');
  });

  test('Transitions work correctly', async ({ page }) => {
    // Check button has transition property
    const button = page.locator('.btn-primary').first();
    const transition = await button.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });

    // Should have transition property defined
    expect(transition).toBeTruthy();
    expect(transition).not.toBe('none');
  });

  test('Sticky header works correctly', async ({ page }) => {
    // Check header position is sticky
    const header = page.locator('.header');
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });

    expect(position).toBe('sticky');
  });

  test('Scroll behavior is smooth', async ({ page }) => {
    // Check html has smooth scroll behavior
    const html = page.locator('html');
    const scrollBehavior = await html.evaluate((el) => {
      return window.getComputedStyle(el).scrollBehavior;
    });

    expect(scrollBehavior).toBe('smooth');
  });
});

test.describe('Cross-Browser Compatibility - Interactivity', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
    await page.waitForLoadState('domcontentloaded');
  });

  test('Navigation links are clickable', async ({ page }) => {
    // On larger viewports, nav links are visible
    await page.setViewportSize({ width: 1200, height: 800 });

    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();

    // Verify we scrolled to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.3 });
  });

  test('CTA buttons are clickable', async ({ page }) => {
    const getStartedBtn = page.locator('.hero .btn-primary');
    await getStartedBtn.click();

    // Should navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport({ ratio: 0.3 });
  });

  test('Mobile menu toggle works', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const hamburgerMenu = page.locator('.mobile-menu-btn');
    await expect(hamburgerMenu).toBeVisible();

    const navLinks = page.locator('.nav-links');
    await expect(navLinks).not.toBeVisible();

    // Click to open
    await hamburgerMenu.click();
    await expect(navLinks).toBeVisible();

    // Click to close
    await hamburgerMenu.click();
    await expect(navLinks).not.toBeVisible();
  });

  test('Focus states work correctly', async ({ page }) => {
    // Tab to first focusable element
    await page.keyboard.press('Tab');

    // Check focus outline is visible
    const focusedElement = page.locator(':focus');
    const outlineWidth = await focusedElement.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.outlineWidth;
    });

    // Should have visible focus outline
    expect(outlineWidth).toBeTruthy();
  });

  test('External links have correct attributes', async ({ page }) => {
    // Check GitHub links that should open in new tabs have target="_blank" and rel="noopener noreferrer"
    // Links with target="_blank" should have security attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');
      await expect(link).toHaveAttribute('rel', /noopener/);
    }

    // Verify at least some GitHub links exist
    const githubLinks = page.locator('a[href*="github.com"]');
    const githubCount = await githubLinks.count();
    expect(githubCount).toBeGreaterThan(0);
  });
});

test.describe('Cross-Browser Compatibility - Visual Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
    await page.waitForLoadState('domcontentloaded');
  });

  test('Font family is applied correctly', async ({ page }) => {
    const body = page.locator('body');
    const fontFamily = await body.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Should have system font stack
    expect(fontFamily).toBeTruthy();
    // System fonts should be in the stack
    expect(fontFamily.toLowerCase()).toMatch(/system-ui|segoe|roboto|arial|sans-serif/);
  });

  test('Code blocks use monospace font', async ({ page }) => {
    const codeBlock = page.locator('pre code').first();
    const fontFamily = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Should have monospace font
    expect(fontFamily.toLowerCase()).toMatch(/fira|monaco|consolas|monospace/);
  });

  test('Images and SVGs render correctly', async ({ page }) => {
    // Check SVG icons in feature cards
    const svgIcons = page.locator('.feature-icon svg');
    const count = await svgIcons.count();
    expect(count).toBe(3);

    for (let i = 0; i < count; i++) {
      const svg = svgIcons.nth(i);
      await expect(svg).toBeVisible();

      // Check SVG has valid dimensions
      const width = await svg.evaluate((el) => el.getAttribute('width'));
      const height = await svg.evaluate((el) => el.getAttribute('height'));
      expect(width).toBeTruthy();
      expect(height).toBeTruthy();
    }
  });

  test('Colors have sufficient contrast', async ({ page }) => {
    // Get primary button colors
    const button = page.locator('.btn-primary').first();
    const bgColor = await button.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const textColor = await button.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Both should be defined (basic check)
    expect(bgColor).toBeTruthy();
    expect(textColor).toBeTruthy();

    // Primary button should have white text
    expect(textColor).toMatch(/rgb\(255,\s*255,\s*255\)|white/);
  });

  test('No horizontal overflow on page', async ({ page }) => {
    // Check that body doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);

    // Allow 1px tolerance
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);
  });

  test('Line height is readable', async ({ page }) => {
    const body = page.locator('body');
    const lineHeight = await body.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).lineHeight);
    });

    // Line height should be at least 1.4 for readability
    // Computed as absolute pixel value, so check it's reasonable relative to font size
    const fontSize = await body.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    const ratio = lineHeight / fontSize;
    expect(ratio).toBeGreaterThanOrEqual(1.4);
  });
});

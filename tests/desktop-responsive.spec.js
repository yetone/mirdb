// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

// Desktop viewport dimensions (Full HD, common desktop resolution)
const DESKTOP_VIEWPORT = { width: 1920, height: 1080 };

test.describe('Desktop Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport before navigating
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto(pageUrl);
  });

  /**
   * Test Case 1: Page renders with desktop layout at 1920px viewport width
   * Input: Load page at 1920px viewport width
   * Expected: Page renders with desktop layout, max-width constraints applied
   */
  test('TC1: Page renders with desktop layout and max-width constraints at 1920px', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Verify the page renders properly
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Check that the navigation container has max-width constraint
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Get the computed max-width of nav
    const navMaxWidth = await nav.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    // Should have max-width of 1200px (--max-width custom property)
    expect(navMaxWidth).toBe('1200px');

    // Check that the hero section has max-width constraint
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const heroMaxWidth = await heroSection.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(heroMaxWidth).toBe('1200px');

    // Check that the features grid has max-width constraint
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const featuresGridMaxWidth = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(featuresGridMaxWidth).toBe('1200px');

    // Verify the content is centered (margin auto)
    const navMargin = await nav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.marginLeft + ' ' + style.marginRight;
    });
    // Margin should be auto (rendered as actual pixel values, both should be equal)
    const heroMargin = await heroSection.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        left: parseFloat(style.marginLeft),
        right: parseFloat(style.marginRight),
      };
    });
    // With 1920px viewport and 1200px max-width, margins should be approximately (1920-1200)/2 = 360px each
    expect(heroMargin.left).toBeGreaterThan(300);
    expect(heroMargin.right).toBeGreaterThan(300);
    // Margins should be approximately equal (centered)
    expect(Math.abs(heroMargin.left - heroMargin.right)).toBeLessThan(50);

    // Verify page doesn't have horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);
  });

  /**
   * Test Case 2: Feature cards display in 3-column grid on desktop
   * Input: Check feature cards layout on desktop
   * Expected: Feature cards display in 3-column grid
   */
  test('TC2: Feature cards display in 3-column grid on desktop', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes of all feature cards
    const card1 = featureCards.nth(0);
    const card2 = featureCards.nth(1);
    const card3 = featureCards.nth(2);

    const box1 = await card1.boundingBox();
    const box2 = await card2.boundingBox();
    const box3 = await card3.boundingBox();

    // Verify all cards are rendered
    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();
    expect(box3).not.toBeNull();

    // Verify all three cards are on the same row (Y coordinates should be approximately equal)
    // Allow 5px tolerance for any minor rendering differences
    expect(Math.abs(box1.y - box2.y)).toBeLessThan(5);
    expect(Math.abs(box2.y - box3.y)).toBeLessThan(5);
    expect(Math.abs(box1.y - box3.y)).toBeLessThan(5);

    // Verify cards are arranged horizontally (X coordinates should be different)
    // Card 2 should be to the right of Card 1
    expect(box2.x).toBeGreaterThan(box1.x + box1.width - 10); // 10px overlap tolerance

    // Card 3 should be to the right of Card 2
    expect(box3.x).toBeGreaterThan(box2.x + box2.width - 10);

    // Verify cards have reasonable widths for 3-column layout
    // With max-width 1200px and 3 cards with gaps, each card should be roughly 300-400px
    expect(box1.width).toBeGreaterThan(250);
    expect(box1.width).toBeLessThan(450);
    expect(box2.width).toBeGreaterThan(250);
    expect(box2.width).toBeLessThan(450);
    expect(box3.width).toBeGreaterThan(250);
    expect(box3.width).toBeLessThan(450);

    // Verify the grid uses the expected grid layout
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');
  });

  /**
   * Test Case 3: Full navigation bar is visible without hamburger menu
   * Input: Check navigation on desktop
   * Expected: Full navigation bar is visible without hamburger menu
   */
  test('TC3: Full navigation bar is visible without hamburger menu on desktop', async ({ page }) => {
    // Verify nav links are visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify hamburger menu is hidden on desktop
    const hamburgerMenu = page.locator('.mobile-menu-btn');
    await expect(hamburgerMenu).not.toBeVisible();

    // Verify all navigation links are visible and accessible
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toHaveText('Getting Started');

    const githubLink = navLinks.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('GitHub');

    // Verify nav links are displayed horizontally (flex row layout)
    const navLinksDisplay = await navLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        flexDirection: style.flexDirection,
      };
    });
    expect(navLinksDisplay.display).toBe('flex');
    // On desktop, flex-direction should be row (default) not column
    expect(navLinksDisplay.flexDirection).not.toBe('column');

    // Verify navigation links are on the same horizontal line
    const featuresBox = await featuresLink.boundingBox();
    const gettingStartedBox = await gettingStartedLink.boundingBox();
    const githubBox = await githubLink.boundingBox();

    expect(featuresBox).not.toBeNull();
    expect(gettingStartedBox).not.toBeNull();
    expect(githubBox).not.toBeNull();

    // All links should be on approximately the same Y position (same row)
    expect(Math.abs(featuresBox.y - gettingStartedBox.y)).toBeLessThan(10);
    expect(Math.abs(gettingStartedBox.y - githubBox.y)).toBeLessThan(10);

    // Links should be arranged horizontally
    expect(gettingStartedBox.x).toBeGreaterThan(featuresBox.x);
    expect(githubBox.x).toBeGreaterThan(gettingStartedBox.x);
  });
});

test.describe('Desktop Layout - Additional Verifications', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto(pageUrl);
  });

  test('Hero section uses full desktop styling', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check hero heading font size is desktop size (3.5rem = 56px)
    const heroTitle = heroSection.locator('h1');
    const fontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Desktop font size should be 3.5rem (56px)
    expect(fontSize).toBeGreaterThanOrEqual(54);
    expect(fontSize).toBeLessThanOrEqual(60);

    // Check tagline font size is desktop size (1.5rem = 24px)
    const tagline = heroSection.locator('.tagline');
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(22);
    expect(taglineFontSize).toBeLessThanOrEqual(26);
  });

  test('CTA buttons display horizontally on desktop', async ({ page }) => {
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    const primaryBtn = ctaButtons.locator('.btn-primary');
    const secondaryBtn = ctaButtons.locator('.btn-secondary');

    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Get bounding boxes
    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    // Buttons should be on the same row (similar Y position)
    expect(Math.abs(primaryBox.y - secondaryBox.y)).toBeLessThan(10);

    // Buttons should be side by side (different X positions)
    expect(secondaryBox.x).toBeGreaterThan(primaryBox.x);
  });

  test('Commands grid displays in multi-column layout on desktop', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    const commandCategories = commandsSection.locator('.command-category');
    const count = await commandCategories.count();
    expect(count).toBe(4);

    // Get bounding boxes of first two command categories
    const cat1 = commandCategories.nth(0);
    const cat2 = commandCategories.nth(1);

    const box1 = await cat1.boundingBox();
    const box2 = await cat2.boundingBox();

    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();

    // On desktop, categories should be in a grid (at least 2 columns)
    // Check that category 2 is to the right of category 1 (same row)
    expect(box2.x).toBeGreaterThan(box1.x);
    // They should be on approximately the same row
    expect(Math.abs(box1.y - box2.y)).toBeLessThan(10);
  });

  test('Footer content displays horizontally on desktop', async ({ page }) => {
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    const footerContent = page.locator('.footer-content');
    const footerDisplay = await footerContent.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        flexDirection: style.flexDirection,
      };
    });

    expect(footerDisplay.display).toBe('flex');
    // On desktop, footer should use row layout (default flex-direction)
    expect(footerDisplay.flexDirection).not.toBe('column');
  });
});

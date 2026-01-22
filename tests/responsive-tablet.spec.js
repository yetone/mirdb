// @ts-check
const { test, expect } = require('@playwright/test');

// Tablet viewport configuration (iPad dimensions)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet dimensions
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: Layout adapts to two-column grid where appropriate at 768px width', async ({ page }) => {
    // Verify the page loads correctly at tablet viewport
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify the viewport is set correctly
    const viewportSize = page.viewportSize();
    expect(viewportSize.width).toBe(768);

    // Check that the roadmap section uses grid layout
    const roadmapContent = page.locator('.roadmap-content');
    await roadmapContent.scrollIntoViewIfNeeded();
    await expect(roadmapContent).toBeVisible();

    // Get computed style for the roadmap content grid
    const roadmapDisplay = await roadmapContent.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(roadmapDisplay).toBe('grid');

    // Verify roadmap has appropriate column structure for tablet
    const roadmapGridColumns = await roadmapContent.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // At 768px with minmax(280px, 1fr), should show 2 columns
    // gridTemplateColumns returns computed pixel values
    const columnCount = roadmapGridColumns.split(' ').filter(col => col !== '').length;
    expect(columnCount).toBeGreaterThanOrEqual(1);
    expect(columnCount).toBeLessThanOrEqual(2);

    // Verify footer content adapts for tablet
    const footerContent = page.locator('.footer-content');
    await footerContent.scrollIntoViewIfNeeded();
    await expect(footerContent).toBeVisible();

    // Footer should use flex layout
    const footerDisplay = await footerContent.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(footerDisplay).toBe('flex');
  });

  test('TC2: Features grid displays in 2-column or 3-column layout on tablet', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify the grid is using CSS grid
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Get the computed grid template columns
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // At 768px with minmax(300px, 1fr), should display 2 columns
    // The gridTemplateColumns returns the computed pixel values
    const columnWidths = gridColumns.split(' ').filter(col => col !== '');
    const columnCount = columnWidths.length;

    // At 768px, with min-width 300px per card, we expect 2 columns
    // (768px container with padding leaves ~720px, fitting 2 x 300px columns)
    expect(columnCount).toBeGreaterThanOrEqual(2);
    expect(columnCount).toBeLessThanOrEqual(3);

    // Verify all 6 feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Check each feature card is visible
    for (let i = 0; i < 6; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Verify feature cards have appropriate width for tablet
    const firstCard = featureCards.first();
    const cardBoundingBox = await firstCard.boundingBox();
    expect(cardBoundingBox).not.toBeNull();

    // Each card should take roughly half the container width (minus gap)
    // With container padding and gap, each card should be around 300-380px
    expect(cardBoundingBox.width).toBeGreaterThan(280);
    expect(cardBoundingBox.width).toBeLessThan(500);
  });

  test('TC3: Usage GIF scales appropriately for tablet viewport', async ({ page }) => {
    // Navigate to demo section
    const demoSection = page.locator('#demo');
    await demoSection.scrollIntoViewIfNeeded();
    await expect(demoSection).toBeVisible();

    // Get the demo GIF
    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    // Check that the GIF has width: 100% applied
    const gifWidth = await demoGif.evaluate((el) => {
      return window.getComputedStyle(el).width;
    });

    // The GIF should scale to fit its container (demo-content has max-width: 900px)
    // At 768px viewport, it should scale down appropriately
    const gifBoundingBox = await demoGif.boundingBox();
    expect(gifBoundingBox).not.toBeNull();

    // GIF width should be less than or equal to viewport width minus padding
    // Container has 1.5rem (24px) padding on each side
    const maxExpectedWidth = 768 - 48; // 720px
    expect(gifBoundingBox.width).toBeLessThanOrEqual(maxExpectedWidth + 5); // Small tolerance

    // GIF should maintain reasonable width (not too narrow)
    expect(gifBoundingBox.width).toBeGreaterThan(500);

    // Verify the GIF is responsive with width: 100%
    const gifStyleWidth = await demoGif.evaluate((el) => {
      return window.getComputedStyle(el).width;
    });
    // Width should be a pixel value representing the scaled image
    expect(gifStyleWidth).toMatch(/^\d+(\.\d+)?px$/);

    // Verify the demo-content container is centered and appropriately sized
    const demoContent = page.locator('.demo-content');
    const demoContentBox = await demoContent.boundingBox();
    expect(demoContentBox).not.toBeNull();

    // Demo content should be centered with appropriate margins
    expect(demoContentBox.width).toBeLessThanOrEqual(900);
    expect(demoContentBox.width).toBeGreaterThan(600);
  });

  test('Navigation adapts appropriately for tablet viewport', async ({ page }) => {
    // Check that navigation is visible
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // At 768px (tablet), navigation should stack vertically per the CSS media query
    // But first check if the breakpoint is at 768px or below
    const navFlexDirection = await nav.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });

    // The CSS has @media (max-width: 768px) which should apply at exactly 768px
    // Navigation should be column direction
    expect(navFlexDirection).toBe('column');

    // Check nav links are centered
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    const navLinksJustify = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).justifyContent;
    });
    expect(navLinksJustify).toBe('center');

    // Verify all navigation links are visible
    const featureLink = page.locator('.nav-links a[href="#features"]');
    const demoLink = page.locator('.nav-links a[href="#demo"]');
    const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
    const githubLink = page.locator('.nav-links .btn-secondary');

    await expect(featureLink).toBeVisible();
    await expect(demoLink).toBeVisible();
    await expect(quickStartLink).toBeVisible();
    await expect(githubLink).toBeVisible();
  });

  test('Hero section adapts for tablet viewport', async ({ page }) => {
    // Verify hero section is visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Hero CTAs should remain in row layout on tablet (not column like mobile)
    const heroCtas = page.locator('.hero-ctas');
    await expect(heroCtas).toBeVisible();

    const ctasFlexDirection = await heroCtas.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    // At 768px (which is max-width breakpoint), it should switch to column
    expect(ctasFlexDirection).toBe('column');

    // Verify hero title is appropriately sized
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();

    const heroFontSize = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    // At 768px breakpoint, font-size should be 2.25rem (36px)
    expect(parseFloat(heroFontSize)).toBe(36);

    // Verify hero logo is visible and appropriately sized
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    const logoBoundingBox = await heroLogo.boundingBox();
    expect(logoBoundingBox).not.toBeNull();
    // Logo should be reasonably sized for tablet
    expect(logoBoundingBox.width).toBeGreaterThan(100);
  });

  test('Code blocks scale appropriately for tablet viewport', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Get code blocks
    const codeBlocks = page.locator('.code-block');
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Check code block width is appropriate for tablet
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();

    // Code block should have max-width: 700px and be centered
    // At 768px viewport, it should fit within the viewport with padding
    expect(codeBlockBox.width).toBeLessThanOrEqual(720);
    expect(codeBlockBox.width).toBeGreaterThan(300);

    // Verify code block has horizontal scroll capability if needed
    const overflowX = await firstCodeBlock.locator('pre').evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(overflowX).toBe('auto');

    // Verify copy button is visible and accessible
    const copyButton = firstCodeBlock.locator('.copy-btn');
    await expect(copyButton).toBeVisible();
  });
});

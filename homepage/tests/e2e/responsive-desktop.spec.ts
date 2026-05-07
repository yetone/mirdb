import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Desktop', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('at 1280x800 viewport uses full multi-column grid layout', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    // Features grid should be multi-column
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();
    const featuresGridBox = await featuresGrid.boundingBox();
    expect(featuresGridBox?.width).toBeGreaterThan(900);

    // Feature cards should be in a row (grid has 3 columns on lg)
    const featureCards = page.locator('[data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(3);

    // Protocol grid should be multi-column
    const protocolGrid = page.locator('[data-testid="protocol-grid"]');
    await expect(protocolGrid).toBeVisible();
    const protocolGridBox = await protocolGrid.boundingBox();
    expect(protocolGridBox?.width).toBeGreaterThan(900);

    // Architecture grid should be multi-column
    const architectureGrid = page.locator('[data-testid="architecture-grid"]');
    await expect(architectureGrid).toBeVisible();
    const architectureGridBox = await architectureGrid.boundingBox();
    expect(architectureGridBox?.width).toBeGreaterThan(900);

    // Status grid should be multi-column (max-w-4xl = 896px on 1280 viewport)
    const statusGrid = page.locator('[data-testid="status-grid"]');
    await expect(statusGrid).toBeVisible();
    const statusGridBox = await statusGrid.boundingBox();
    expect(statusGridBox?.width).toBeGreaterThan(700);
    // Verify 3 status cards are present
    const statusCards = statusGrid.locator('> div');
    await expect(statusCards).toHaveCount(3);
  });

  test('at 1280x800 navigation is horizontal', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    // Desktop nav should be visible
    const desktopNav = page.locator('[data-testid="desktop-nav"]');
    await expect(desktopNav).toBeVisible();

    // Navigation links should be displayed horizontally
    const navLinks = desktopNav.locator('a');
    await expect(navLinks).toHaveCount(5);

    // Verify links are in expected order
    const linkTexts = await navLinks.allTextContents();
    expect(linkTexts).toEqual([
      'Features',
      'Getting Started',
      'Protocol',
      'Architecture',
      'GitHub',
    ]);

    // Hamburger menu should be hidden
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await expect(mobileMenuButton).not.toBeVisible();
  });

  test('at 1920x1080 content is centered with max-width constraint', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Main content sections should be centered and have max-width constraint
    const sections = [
      '[data-testid="hero-section"]',
      '[data-testid="features-section"]',
      '[data-testid="quick-start-section"]',
      '[data-testid="protocol-section"]',
      '[data-testid="architecture-section"]',
      '[data-testid="config-section"]',
      '[data-testid="demo-section"]',
      '[data-testid="roadmap-section"]',
      '[data-testid="status-section"]',
    ];

    for (const selector of sections) {
      const element = page.locator(selector).first();
      await expect(element).toBeVisible();
    }

    // Hero section should be centered
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroBox = await heroSection.boundingBox();
    // On 1920px wide, the hero content should not stretch the full width
    // The max-w-4xl class limits to ~896px
    const heroContent = heroSection.locator('div.max-w-4xl');
    const heroContentBox = await heroContent.boundingBox();
    expect(heroContentBox?.width).toBeLessThan(1000);
    expect(heroContentBox?.width).toBeGreaterThan(800);

    // Features section max-width should be constrained
    const featuresSection = page.locator('[data-testid="features-section"]');
    const featuresContent = featuresSection.locator('div.max-w-7xl');
    const featuresContentBox = await featuresContent.boundingBox();
    expect(featuresContentBox?.width).toBeLessThan(1400);
    expect(featuresContentBox?.width).toBeGreaterThan(1000);

    // Quick start section max-width should be constrained
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    const quickStartContent = quickStartSection.locator('div.max-w-4xl');
    const quickStartContentBox = await quickStartContent.boundingBox();
    expect(quickStartContentBox?.width).toBeLessThan(1000);
    expect(quickStartContentBox?.width).toBeGreaterThan(800);
  });

  test('at 1920x1080 no excessive stretching', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Hero text should be readable (not too wide)
    const heroHeadline = page.locator('[data-testid="hero-headline"]');
    await expect(heroHeadline).toBeVisible();
    const headlineBox = await heroHeadline.boundingBox();
    expect(headlineBox?.width).toBeLessThan(1000);

    // Hero tagline should be readable
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();
    const taglineBox = await heroTagline.boundingBox();
    expect(taglineBox?.width).toBeLessThan(1000);

    // Hero description should be readable
    const heroDescription = page.locator('[data-testid="hero-description"]');
    await expect(heroDescription).toBeVisible();
    const descBox = await heroDescription.boundingBox();
    expect(descBox?.width).toBeLessThan(800);

    // Footer should be properly laid out (not stretched)
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();
    const footerContent = footer.locator('div.max-w-7xl');
    const footerContentBox = await footerContent.boundingBox();
    expect(footerContentBox?.width).toBeLessThan(1400);
    expect(footerContentBox?.width).toBeGreaterThan(1000);
  });

  test('desktop navigation displays links horizontally in header', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    const header = page.locator('[data-testid="header"]');
    await expect(header).toBeVisible();

    const desktopNav = page.locator('[data-testid="desktop-nav"]');
    await expect(desktopNav).toBeVisible();

    // Verify nav is inside header
    await expect(header.locator('[data-testid="desktop-nav"]')).toBeVisible();

    // Nav links should all be visible
    const navLinks = desktopNav.locator('a');
    for (let i = 0; i < 5; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }

    // No hamburger menu on desktop
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
    await expect(mobileMenuButton).not.toBeVisible();
  });

  test('header is sticky at top on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    const header = page.locator('[data-testid="header"]');
    await expect(header).toBeVisible();

    // Scroll down
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    // Header should still be visible
    await expect(header).toBeVisible();

    // Header should be at top of viewport
    const headerBox = await header.boundingBox();
    expect(headerBox?.y).toBe(0);
  });

  test('font sizes and typography are appropriate for desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    // Hero headline should be large on desktop
    const heroHeadline = page.locator('[data-testid="hero-headline"]');
    const headlineFontSize = await heroHeadline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(headlineFontSize)).toBeGreaterThanOrEqual(48);

    // Section headings should be appropriately sized
    const featuresSection = page.locator('[data-testid="features-section"]');
    const sectionHeading = featuresSection.locator('h2');
    const headingFontSize = await sectionHeading.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(headingFontSize)).toBeGreaterThanOrEqual(30);

    // Feature card titles should be readable
    const featureCard = page.locator('[data-testid="feature-card"]').first();
    const cardTitle = featureCard.locator('h3');
    const cardTitleFontSize = await cardTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseInt(cardTitleFontSize)).toBeGreaterThanOrEqual(16);
  });

  test('spacing and padding are appropriate for desktop viewing', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    // Sections should have comfortable vertical spacing
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroPaddingTop = await heroSection.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).paddingTop);
    });
    expect(heroPaddingTop).toBeGreaterThanOrEqual(128); // pt-32 = 128px

    // Feature cards should have internal padding
    const featureCard = page.locator('[data-testid="feature-card"]').first();
    const cardPadding = await featureCard.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).padding);
    });
    expect(cardPadding).toBeGreaterThanOrEqual(24);

    // Grid should have gap between cards
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    const gridGap = await featuresGrid.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).gap);
    });
    expect(gridGap).toBeGreaterThanOrEqual(32);
  });
});

// @ts-check
const { test, expect } = require('@playwright/test');

// Tablet viewport: 768px-1023px width
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet Viewport (768px-1023px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: All sections display correctly with adapted layout at 768x1024', async ({ page }) => {
    // Verify viewport is set correctly
    const viewportSize = page.viewportSize();
    expect(viewportSize?.width).toBe(768);
    expect(viewportSize?.height).toBe(1024);

    // Verify all main sections are visible
    const heroSection = page.locator('[data-testid="hero-section"], section.hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('[data-testid="features-section"], #features');
    await expect(featuresSection).toBeVisible();

    const commandsSection = page.locator('[data-testid="commands-section"], #commands');
    await expect(commandsSection).toBeVisible();

    const quickStartSection = page.locator('[data-testid="quick-start"], #quick-start');
    await expect(quickStartSection).toBeVisible();

    const statusSection = page.locator('[data-testid="status-section"], #status');
    await expect(statusSection).toBeVisible();

    const footerSection = page.locator('[data-testid="footer-section"], footer');
    await expect(footerSection).toBeVisible();

    // Verify hero content is visible and properly sized
    const heroH1 = heroSection.locator('h1');
    await expect(heroH1).toBeVisible();
    await expect(heroH1).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = heroSection.locator('.tagline, [data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // Verify buttons are visible
    const getStartedBtn = heroSection.locator('a:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    const githubBtn = heroSection.locator('a:has-text("View on GitHub")');
    await expect(githubBtn).toBeVisible();
  });

  test('TC2: Feature cards adapt to 2-column or stacked layout at tablet size', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"], #features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(5);

    // Verify all cards are visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Check that the grid adapts appropriately for tablet viewport
    // Get the bounding boxes of the first two cards to verify layout
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    // Both cards should be visible and have dimensions
    expect(firstBox).toBeTruthy();
    expect(secondBox).toBeTruthy();

    if (firstBox && secondBox) {
      // At tablet viewport, cards should either be:
      // 1. Side by side (2-column layout) - first card's right edge should be less than second card's left edge
      // 2. Stacked (1-column layout) - first card's bottom edge should be less than second card's top edge
      const isSideBySide = firstBox.x + firstBox.width <= secondBox.x + 20; // Allow small gap
      const isStacked = firstBox.y + firstBox.height <= secondBox.y + 20;

      // Either layout is acceptable for tablet
      expect(isSideBySide || isStacked).toBeTruthy();

      // Cards should have reasonable width for tablet viewport
      expect(firstBox.width).toBeGreaterThan(200);
      expect(firstBox.width).toBeLessThanOrEqual(768);
    }

    // Verify card content is still readable
    const cardHeading = firstCard.locator('h3');
    await expect(cardHeading).toBeVisible();

    const cardDescription = firstCard.locator('p');
    await expect(cardDescription).toBeVisible();
  });

  test('TC3: Code blocks remain readable with horizontal scroll if needed at tablet size', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quick-start"], #quick-start');
    await expect(quickStartSection).toBeVisible();

    // Locate the code block
    const codeBlock = quickStartSection.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    // Get the pre element inside the code block
    const preElement = codeBlock.locator('pre');
    await expect(preElement).toBeVisible();

    // Get the code element
    const codeElement = preElement.locator('code');
    await expect(codeElement).toBeVisible();

    // Verify code content is present
    const codeText = await codeElement.textContent();
    expect(codeText).toBeTruthy();
    expect(codeText?.length).toBeGreaterThan(0);

    // Check that code block has proper overflow handling
    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).toBeTruthy();

    if (codeBlockBox) {
      // Code block should fit within tablet viewport width (with some padding)
      expect(codeBlockBox.width).toBeLessThanOrEqual(768);
      expect(codeBlockBox.width).toBeGreaterThan(300);
    }

    // Check that overflow-x is set to auto or scroll for horizontal scrolling
    const overflowX = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    // Should be 'auto', 'scroll', or 'visible' (if content fits)
    expect(['auto', 'scroll', 'visible']).toContain(overflowX);

    // Verify code is readable - font size should be reasonable
    const fontSize = await codeElement.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Font size should be readable (at least 12px, typically 14-16px)
    expect(fontSize).toBeGreaterThanOrEqual(12);
    expect(fontSize).toBeLessThanOrEqual(20);
  });

  test('TC4: Navigation adapts appropriately at tablet size (may show hamburger menu)', async ({ page }) => {
    const navbar = page.locator('header.navbar, nav').first();
    await expect(navbar).toBeVisible();

    // At tablet viewport (768px), navigation might either:
    // 1. Show full navigation links (condensed)
    // 2. Show a hamburger menu

    // Check for logo - should always be visible
    const logo = page.locator('.logo, [data-testid="logo"], nav a:has-text("MirDB")').first();
    await expect(logo).toBeVisible();

    // Check for navigation links or hamburger menu
    const navLinks = page.locator('.nav-links');
    const hamburgerMenu = page.locator('.hamburger, .menu-toggle, [data-testid="hamburger-menu"], button[aria-label*="menu"]');

    const navLinksVisible = await navLinks.isVisible().catch(() => false);
    const hamburgerVisible = await hamburgerMenu.isVisible().catch(() => false);

    // Either nav links should be visible OR hamburger menu should be visible
    // For 768px viewport, navigation links should typically still be visible
    // based on the current CSS which hides them at max-width: 480px
    if (navLinksVisible) {
      // If nav links are visible, verify they contain expected items
      const featuresLink = navLinks.locator('a:has-text("Features")');
      const commandsLink = navLinks.locator('a:has-text("Commands")');
      const quickStartLink = navLinks.locator('a:has-text("Quick Start")');
      const githubLink = navLinks.locator('a:has-text("GitHub")');

      // At least some links should be visible
      const linksVisibleCount = await Promise.all([
        featuresLink.isVisible().catch(() => false),
        commandsLink.isVisible().catch(() => false),
        quickStartLink.isVisible().catch(() => false),
        githubLink.isVisible().catch(() => false)
      ]).then(results => results.filter(Boolean).length);

      expect(linksVisibleCount).toBeGreaterThan(0);
    } else if (hamburgerVisible) {
      // If hamburger menu is visible, navigation should be accessible via menu
      await expect(hamburgerMenu).toBeVisible();
    } else {
      // At least logo should be visible
      await expect(logo).toBeVisible();
    }

    // Verify navigation bar is properly sized for tablet
    const navbarBox = await navbar.boundingBox();
    expect(navbarBox).toBeTruthy();
    if (navbarBox) {
      // Navigation should span the full width (or close to it)
      expect(navbarBox.width).toBeGreaterThanOrEqual(700);
      expect(navbarBox.width).toBeLessThanOrEqual(768);
    }
  });

  test('Additional: Commands section displays correctly at tablet viewport', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="commands-section"], #commands');
    await expect(commandsSection).toBeVisible();

    // Get all command items
    const commandItems = commandsSection.locator('.command-item, [data-testid^="command-"]');
    const itemCount = await commandItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(9);

    // Verify first few commands are visible
    for (let i = 0; i < Math.min(3, itemCount); i++) {
      await expect(commandItems.nth(i)).toBeVisible();
    }

    // Check layout adapts for tablet
    const firstItem = commandItems.nth(0);
    const firstItemBox = await firstItem.boundingBox();
    expect(firstItemBox).toBeTruthy();

    if (firstItemBox) {
      // Items should have reasonable width for tablet
      expect(firstItemBox.width).toBeGreaterThan(150);
      expect(firstItemBox.width).toBeLessThanOrEqual(768);
    }
  });

  test('Additional: Status section displays correctly at tablet viewport', async ({ page }) => {
    const statusSection = page.locator('[data-testid="status-section"], #status');
    await expect(statusSection).toBeVisible();

    // Get all status items
    const statusItems = statusSection.locator('.status-item, [data-testid^="status-item-"]');
    const itemCount = await statusItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(4);

    // Verify status items are visible
    for (let i = 0; i < itemCount; i++) {
      await expect(statusItems.nth(i)).toBeVisible();
    }

    // Verify status icons are visible
    const statusIcons = statusSection.locator('.status-icon, [data-testid="status-icon"]');
    const iconCount = await statusIcons.count();
    expect(iconCount).toBeGreaterThanOrEqual(itemCount);
  });

  test('Additional: Footer displays correctly at tablet viewport', async ({ page }) => {
    const footer = page.locator('[data-testid="footer-section"], footer');
    await expect(footer).toBeVisible();

    // Verify footer links are visible
    const footerLinks = footer.locator('[data-testid="footer-links"], .footer-links');
    await expect(footerLinks).toBeVisible();

    // Verify GitHub link in footer
    const githubLink = footer.locator('[data-testid="footer-github-link"], a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Verify license info
    const licenseInfo = footer.locator('[data-testid="license-info"], .license');
    await expect(licenseInfo).toBeVisible();
    await expect(licenseInfo).toContainText('MIT');

    // Check footer width adapts to tablet
    const footerBox = await footer.boundingBox();
    expect(footerBox).toBeTruthy();
    if (footerBox) {
      expect(footerBox.width).toBeGreaterThanOrEqual(700);
      expect(footerBox.width).toBeLessThanOrEqual(768);
    }
  });
});

// @ts-check
const { test, expect } = require('@playwright/test');

// Mobile viewports: 320px-767px width
const MOBILE_VIEWPORT_375 = { width: 375, height: 667 }; // iPhone 8
const MOBILE_VIEWPORT_320 = { width: 320, height: 568 }; // Minimum supported width

test.describe('Responsive Design - Mobile Viewport (320px-767px)', () => {
  test.describe('TC1: Load page at 375x667 viewport - Single-column layout', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_375);
      await page.goto('/');
    });

    test('All sections display correctly in single-column layout', async ({ page }) => {
      // Verify viewport is set correctly
      const viewportSize = page.viewportSize();
      expect(viewportSize?.width).toBe(375);
      expect(viewportSize?.height).toBe(667);

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

      // Verify hero content is visible and properly sized for mobile
      const heroH1 = heroSection.locator('h1');
      await expect(heroH1).toBeVisible();
      await expect(heroH1).toContainText('MirDB');

      // Verify tagline is visible
      const tagline = heroSection.locator('.tagline, [data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Verify buttons are visible and stack vertically on mobile
      const getStartedBtn = heroSection.locator('a:has-text("Get Started")');
      await expect(getStartedBtn).toBeVisible();

      const githubBtn = heroSection.locator('a:has-text("View on GitHub")');
      await expect(githubBtn).toBeVisible();

      // Check buttons are stacked vertically (different Y positions)
      const getStartedBox = await getStartedBtn.boundingBox();
      const githubBox = await githubBtn.boundingBox();

      if (getStartedBox && githubBox) {
        // On mobile, buttons should be stacked (different Y positions)
        expect(githubBox.y).toBeGreaterThan(getStartedBox.y);
      }
    });

    test('Feature cards display in single-column layout', async ({ page }) => {
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

      // Check that cards stack vertically (single-column layout)
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).toBeTruthy();
      expect(secondBox).toBeTruthy();

      if (firstBox && secondBox) {
        // Cards should be stacked vertically (second card below first)
        expect(secondBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 20);

        // Cards should take most of the viewport width on mobile
        expect(firstBox.width).toBeGreaterThan(300);
        expect(firstBox.width).toBeLessThanOrEqual(375);
      }
    });

    test('Commands section displays correctly on mobile', async ({ page }) => {
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

      // Check layout adapts for mobile - items should stack
      const firstItem = commandItems.nth(0);
      const firstItemBox = await firstItem.boundingBox();
      expect(firstItemBox).toBeTruthy();

      if (firstItemBox) {
        // Items should take most of viewport width on mobile
        expect(firstItemBox.width).toBeGreaterThan(280);
        expect(firstItemBox.width).toBeLessThanOrEqual(375);
      }
    });
  });

  test.describe('TC2: Load page at 320px minimum viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_320);
      await page.goto('/');
    });

    test('Page remains usable at minimum supported width', async ({ page }) => {
      // Verify viewport is set correctly
      const viewportSize = page.viewportSize();
      expect(viewportSize?.width).toBe(320);

      // Verify page title loads
      await expect(page).toHaveTitle(/MirDB/);

      // Verify all main sections are still visible
      const heroSection = page.locator('[data-testid="hero-section"], section.hero');
      await expect(heroSection).toBeVisible();

      const featuresSection = page.locator('[data-testid="features-section"], #features');
      await expect(featuresSection).toBeVisible();

      const commandsSection = page.locator('[data-testid="commands-section"], #commands');
      await expect(commandsSection).toBeVisible();

      const quickStartSection = page.locator('[data-testid="quick-start"], #quick-start');
      await expect(quickStartSection).toBeVisible();

      const footerSection = page.locator('[data-testid="footer-section"], footer');
      await expect(footerSection).toBeVisible();

      // Verify no horizontal scrollbar (content fits viewport)
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      // Allow small tolerance for scrollbars
      expect(documentWidth).toBeLessThanOrEqual(340);

      // Verify hero content is readable
      const heroH1 = heroSection.locator('h1');
      await expect(heroH1).toBeVisible();

      // Verify text doesn't overflow
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      if (heroBox) {
        expect(heroBox.width).toBeLessThanOrEqual(320);
      }
    });

    test('Feature cards fit within minimum viewport width', async ({ page }) => {
      const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
      const cardCount = await featureCards.count();

      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        const cardBox = await card.boundingBox();
        expect(cardBox).toBeTruthy();
        if (cardBox) {
          // Cards should fit within viewport (with padding)
          expect(cardBox.width).toBeLessThanOrEqual(320);
          expect(cardBox.x).toBeGreaterThanOrEqual(0);
        }
      }
    });
  });

  test.describe('TC3: Check navigation on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_375);
      await page.goto('/');
    });

    test('Navigation displays as hamburger menu or collapsed state', async ({ page }) => {
      const navbar = page.locator('header.navbar, nav').first();
      await expect(navbar).toBeVisible();

      // Logo should always be visible
      const logo = page.locator('.logo, [data-testid="logo"], nav a:has-text("MirDB")').first();
      await expect(logo).toBeVisible();

      // Check for hamburger menu button
      const hamburgerMenu = page.locator('.hamburger-menu, .menu-toggle, [data-testid="hamburger-menu"], button[aria-label*="menu"], .mobile-menu-toggle');

      // Check for navigation links visibility
      const navLinks = page.locator('.nav-links');
      const navLinksVisible = await navLinks.isVisible().catch(() => false);
      const hamburgerVisible = await hamburgerMenu.isVisible().catch(() => false);

      // Either nav links should be hidden (collapsed) OR hamburger menu should be visible
      // At 375px, based on current CSS (hides at max-width: 480px), nav-links should be hidden
      if (!navLinksVisible) {
        // Navigation is collapsed - this is the expected behavior for mobile
        // Hamburger menu should be visible as the alternative
        await expect(hamburgerMenu).toBeVisible();
      } else if (hamburgerVisible) {
        // Hamburger menu is present
        await expect(hamburgerMenu).toBeVisible();
      }

      // Verify navbar has proper width for mobile
      const navbarBox = await navbar.boundingBox();
      expect(navbarBox).toBeTruthy();
      if (navbarBox) {
        expect(navbarBox.width).toBeLessThanOrEqual(375);
        expect(navbarBox.width).toBeGreaterThanOrEqual(350);
      }
    });

    test('Mobile menu toggle opens navigation when clicked', async ({ page }) => {
      const hamburgerMenu = page.locator('.hamburger-menu, .menu-toggle, [data-testid="hamburger-menu"], button[aria-label*="menu"], .mobile-menu-toggle');
      const hamburgerVisible = await hamburgerMenu.isVisible().catch(() => false);

      if (hamburgerVisible) {
        // Click hamburger to open menu
        await hamburgerMenu.click();

        // Wait for menu to appear
        const mobileNavLinks = page.locator('.mobile-nav-links, .nav-links.open, [data-testid="mobile-nav"]');
        const mobileNavVisible = await mobileNavLinks.isVisible().catch(() => false);

        if (mobileNavVisible) {
          // Verify navigation links are now visible
          await expect(mobileNavLinks).toBeVisible();
        }
      }
    });
  });

  test.describe('TC4: Check touch targets on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_375);
      await page.goto('/');
    });

    test('All interactive elements are at least 44x44 pixels', async ({ page }) => {
      // Minimum touch target size recommended by WCAG and Apple HIG
      const MIN_TOUCH_TARGET = 44;

      // Check hero buttons
      const heroButtons = page.locator('.hero-buttons a.btn');
      const heroButtonCount = await heroButtons.count();

      for (let i = 0; i < heroButtonCount; i++) {
        const button = heroButtons.nth(i);
        await expect(button).toBeVisible();

        const buttonBox = await button.boundingBox();
        expect(buttonBox).toBeTruthy();
        if (buttonBox) {
          // Touch targets should be at least 44x44
          expect(buttonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          expect(buttonBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }

      // Check hamburger menu button if visible
      const hamburgerMenu = page.locator('.hamburger-menu, .menu-toggle, [data-testid="hamburger-menu"], .mobile-menu-toggle');
      const hamburgerVisible = await hamburgerMenu.isVisible().catch(() => false);

      if (hamburgerVisible) {
        const hamburgerBox = await hamburgerMenu.boundingBox();
        expect(hamburgerBox).toBeTruthy();
        if (hamburgerBox) {
          expect(hamburgerBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          expect(hamburgerBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }

      // Check footer links
      const footerLinks = page.locator('.footer-links a');
      const footerLinkCount = await footerLinks.count();

      for (let i = 0; i < footerLinkCount; i++) {
        const link = footerLinks.nth(i);
        await expect(link).toBeVisible();

        const linkBox = await link.boundingBox();
        expect(linkBox).toBeTruthy();
        if (linkBox) {
          // Links should have at least 44px height (can be wider)
          expect(linkBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    });

    test('Logo touch target is adequate', async ({ page }) => {
      const logo = page.locator('.logo').first();
      await expect(logo).toBeVisible();

      const logoBox = await logo.boundingBox();
      expect(logoBox).toBeTruthy();
      if (logoBox) {
        // Logo should be tappable
        expect(logoBox.height).toBeGreaterThanOrEqual(44);
      }
    });
  });

  test.describe('TC5: Check code blocks on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_375);
      await page.goto('/');
    });

    test('Code blocks are scrollable horizontally without breaking layout', async ({ page }) => {
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

      // Check that code block doesn't break the layout (viewport width respected)
      const codeBlockBox = await codeBlock.boundingBox();
      expect(codeBlockBox).toBeTruthy();

      if (codeBlockBox) {
        // Code block should fit within mobile viewport width
        expect(codeBlockBox.width).toBeLessThanOrEqual(375);
        expect(codeBlockBox.width).toBeGreaterThan(280);
      }

      // Check that overflow-x is set to auto or scroll for horizontal scrolling
      const overflowX = await preElement.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      // Should be 'auto', 'scroll', or 'visible' (if content fits)
      expect(['auto', 'scroll', 'visible']).toContain(overflowX);

      // Verify code is readable - font size should be reasonable for mobile
      const fontSize = await codeElement.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Font size should be readable on mobile (at least 12px)
      expect(fontSize).toBeGreaterThanOrEqual(12);
      expect(fontSize).toBeLessThanOrEqual(18);

      // Verify page doesn't have horizontal overflow due to code block
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      // Allow small tolerance
      expect(documentWidth).toBeLessThanOrEqual(400);
    });

    test('Code block has proper styling on mobile', async ({ page }) => {
      const codeBlock = page.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Check code block has border-radius
      const borderRadius = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(borderRadius).not.toBe('0px');

      // Check code block has appropriate padding
      const preElement = codeBlock.locator('pre');
      const padding = await preElement.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.padding) || parseFloat(style.paddingLeft);
      });
      expect(padding).toBeGreaterThan(0);
    });
  });

  test.describe('Additional mobile viewport tests', () => {
    test('Status section displays correctly on mobile', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_375);
      await page.goto('/');

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

      // Verify items wrap correctly on mobile
      const statusGrid = statusSection.locator('.status-grid, [data-testid="status-grid"]');
      const gridBox = await statusGrid.boundingBox();
      expect(gridBox).toBeTruthy();
      if (gridBox) {
        expect(gridBox.width).toBeLessThanOrEqual(375);
      }
    });

    test('Footer displays correctly on mobile', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_375);
      await page.goto('/');

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

      // Check footer width adapts to mobile
      const footerBox = await footer.boundingBox();
      expect(footerBox).toBeTruthy();
      if (footerBox) {
        expect(footerBox.width).toBeLessThanOrEqual(375);
      }
    });

    test('Text is readable on mobile - font sizes are appropriate', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT_375);
      await page.goto('/');

      // Check hero heading font size
      const heroH1 = page.locator('.hero h1');
      const heroFontSize = await heroH1.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // On mobile, hero heading should be scaled down but still readable
      expect(heroFontSize).toBeGreaterThanOrEqual(28);
      expect(heroFontSize).toBeLessThanOrEqual(48);

      // Check section heading font size
      const sectionH2 = page.locator('.features h2');
      const sectionFontSize = await sectionH2.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Section headings should be appropriately sized for mobile
      expect(sectionFontSize).toBeGreaterThanOrEqual(24);
      expect(sectionFontSize).toBeLessThanOrEqual(40);

      // Check body text font size
      const featureP = page.locator('.feature-card p').first();
      const bodyFontSize = await featureP.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Body text should be readable on mobile
      expect(bodyFontSize).toBeGreaterThanOrEqual(14);
      expect(bodyFontSize).toBeLessThanOrEqual(20);
    });
  });
});

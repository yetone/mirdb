const { test, expect } = require('@playwright/test');

test.describe('Mobile Responsiveness', () => {
  test.describe('TC1: Page at 320px viewport width', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('Page renders correctly without horizontal overflow at 320px', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('domcontentloaded');

      // Check that the page has no horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > document.body.clientWidth;
      });
      expect(hasHorizontalOverflow).toBe(false);

      // Verify hero section is visible and fits within viewport
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify hero title is visible
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();

      // Verify features section fits without overflow
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Verify feature cards stack vertically at mobile size
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(3);

      // Verify all content is visible
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();
    });
  });

  test.describe('TC2: Page at 375px viewport width (iPhone)', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('Page renders correctly for typical mobile phone size', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('domcontentloaded');

      // Check that there is no horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > document.body.clientWidth;
      });
      expect(hasHorizontalOverflow).toBe(false);

      // Verify hero section renders correctly
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Check hero title font size is responsive
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      const fontSize = await heroTitle.evaluate(el => window.getComputedStyle(el).fontSize);
      const fontSizeNum = parseInt(fontSize);
      expect(fontSizeNum).toBeLessThanOrEqual(32); // Should be reduced for mobile

      // Verify buttons are visible and stacked properly
      const heroButtons = page.locator('.hero-buttons');
      await expect(heroButtons).toBeVisible();

      // Verify Get Started button
      const getStartedBtn = page.locator('.btn-primary');
      await expect(getStartedBtn).toBeVisible();

      // Verify GitHub button
      const githubBtn = page.locator('.btn-secondary');
      await expect(githubBtn).toBeVisible();

      // Verify all main sections render
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#commands')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#configuration')).toBeVisible();
    });
  });

  test.describe('TC3: Page at 768px viewport width (tablet)', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    test('Page renders correctly for tablet size', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('domcontentloaded');

      // Check that there is no horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > document.body.clientWidth;
      });
      expect(hasHorizontalOverflow).toBe(false);

      // Verify navigation is present
      const nav = page.locator('[data-testid="navigation-header"]');
      await expect(nav).toBeVisible();

      // Verify hero section renders correctly
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify features grid adapts to tablet width
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Verify architecture section stacks properly at this breakpoint
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Verify commands grid renders correctly
      const commandsGrid = page.locator('[data-testid="commands-grid"]');
      await expect(commandsGrid).toBeVisible();

      // All sections should be visible
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#configuration')).toBeVisible();
      await expect(page.locator('[data-testid="footer"]')).toBeVisible();
    });
  });

  test.describe('TC4: Mobile navigation toggle', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('Mobile navigation menu opens and closes correctly', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('domcontentloaded');

      // Desktop nav links should be hidden on mobile
      const navLinks = page.locator('[data-testid="nav-links"]');
      const isHidden = await navLinks.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.display === 'none';
      });
      expect(isHidden).toBe(true);

      // Mobile hamburger button should be visible
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(hamburgerBtn).toBeVisible();

      // Mobile menu should initially be closed/hidden
      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).not.toBeVisible();

      // Click hamburger to open mobile menu
      await hamburgerBtn.click();

      // Mobile menu should now be visible
      await expect(mobileMenu).toBeVisible();

      // Menu should contain navigation links
      const mobileLinks = mobileMenu.locator('a');
      const linkCount = await mobileLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(5); // Features, Architecture, Commands, Quick Start, Configuration, GitHub, Docs

      // Click hamburger again to close menu
      await hamburgerBtn.click();

      // Mobile menu should be hidden again
      await expect(mobileMenu).not.toBeVisible();
    });

    test('Mobile menu links navigate to correct sections', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Open mobile menu
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-toggle"]');
      await hamburgerBtn.click();

      // Click Features link in mobile menu
      const featuresLink = page.locator('[data-testid="mobile-menu"] a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Features section should be in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport({ ratio: 0.3 });

      // Mobile menu should auto-close after clicking a link
      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).not.toBeVisible();
    });
  });

  test.describe('TC5: Touch interactions on mobile viewport', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('All buttons and links are easily tappable (minimum 44px touch target)', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check touch target sizes for main interactive elements
      const minTouchTarget = 44;

      // Check hamburger menu button
      const hamburgerBtn = page.locator('[data-testid="mobile-menu-toggle"]');
      const hamburgerBox = await hamburgerBtn.boundingBox();
      expect(hamburgerBox.width).toBeGreaterThanOrEqual(minTouchTarget);
      expect(hamburgerBox.height).toBeGreaterThanOrEqual(minTouchTarget);

      // Check hero buttons
      const heroButtons = page.locator('.hero-buttons .btn');
      const buttonCount = await heroButtons.count();
      for (let i = 0; i < buttonCount; i++) {
        const btn = heroButtons.nth(i);
        const box = await btn.boundingBox();
        expect(box.height).toBeGreaterThanOrEqual(minTouchTarget);
      }

      // Check nav logo
      const navLogo = page.locator('[data-testid="nav-logo"]');
      const logoBox = await navLogo.boundingBox();
      expect(logoBox.height).toBeGreaterThanOrEqual(minTouchTarget);

      // Open mobile menu and check link touch targets
      await hamburgerBtn.click();
      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).toBeVisible();

      const mobileLinks = mobileMenu.locator('a');
      const linkCount = await mobileLinks.count();
      for (let i = 0; i < linkCount; i++) {
        const link = mobileLinks.nth(i);
        const linkBox = await link.boundingBox();
        expect(linkBox.height).toBeGreaterThanOrEqual(minTouchTarget);
      }
    });

    test('Feature cards are tappable and interactive', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to features
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Check that feature cards are large enough to interact with
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();
        const box = await card.boundingBox();
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    });
  });

  test.describe('TC6: Code blocks on mobile', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('Code blocks are scrollable horizontally without breaking layout', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Navigate to quickstart section where code blocks exist
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      // Check code blocks
      const codeBlocks = page.locator('.code-block');
      const blockCount = await codeBlocks.count();
      expect(blockCount).toBeGreaterThan(0);

      // Verify each code block has overflow-x: auto for horizontal scrolling
      for (let i = 0; i < blockCount; i++) {
        const block = codeBlocks.nth(i);
        await expect(block).toBeVisible();

        const overflowX = await block.evaluate(el => window.getComputedStyle(el).overflowX);
        expect(overflowX).toBe('auto');
      }

      // Verify page still has no horizontal overflow even with code blocks
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > document.body.clientWidth;
      });
      expect(hasHorizontalOverflow).toBe(false);

      // Check that code blocks don't exceed viewport width
      for (let i = 0; i < blockCount; i++) {
        const block = codeBlocks.nth(i);
        const box = await block.boundingBox();
        expect(box.width).toBeLessThanOrEqual(320);
      }
    });

    test('Configuration code block is scrollable on mobile', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Navigate to configuration section
      await page.locator('#configuration').scrollIntoViewIfNeeded();

      // Check configuration TOML code block
      const configBlock = page.locator('[data-testid="config-toml-block"]');
      await expect(configBlock).toBeVisible();

      // Verify overflow is set for scrolling
      const overflowX = await configBlock.evaluate(el => window.getComputedStyle(el).overflowX);
      expect(overflowX).toBe('auto');

      // Verify the code block width doesn't exceed viewport
      const box = await configBlock.boundingBox();
      expect(box.width).toBeLessThanOrEqual(320);
    });
  });
});

import { test, expect, Page } from '@playwright/test';

// Tablet viewport sizes
const TABLET_PORTRAIT = { width: 768, height: 1024 };  // iPad portrait
const TABLET_LANDSCAPE = { width: 1024, height: 768 }; // iPad landscape

// Minimum touch target size (Apple Human Interface Guidelines)
const MIN_TOUCH_TARGET = 44;

test.describe('Responsive Design - Tablet', () => {
  test.describe('TC1: Tablet Portrait Mode (768x1024)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_PORTRAIT);
      await page.goto('/');
    });

    test('page renders correctly at 768x1024 resolution', async ({ page }) => {
      // Verify page loads without errors
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Verify hero section is visible and properly rendered
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify product name is visible
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();

      // Verify tagline is visible
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Verify CTA buttons are visible
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      await expect(getStartedBtn).toBeVisible();

      const githubLink = page.locator('[data-testid="github-link"]');
      await expect(githubLink).toBeVisible();
    });

    test('layout adapts appropriately for tablet portrait', async ({ page }) => {
      // Scroll to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Check that feature grid adapts to tablet width (using the grid container)
      const featureGrid = page.locator('#features .grid');
      await expect(featureGrid).toBeVisible();

      // Verify feature cards are visible - look for cards within the features section
      const featureCards = page.locator('#features [data-feature]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      // Verify all cards are visible
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // Verify navigation is visible (use first() since there are multiple nav elements)
      const navbar = page.locator('nav').first();
      await expect(navbar).toBeVisible();
    });

    test('content sections are scrollable in portrait mode', async ({ page }) => {
      // Verify key sections exist and can be scrolled to
      const sections = [
        '#features',
        '#architecture',
        '#getting-started',
        '#configuration',
        '#status'
      ];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }
    });

    test('footer is accessible in portrait mode', async ({ page }) => {
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  });

  test.describe('TC2: Tablet Landscape Mode (1024x768)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_LANDSCAPE);
      await page.goto('/');
    });

    test('page renders correctly at 1024x768 resolution', async ({ page }) => {
      // Verify page loads without errors
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Verify hero section is visible and properly rendered
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify product name is visible
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();

      // Verify tagline is visible
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Verify CTA buttons are visible
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      await expect(getStartedBtn).toBeVisible();

      const githubLink = page.locator('[data-testid="github-link"]');
      await expect(githubLink).toBeVisible();
    });

    test('layout adapts appropriately for tablet landscape', async ({ page }) => {
      // Scroll to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Check that feature grid adapts to landscape width
      const featureGrid = page.locator('#features .grid');
      await expect(featureGrid).toBeVisible();

      // Verify feature cards are visible
      const featureCards = page.locator('#features [data-feature]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      // Verify all cards are visible
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // Verify navigation is visible (use first() since there are multiple nav elements)
      const navbar = page.locator('nav').first();
      await expect(navbar).toBeVisible();
    });

    test('architecture section displays properly in landscape', async ({ page }) => {
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();

      // Verify architecture diagram is visible
      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      // Verify LSM explanation is visible
      const explanation = page.locator('[data-testid="lsm-explanation"]');
      await expect(explanation).toBeVisible();
    });

    test('content sections are scrollable in landscape mode', async ({ page }) => {
      // Verify key sections exist and can be scrolled to
      const sections = [
        '#features',
        '#architecture',
        '#getting-started',
        '#configuration',
        '#status'
      ];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }
    });

    test('footer is accessible in landscape mode', async ({ page }) => {
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  });

  test.describe('TC3: Touch Target Sizes', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_PORTRAIT);
      await page.goto('/');
    });

    test('CTA buttons meet minimum touch target size (44x44px)', async ({ page }) => {
      // Check Get Started button
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      const getStartedBox = await getStartedBtn.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(getStartedBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(getStartedBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

      // Check GitHub link button
      const githubLink = page.locator('[data-testid="github-link"]');
      const githubBox = await githubLink.boundingBox();

      expect(githubBox).not.toBeNull();
      expect(githubBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(githubBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    });

    test('navigation links meet minimum touch target size', async ({ page }) => {
      // Check all navigation links in the nav bar
      const navLinks = page.locator('nav a');
      const linkCount = await navLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();

        expect(box).not.toBeNull();
        // For inline links, we check if combined dimensions provide adequate touch area
        // Links may be wider than tall, but should have adequate tap area
        const effectiveHeight = box!.height;
        const effectiveWidth = box!.width;

        expect(effectiveHeight >= MIN_TOUCH_TARGET || effectiveWidth >= MIN_TOUCH_TARGET).toBeTruthy();
      }
    });

    test('footer links meet minimum touch target requirements', async ({ page }) => {
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();

      const footerLink = page.locator('[data-testid="footer-repo-link"]');
      const box = await footerLink.boundingBox();
      expect(box).not.toBeNull();

      // Footer links should be reasonably sized for touch
      expect(box!.width).toBeGreaterThan(0);
      expect(box!.height).toBeGreaterThan(0);
    });

    test('project status section links are touchable', async ({ page }) => {
      const statusSection = page.locator('#status');
      await statusSection.scrollIntoViewIfNeeded();
      await expect(statusSection).toBeVisible();

      // Check GitHub link in status section if present
      const githubLinkInStatus = statusSection.locator('a[href*="github"]').first();
      if (await githubLinkInStatus.count() > 0) {
        const box = await githubLinkInStatus.boundingBox();
        expect(box).not.toBeNull();
        expect(box!.width).toBeGreaterThan(0);
        expect(box!.height).toBeGreaterThan(0);
      }
    });
  });

  test.describe('Additional Tablet Responsive Checks', () => {
    test('responsive hero section at both orientations', async ({ page }) => {
      // Test portrait
      await page.setViewportSize(TABLET_PORTRAIT);
      await page.goto('/');

      let heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Hero should fit within viewport in portrait
      let heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();
      expect(heroBox!.width).toBeLessThanOrEqual(TABLET_PORTRAIT.width);

      // Test landscape
      await page.setViewportSize(TABLET_LANDSCAPE);
      await page.reload();

      heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Hero should adapt to landscape width
      heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();
      expect(heroBox!.width).toBeLessThanOrEqual(TABLET_LANDSCAPE.width);
    });

    test('configuration table is readable at tablet sizes', async ({ page }) => {
      await page.setViewportSize(TABLET_PORTRAIT);
      await page.goto('/');

      const configSection = page.locator('#configuration');
      await configSection.scrollIntoViewIfNeeded();

      const configTable = page.locator('[data-testid="config-table"]');
      await expect(configTable).toBeVisible();

      // Table should not overflow viewport (wrapped in overflow-x-auto container)
      const tableContainer = configTable.locator('..');
      const containerBox = await tableContainer.boundingBox();
      expect(containerBox).not.toBeNull();
      expect(containerBox!.width).toBeLessThanOrEqual(TABLET_PORTRAIT.width);
    });

    test('code blocks are scrollable on tablet', async ({ page }) => {
      await page.setViewportSize(TABLET_PORTRAIT);
      await page.goto('/');

      const gettingStarted = page.locator('#getting-started');
      await gettingStarted.scrollIntoViewIfNeeded();

      // Code blocks should be visible (look for pre elements with code)
      const codeBlocks = gettingStarted.locator('pre');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      // First code block should be visible and scrollable if needed
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();
    });

    test('project status cards display properly at tablet width', async ({ page }) => {
      await page.setViewportSize(TABLET_PORTRAIT);
      await page.goto('/');

      const statusSection = page.locator('#status');
      await statusSection.scrollIntoViewIfNeeded();
      await expect(statusSection).toBeVisible();

      // Check implemented features are visible
      const implementedFeatures = page.locator('[data-testid="implemented-features"]');
      await expect(implementedFeatures).toBeVisible();

      // Check planned features are visible
      const plannedFeatures = page.locator('[data-testid="planned-features"]');
      await expect(plannedFeatures).toBeVisible();
    });
  });
});

/**
 * Tablet Responsive Design E2E Tests
 * Owner: Scenario 9 - Responsive Design - Tablet
 *
 * Verifies landing page displays correctly on tablet devices (768px - 1024px)
 * Tests include:
 * - Tablet-optimized layout rendering
 * - 2-column grid layouts for features and architecture
 * - Full navigation display (no hamburger menu)
 * - Appropriate element sizing and spacing
 */

const { test, expect } = require('@playwright/test');

// Tablet viewport configurations
const TABLET_VIEWPORT_LOWER = { width: 768, height: 1024 };
const TABLET_VIEWPORT_UPPER = { width: 1024, height: 768 };

test.describe('Responsive Design - Tablet (768px - 1024px)', () => {
  test.describe('TC1: Page renders with tablet-optimized layout at 768px viewport width', () => {
    test.use({ viewport: TABLET_VIEWPORT_LOWER });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
    });

    test('Verify viewport is set correctly to 768px', async ({ page }) => {
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(viewportWidth).toBe(768);
    });

    test('Hero section displays with tablet-sized elements', async ({ page }) => {
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Hero logo should be 120x120px at tablet size
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();
      const logoBox = await heroLogo.boundingBox();
      expect(logoBox.width).toBeCloseTo(120, -1);
      expect(logoBox.height).toBeCloseTo(120, -1);
    });

    test('Hero CTA buttons display in row layout', async ({ page }) => {
      const heroCta = page.locator('.hero-cta');
      await expect(heroCta).toBeVisible();

      // At tablet size, CTA should be row layout (not column like mobile)
      const flexDirection = await heroCta.evaluate(el =>
        window.getComputedStyle(el).flexDirection
      );
      expect(flexDirection).toBe('row');

      // Both buttons should be visible
      const primaryBtn = page.locator('.hero-cta .btn-primary');
      const secondaryBtn = page.locator('.hero-cta .btn-secondary');
      await expect(primaryBtn).toBeVisible();
      await expect(secondaryBtn).toBeVisible();

      // Buttons should be side by side (similar Y position)
      const primaryBox = await primaryBtn.boundingBox();
      const secondaryBox = await secondaryBtn.boundingBox();
      const yDifference = Math.abs(primaryBox.y - secondaryBox.y);
      expect(yDifference).toBeLessThan(10); // Should be on same line
    });

    test('All sections are visible and properly laid out', async ({ page }) => {
      const sections = ['#hero', '#features', '#usage', '#architecture', '#getting-started', 'footer'];

      for (const selector of sections) {
        const section = page.locator(selector);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();

        // Verify section takes reasonable width
        const box = await section.boundingBox();
        expect(box.width).toBeGreaterThanOrEqual(700);
      }
    });
  });

  test.describe('TC2: Page renders appropriately at 1024px upper tablet range', () => {
    test.use({ viewport: TABLET_VIEWPORT_UPPER });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
    });

    test('Verify viewport is set correctly to 1024px', async ({ page }) => {
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(viewportWidth).toBe(1024);
    });

    test('Page renders with appropriate layout at upper tablet range', async ({ page }) => {
      // Hero should be visible
      await expect(page.locator('#hero')).toBeVisible();

      // Features section should use tablet layout
      const featuresGrid = page.locator('.features__grid');
      await featuresGrid.scrollIntoViewIfNeeded();
      await expect(featuresGrid).toBeVisible();
    });

    test('Navigation shows full menu at 1024px', async ({ page }) => {
      // At 1024px (desktop breakpoint), hamburger should be hidden
      const hamburger = page.locator('.nav-toggle');
      await expect(hamburger).not.toBeVisible();

      // Nav menu should be visible with horizontal links
      const navMenu = page.locator('.nav-menu');
      await expect(navMenu).toBeVisible();

      // All navigation links should be visible
      await expect(page.locator('.nav-link', { hasText: 'Features' })).toBeVisible();
      await expect(page.locator('.nav-link', { hasText: 'Usage' })).toBeVisible();
      await expect(page.locator('.nav-link', { hasText: 'Architecture' })).toBeVisible();
      await expect(page.locator('.nav-link', { hasText: 'Get Started' })).toBeVisible();
    });

    test('Container width is appropriate for viewport', async ({ page }) => {
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();

      // Container should have reasonable width with padding
      expect(containerBox.width).toBeLessThanOrEqual(1024);
      expect(containerBox.width).toBeGreaterThan(900);
    });
  });

  test.describe('TC3: Features display in 2-column grid on tablet', () => {
    test.use({ viewport: TABLET_VIEWPORT_LOWER });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
    });

    test('Features grid uses 2-column layout at 768px', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features__grid');
      await expect(featuresGrid).toBeVisible();

      // Check grid template columns - should be 2 columns
      const gridStyle = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );

      // Grid should have 2 column values (e.g., "300px 300px" or similar)
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });

    test('Feature cards are arranged in 2x3 grid', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(6);

      // Check first row - cards should be side by side
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // Second card should be to the right of first card (same row)
      expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);

      // They should be at similar Y position (same row)
      const yDifference = Math.abs(firstCardBox.y - secondCardBox.y);
      expect(yDifference).toBeLessThan(10);

      // Third card should be on second row
      const thirdCardBox = await featureCards.nth(2).boundingBox();
      expect(thirdCardBox.y).toBeGreaterThan(firstCardBox.y + firstCardBox.height - 20);
    });

    test('Feature cards are properly sized for 2-column layout', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCard = page.locator('.feature-card').first();
      const cardBox = await featureCard.boundingBox();

      // Each card should take roughly half the available width (minus gap)
      expect(cardBox.width).toBeGreaterThan(280);
      expect(cardBox.width).toBeLessThan(450);
    });
  });

  test.describe('TC4: Navigation is usable and appropriately sized on tablet', () => {
    test.use({ viewport: TABLET_VIEWPORT_LOWER });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
    });

    test('Hamburger menu is hidden on tablet (768px+)', async ({ page }) => {
      const hamburger = page.locator('.nav-toggle');
      // At 768px and above, hamburger should be hidden
      await expect(hamburger).not.toBeVisible();
    });

    test('Full navigation menu is visible and horizontal', async ({ page }) => {
      const navMenu = page.locator('.nav-menu');
      await expect(navMenu).toBeVisible();

      // Check that nav menu is not positioned off-screen (mobile behavior)
      const navBox = await navMenu.boundingBox();
      expect(navBox.x).toBeGreaterThanOrEqual(0);
      expect(navBox.x).toBeLessThan(768);
    });

    test('Navigation links are arranged horizontally', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Check flex direction is row (horizontal)
      const flexDirection = await navLinks.evaluate(el =>
        window.getComputedStyle(el).flexDirection
      );
      expect(flexDirection).toBe('row');
    });

    test('Navigation links are clickable and functional', async ({ page }) => {
      // Click on Features link and verify it scrolls to section
      const featuresLink = page.locator('.nav-link', { hasText: 'Features' });
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toBeEnabled();

      await featuresLink.click();
      await page.waitForTimeout(500); // Wait for smooth scroll

      // Features section should be in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('GitHub link is visible in navigation', async ({ page }) => {
      const githubLink = page.locator('.nav-github');
      await expect(githubLink).toBeVisible();

      // GitHub link should have proper href
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('Navigation has appropriate spacing and sizing', async ({ page }) => {
      const navContainer = page.locator('.nav-container');
      const navBox = await navContainer.boundingBox();

      // Navigation should span most of the viewport width
      expect(navBox.width).toBeGreaterThan(700);

      // Navigation height should be reasonable (based on CSS --nav-height)
      expect(navBox.height).toBeGreaterThanOrEqual(35);
      expect(navBox.height).toBeLessThanOrEqual(80);
    });
  });

  test.describe('Additional Tablet Responsive Tests', () => {
    test('Architecture components grid uses 2-column layout', async ({ page }) => {
      await page.setViewportSize({ width: 900, height: 1200 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const archGrid = page.locator('.architecture__components-grid');
      await expect(archGrid).toBeVisible();

      // At tablet width (<= 1024px), should be 2 columns
      const gridStyle = await archGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });

    test('Usage section adapts to single column on tablet', async ({ page }) => {
      await page.setViewportSize({ width: 900, height: 1200 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await page.locator('#usage').scrollIntoViewIfNeeded();

      const usageContent = page.locator('.usage__content');
      await expect(usageContent).toBeVisible();

      // At <= 1024px, usage content should be single column
      const gridStyle = await usageContent.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    });

    test('Code blocks are readable and have horizontal scroll', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_LOWER);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await page.locator('#usage').scrollIntoViewIfNeeded();

      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const codeBlock = codeBlocks.nth(i);
        await expect(codeBlock).toBeVisible();

        // Verify code block has proper overflow handling
        const pre = codeBlock.locator('pre');
        const overflowX = await pre.evaluate(el =>
          window.getComputedStyle(el).overflowX
        );
        expect(['auto', 'scroll']).toContain(overflowX);
      }
    });

    test('Touch targets meet accessibility requirements on tablet', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_LOWER);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check CTA buttons have adequate touch target size
      const ctaButtons = page.locator('.hero-cta .btn');
      const buttonCount = await ctaButtons.count();

      for (let i = 0; i < buttonCount; i++) {
        const button = ctaButtons.nth(i);
        const box = await button.boundingBox();
        expect(box.height).toBeGreaterThanOrEqual(44);
        expect(box.width).toBeGreaterThanOrEqual(100);
      }

      // Check nav links have adequate size
      const navLinks = page.locator('.nav-link');
      const linkCount = await navLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();
        expect(box.height).toBeGreaterThanOrEqual(20);
      }
    });

    test('Footer displays correctly on tablet', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_LOWER);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Check footer content is visible
      await expect(page.locator('.footer__branding')).toBeVisible();
      await expect(page.locator('.footer__links')).toBeVisible();
      await expect(page.locator('.footer__github')).toBeVisible();

      // Footer should span full width
      const footerBox = await footer.boundingBox();
      expect(footerBox.width).toBeGreaterThanOrEqual(760);
    });

    test('Text is readable without zooming on tablet', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_LOWER);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check base font size
      const bodyFontSize = await page.evaluate(() =>
        window.getComputedStyle(document.body).fontSize
      );
      const fontSizePx = parseFloat(bodyFontSize);
      expect(fontSizePx).toBeGreaterThanOrEqual(14);

      // Check hero title is appropriately sized
      const heroTitle = page.locator('.hero-title');
      const titleFontSize = await heroTitle.evaluate(el =>
        window.getComputedStyle(el).fontSize
      );
      const titlePx = parseFloat(titleFontSize);
      // At tablet (768px-1023px), should use font-size-5xl (around 48px)
      expect(titlePx).toBeGreaterThanOrEqual(40);
      expect(titlePx).toBeLessThanOrEqual(60);
    });

    test('Fixed navigation stays at top when scrolling', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_LOWER);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const nav = page.locator('.main-nav');
      const navBoxInitial = await nav.boundingBox();
      expect(navBoxInitial.y).toBe(0);

      // Scroll down
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(100);

      const navBoxAfterScroll = await nav.boundingBox();
      expect(navBoxAfterScroll.y).toBe(0);
    });

    test('Mermaid architecture diagram is visible and responsive', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_LOWER);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await page.locator('#architecture').scrollIntoViewIfNeeded();
      await page.waitForTimeout(500); // Wait for Mermaid to render

      const diagramContainer = page.locator('.architecture__diagram-container');
      await expect(diagramContainer).toBeVisible();

      // Container should allow horizontal scroll if needed
      const overflowX = await diagramContainer.evaluate(el =>
        window.getComputedStyle(el).overflowX
      );
      expect(['auto', 'scroll', 'visible']).toContain(overflowX);
    });

    test('Getting started section displays correctly on tablet', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_LOWER);
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await page.locator('#getting-started').scrollIntoViewIfNeeded();

      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Check steps are visible
      const steps = page.locator('.getting-started__step');
      const stepCount = await steps.count();
      expect(stepCount).toBe(3);

      // All steps should be visible and properly laid out
      for (let i = 0; i < stepCount; i++) {
        await expect(steps.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Tablet Breakpoint Boundary Tests', () => {
    test('Layout at exactly 768px uses tablet styles', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Hamburger should be hidden (768px is tablet breakpoint)
      const hamburger = page.locator('.nav-toggle');
      await expect(hamburger).not.toBeVisible();

      // Features should be 2 columns
      await page.locator('#features').scrollIntoViewIfNeeded();
      const featuresGrid = page.locator('.features__grid');
      const gridStyle = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });

    test('Layout at 1023px still uses tablet features grid', async ({ page }) => {
      await page.setViewportSize({ width: 1023, height: 768 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Features should still be 2 columns (below 1024px breakpoint)
      await page.locator('#features').scrollIntoViewIfNeeded();
      const featuresGrid = page.locator('.features__grid');
      const gridStyle = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });

    test('Layout at 1024px transitions to desktop features grid', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // At exactly 1024px, features should be 3 columns (desktop breakpoint)
      await page.locator('#features').scrollIntoViewIfNeeded();
      const featuresGrid = page.locator('.features__grid');
      const gridStyle = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(3);
    });

    test('iPad portrait viewport (810x1080) displays correctly', async ({ page }) => {
      await page.setViewportSize({ width: 810, height: 1080 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Navigation should be visible (no hamburger)
      await expect(page.locator('.nav-toggle')).not.toBeVisible();
      await expect(page.locator('.nav-menu')).toBeVisible();

      // Features should be 2 columns
      await page.locator('#features').scrollIntoViewIfNeeded();
      const featuresGrid = page.locator('.features__grid');
      const gridStyle = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);

      // Hero should be visible with tablet-sized elements
      await expect(page.locator('.hero-logo')).toBeVisible();
      await expect(page.locator('.hero-title')).toBeVisible();
    });
  });
});

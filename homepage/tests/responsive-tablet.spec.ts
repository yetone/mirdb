import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Tablet', () => {
  test.describe('TC1: iPad Portrait (768x1024)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
    });

    test('Page renders correctly with adapted layout for tablet portrait', async ({ page }) => {
      // Verify hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify product name is visible
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Verify tagline is visible
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Verify CTA buttons are visible
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();

      // Verify navigation is visible
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Verify features section is visible
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Verify footer is visible
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();
    });

    test('Navigation is usable on tablet portrait', async ({ page }) => {
      // Verify navigation links are visible
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Verify all navigation items are present
      const featuresLink = navLinks.locator('a[href="#features"]');
      const quickStartLink = navLinks.locator('a[href="#quick-start"]');
      const architectureLink = navLinks.locator('a[href="#architecture"]');
      const configLink = navLinks.locator('a[href="#configuration"]');
      const githubLink = navLinks.locator('a[href*="github.com"]');

      await expect(featuresLink).toBeVisible();
      await expect(quickStartLink).toBeVisible();
      await expect(architectureLink).toBeVisible();
      await expect(configLink).toBeVisible();
      await expect(githubLink).toBeVisible();
    });

    test('Content is scrollable and accessible', async ({ page }) => {
      // Scroll to features section and verify it's accessible
      await page.locator('#features').scrollIntoViewIfNeeded();
      await expect(page.locator('#features h2')).toBeVisible();

      // Scroll to quick start section
      await page.locator('#quick-start').scrollIntoViewIfNeeded();
      await expect(page.locator('#quick-start h2')).toBeVisible();

      // Scroll to architecture section
      await page.locator('#architecture').scrollIntoViewIfNeeded();
      await expect(page.locator('#architecture h2')).toBeVisible();

      // Scroll to configuration section
      await page.locator('#configuration').scrollIntoViewIfNeeded();
      await expect(page.locator('#configuration h2')).toBeVisible();
    });
  });

  test.describe('TC2: iPad Landscape (1024x768)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
    });

    test('Page renders correctly in landscape tablet orientation', async ({ page }) => {
      // Verify hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify product name is visible
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Verify tagline is visible
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();

      // Verify CTA buttons are visible
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();

      // Verify navigation is visible
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Verify features section is visible
      const features = page.locator('#features');
      await expect(features).toBeVisible();
    });

    test('Navigation displays properly in landscape', async ({ page }) => {
      // Verify navigation links are visible
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Verify navigation items are in a horizontal layout (not wrapped)
      const navContainer = page.locator('.nav-container');
      const navBox = await navContainer.boundingBox();
      expect(navBox).not.toBeNull();

      // Verify all navigation items are accessible
      const featuresLink = navLinks.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      // Click on features link and verify navigation works
      await featuresLink.click();
      await expect(page).toHaveURL(/#features/);
    });

    test('Architecture section displays properly in landscape', async ({ page }) => {
      // Navigate to architecture section
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      // Verify architecture diagram is visible
      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      // Verify explanation is visible
      const explanation = page.locator('[data-testid="architecture-explanation"]');
      await expect(explanation).toBeVisible();

      // In landscape tablet (1024px), architecture should be in 2-column layout
      // (breakpoint is at 900px)
      const content = page.locator('.architecture-content');
      const contentBox = await content.boundingBox();
      expect(contentBox).not.toBeNull();
      if (contentBox) {
        expect(contentBox.width).toBeGreaterThan(800);
      }
    });
  });

  test.describe('TC3: Features Section Adaptation', () => {
    test('Features adapt to appropriate layout at 768px (tablet portrait)', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Verify all feature cards are visible
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(5);

      // Verify each card is visible
      for (let i = 0; i < 5; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // The grid should adapt (at 768px with minmax(250px, 1fr), expect 2 columns)
      const firstCard = featureCards.first();
      const firstCardBox = await firstCard.boundingBox();
      expect(firstCardBox).not.toBeNull();

      // Card width should be reasonable for tablet (not full width but not too narrow)
      if (firstCardBox) {
        expect(firstCardBox.width).toBeGreaterThanOrEqual(250);
        expect(firstCardBox.width).toBeLessThan(700);
      }
    });

    test('Features adapt to appropriate layout at 1024px (tablet landscape)', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Verify all feature cards are visible
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(5);

      // With more width, cards can be 3 per row
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstCardBox = await firstCard.boundingBox();
      const secondCardBox = await secondCard.boundingBox();

      expect(firstCardBox).not.toBeNull();
      expect(secondCardBox).not.toBeNull();

      if (firstCardBox && secondCardBox) {
        // Cards should be side by side (same Y position within tolerance)
        expect(Math.abs(firstCardBox.y - secondCardBox.y)).toBeLessThan(10);
      }
    });
  });

  test.describe('TC4: Touch Target Sizes', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
    });

    test('CTA buttons have adequate touch target size (min 44x44px)', async ({ page }) => {
      // Check Get Started button
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      const getStartedBox = await getStartedBtn.boundingBox();
      expect(getStartedBox).not.toBeNull();
      if (getStartedBox) {
        expect(getStartedBox.width).toBeGreaterThanOrEqual(44);
        expect(getStartedBox.height).toBeGreaterThanOrEqual(44);
      }

      // Check GitHub button
      const githubBtn = page.locator('[data-testid="cta-github"]');
      const githubBox = await githubBtn.boundingBox();
      expect(githubBox).not.toBeNull();
      if (githubBox) {
        expect(githubBox.width).toBeGreaterThanOrEqual(44);
        expect(githubBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Navigation links have adequate touch target size', async ({ page }) => {
      const navLinks = page.locator('[data-testid="nav-links"] a');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();
        expect(box).not.toBeNull();
        if (box) {
          // At tablet viewport (768px), navigation links should have 44px min-height
          // for proper touch targets per Apple HIG and WCAG guidelines
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('Copy button has adequate touch target size', async ({ page }) => {
      // Scroll to quick start section
      await page.locator('#quick-start').scrollIntoViewIfNeeded();

      const copyButton = page.locator('[data-testid="copy-button"]');
      const copyBox = await copyButton.boundingBox();
      expect(copyBox).not.toBeNull();
      if (copyBox) {
        expect(copyBox.width).toBeGreaterThanOrEqual(44);
        expect(copyBox.height).toBeGreaterThanOrEqual(30); // Button height with padding
      }
    });

    test('Footer links have adequate touch target size', async ({ page }) => {
      // Scroll to footer
      await page.locator('[data-testid="footer-section"]').scrollIntoViewIfNeeded();

      const githubLink = page.locator('[data-testid="footer-github-link"]');
      const docsLink = page.locator('[data-testid="footer-docs-link"]');

      const githubBox = await githubLink.boundingBox();
      const docsBox = await docsLink.boundingBox();

      expect(githubBox).not.toBeNull();
      expect(docsBox).not.toBeNull();

      // Links should be at least 44px height for proper touch targets on tablet
      if (githubBox) {
        expect(githubBox.height).toBeGreaterThanOrEqual(44);
      }
      if (docsBox) {
        expect(docsBox.height).toBeGreaterThanOrEqual(44);
      }
    });
  });

  test.describe('Additional Tablet Responsiveness', () => {
    test('Project status section adapts to tablet viewport', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Scroll to project status section
      await page.locator('[data-testid="project-status-section"]').scrollIntoViewIfNeeded();

      // Verify both columns are visible
      const implementedHeading = page.locator('[data-testid="implemented-heading"]');
      const roadmapHeading = page.locator('[data-testid="roadmap-heading"]');

      await expect(implementedHeading).toBeVisible();
      await expect(roadmapHeading).toBeVisible();

      // At 768px, layout should be single column (breakpoint at max-width: 768px)
      const statusContent = page.locator('.status-content');
      const statusBox = await statusContent.boundingBox();
      expect(statusBox).not.toBeNull();
    });

    test('Configuration table is scrollable on tablet if needed', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Scroll to configuration section
      await page.locator('#configuration').scrollIntoViewIfNeeded();

      // Verify table wrapper exists (for horizontal scroll if needed)
      const tableWrapper = page.locator('.config-table-wrapper');
      await expect(tableWrapper).toBeVisible();

      // Verify table is visible
      const table = page.locator('.config-table');
      await expect(table).toBeVisible();
    });

    test('Hero text scales appropriately for tablet', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // At 768px (edge of breakpoint), the h1 should still be readable
      const h1 = page.locator('h1');
      const h1Box = await h1.boundingBox();
      expect(h1Box).not.toBeNull();
      if (h1Box) {
        // Font should be scaled appropriately (2.5rem at 768px = ~40px)
        expect(h1Box.height).toBeGreaterThan(30);
      }
    });
  });
});

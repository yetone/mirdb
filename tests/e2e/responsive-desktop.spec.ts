import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Desktop', () => {
  test.describe('TC1: Desktop layout at 1280px viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
    });

    test('should display full desktop layout with proper spacing', async ({ page }) => {
      // Verify the page loads correctly
      await expect(page).toHaveTitle(/MirDB/);

      // Verify hero section has full desktop styling
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify container has proper max-width constraint
      const container = hero.locator('.container');
      const containerBox = await container.boundingBox();
      expect(containerBox).not.toBeNull();
      expect(containerBox!.width).toBeLessThanOrEqual(1200);

      // Verify hero h1 has large font (desktop size)
      const heroTitle = hero.locator('h1');
      await expect(heroTitle).toBeVisible();
      const fontSize = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // Desktop should have larger font than mobile (at least 3rem = 48px)
      expect(parseFloat(fontSize)).toBeGreaterThanOrEqual(48);
    });

    test('should display CTA buttons horizontally', async ({ page }) => {
      const ctaButtons = page.locator('.cta-buttons');
      await expect(ctaButtons).toBeVisible();

      // Verify flex-direction is row (horizontal layout)
      const flexDirection = await ctaButtons.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');

      // Verify both buttons exist
      const buttons = ctaButtons.locator('.btn');
      await expect(buttons).toHaveCount(2);
    });

    test('should have proper alignment in all sections', async ({ page }) => {
      // Check features section alignment
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Check code example section
      const codeExample = page.locator('#code-example');
      await expect(codeExample).toBeVisible();

      // Check getting started section
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Check architecture section
      const architecture = page.locator('#architecture');
      await expect(architecture).toBeVisible();

      // Check footer
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();
    });
  });

  test.describe('TC2: Desktop layout at 1920px viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
    });

    test('should apply max-width constraints on large screens', async ({ page }) => {
      // Verify containers have max-width applied
      const containers = page.locator('.container');
      const count = await containers.count();

      for (let i = 0; i < count; i++) {
        const container = containers.nth(i);
        const box = await container.boundingBox();
        if (box) {
          // Max-width should be 1200px based on CSS
          expect(box.width).toBeLessThanOrEqual(1200);
        }
      }
    });

    test('should center content on large screens', async ({ page }) => {
      // Get first visible container
      const heroContainer = page.locator('.hero .container');
      const containerBox = await heroContainer.boundingBox();
      const viewportWidth = 1920;

      // Container should be centered (roughly equal margins on both sides)
      if (containerBox) {
        const leftMargin = containerBox.x;
        const rightMargin = viewportWidth - (containerBox.x + containerBox.width);
        // Margins should be roughly equal (within 50px tolerance)
        expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
      }
    });

    test('should scale layout appropriately for large screens', async ({ page }) => {
      // Verify the page maintains readability
      const heroDescription = page.locator('.hero .description');
      await expect(heroDescription).toBeVisible();

      // Description should have max-width for readability
      const descBox = await heroDescription.boundingBox();
      expect(descBox).not.toBeNull();
      expect(descBox!.width).toBeLessThanOrEqual(700);
    });
  });

  test.describe('TC3: Feature grid on desktop', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
    });

    test('should display feature cards in multi-column grid layout', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify grid display is applied
      const display = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('grid');

      // Verify we have 4 feature cards
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);
    });

    test('should have feature cards in 3-4 column layout on desktop', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get bounding boxes of first few cards to check column arrangement
      const featureCards = page.locator('.feature-card');
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      // In a multi-column layout, cards should be side by side (same Y position)
      // with different X positions
      if (firstBox && secondBox) {
        expect(secondBox.x).toBeGreaterThan(firstBox.x);
        // Y positions should be roughly equal for cards in same row
        expect(Math.abs(firstBox.y - secondBox.y)).toBeLessThan(10);
      }
    });

    test('should display all feature card content properly', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      for (let i = 0; i < count; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        // Each card should have an icon, title, and description
        const icon = card.locator('.feature-icon');
        const title = card.locator('h3');
        const description = card.locator('p');

        await expect(icon).toBeVisible();
        await expect(title).toBeVisible();
        await expect(description).toBeVisible();
      }
    });

    test('should verify feature grid has proper gap spacing', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');

      // Check gap property
      const gap = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gap;
      });

      // Gap should be defined (2rem = 32px based on CSS)
      expect(gap).not.toBe('normal');
      expect(gap).not.toBe('0px');
    });
  });

  test.describe('Additional Desktop Layout Verifications', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
    });

    test('should display getting started steps in multi-column layout', async ({ page }) => {
      const gettingStartedContent = page.locator('.getting-started-content');
      await expect(gettingStartedContent).toBeVisible();

      // Verify grid display
      const display = await gettingStartedContent.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('grid');

      // Check that steps are arranged horizontally
      const steps = page.locator('.step');
      const count = await steps.count();
      expect(count).toBeGreaterThanOrEqual(3);

      const firstStep = steps.nth(0);
      const secondStep = steps.nth(1);

      const firstBox = await firstStep.boundingBox();
      const secondBox = await secondStep.boundingBox();

      if (firstBox && secondBox) {
        // On desktop, steps should be side by side
        expect(secondBox.x).toBeGreaterThan(firstBox.x);
      }
    });

    test('should display footer with horizontal layout', async ({ page }) => {
      const footerContainer = page.locator('.footer .container');
      await expect(footerContainer).toBeVisible();

      // Footer should use flexbox for horizontal layout
      const display = await footerContainer.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('flex');

      // Footer nav should exist with horizontal links
      const footerNav = page.locator('.footer-nav');
      await expect(footerNav).toBeVisible();

      const navDisplay = await footerNav.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navDisplay).toBe('flex');
    });

    test('should have proper typography sizing for desktop', async ({ page }) => {
      // Check h2 font sizes
      const sectionH2 = page.locator('.features h2');
      await expect(sectionH2).toBeVisible();

      const h2FontSize = await sectionH2.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // Desktop h2 should be at least 2rem (32px)
      expect(parseFloat(h2FontSize)).toBeGreaterThanOrEqual(32);
    });
  });
});

import { test, expect } from '@playwright/test';

// Viewport configurations for tablet and desktop
const VIEWPORTS = {
  tablet: { width: 768, height: 1024 },   // Standard tablet (iPad portrait)
  desktop: { width: 1024, height: 768 },   // Standard desktop
  largeDesktop: { width: 1440, height: 900 }, // Large desktop
};

// Helper function to get the full file URL
const getPageUrl = () => `file://${process.cwd()}/public/index.html`;

test.describe('Responsive Design - Tablet and Desktop (NFR-3)', () => {
  test.describe('Test Case 1: View page at 768px width (tablet)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto(getPageUrl());
    });

    test('layout adapts to tablet width with appropriate spacing', async ({ page }) => {
      // No horizontal overflow
      const scrollInfo = await page.evaluate(() => {
        return {
          scrollWidth: document.documentElement.scrollWidth,
          clientWidth: document.documentElement.clientWidth,
        };
      });
      expect(scrollInfo.scrollWidth).toBeLessThanOrEqual(scrollInfo.clientWidth + 1);
    });

    test('navbar uses row layout on tablet', async ({ page }) => {
      const navbar = page.locator('.navbar');
      const flexDirection = await navbar.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    });

    test('navigation links are inline on tablet', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      const gap = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).gap;
      });
      // Should have larger gap on tablet (2rem = 32px)
      expect(parseFloat(gap)).toBeGreaterThanOrEqual(24);
    });

    test('hero section has appropriate padding', async ({ page }) => {
      const heroSection = page.locator('.hero-section');
      const padding = await heroSection.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.padding) || parseFloat(style.paddingLeft);
      });
      expect(padding).toBeGreaterThanOrEqual(32);
    });

    test('hero buttons are displayed inline', async ({ page }) => {
      const ctaGroup = page.locator('.hero-cta-group');
      const flexDirection = await ctaGroup.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    });

    test('features section has standard padding', async ({ page }) => {
      const featuresSection = page.locator('.features-section');
      const padding = await featuresSection.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.paddingLeft);
      });
      expect(padding).toBeGreaterThanOrEqual(32);
    });
  });

  test.describe('Test Case 2: View page at 1024px width (desktop)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
    });

    test('layout uses multi-column where appropriate', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      const gridTemplateColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have multiple columns (split by space for multiple column widths)
      const columnCount = gridTemplateColumns.split(' ').filter(col => parseFloat(col) > 0).length;
      expect(columnCount).toBeGreaterThanOrEqual(2);
    });

    test('navbar is horizontal with full navigation visible', async ({ page }) => {
      const navbar = page.locator('.navbar');
      const flexDirection = await navbar.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    });

    test('navigation links are displayed inline', async ({ page }) => {
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();

      // All navigation links should be visible
      for (let i = 0; i < count; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });

    test('hero content has proper max-width', async ({ page }) => {
      const heroContent = page.locator('.hero-content');
      const boundingBox = await heroContent.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(800);
    });

    test('sections have standard desktop padding', async ({ page }) => {
      const featuresSection = page.locator('.features-section');
      const padding = await featuresSection.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.paddingLeft);
      });
      expect(padding).toBeGreaterThanOrEqual(32);
    });
  });

  test.describe('Test Case 3: View page at 1440px width (large desktop)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.largeDesktop);
      await page.goto(getPageUrl());
    });

    test('content is centered with max-width constraint', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      const boundingBox = await featuresGrid.boundingBox();

      expect(boundingBox).not.toBeNull();
      // Content should have max-width constraint (1200px)
      expect(boundingBox!.width).toBeLessThanOrEqual(1200);
    });

    test('features grid is centered on large screens', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      const boundingBox = await featuresGrid.boundingBox();

      // Grid should be centered (has margin on both sides)
      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.x).toBeGreaterThan(50); // Has left margin
    });

    test('architecture explanations have max-width', async ({ page }) => {
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const explanations = page.locator('.architecture-explanations');
      const boundingBox = await explanations.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(1000);
    });

    test('code block has max-width', async ({ page }) => {
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const codeBlock = page.locator('.code-block');
      const boundingBox = await codeBlock.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(700);
    });

    test('configuration table has max-width', async ({ page }) => {
      await page.locator('#configuration').scrollIntoViewIfNeeded();

      const tableContainer = page.locator('.config-table-container');
      const boundingBox = await tableContainer.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(700);
    });

    test('footer content has max-width and is centered', async ({ page }) => {
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerContent = page.locator('.footer-content');
      const boundingBox = await footerContent.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(1200);
    });
  });

  test.describe('Test Case 4: Check feature grid on desktop', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
    });

    test('features display in grid layout (2-4 columns)', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      const gridTemplateColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Count number of columns
      const columnCount = gridTemplateColumns.split(' ').filter(col => parseFloat(col) > 0).length;
      expect(columnCount).toBeGreaterThanOrEqual(2);
      expect(columnCount).toBeLessThanOrEqual(4);
    });

    test('feature cards have equal widths in grid', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBe(4);

      // Get widths of first two cards
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      expect(firstCard).not.toBeNull();
      expect(secondCard).not.toBeNull();

      // Cards should have similar widths (within 10px tolerance)
      expect(Math.abs(firstCard!.width - secondCard!.width)).toBeLessThan(10);
    });

    test('feature cards are arranged in rows on desktop', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');

      // First two cards should be on same row (similar Y position)
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      expect(firstCard).not.toBeNull();
      expect(secondCard).not.toBeNull();

      // Same row means similar Y positions (within 10px)
      expect(Math.abs(firstCard!.y - secondCard!.y)).toBeLessThan(10);
    });

    test('feature grid gap is appropriate', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      const gap = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gap;
      });

      expect(parseFloat(gap)).toBeGreaterThanOrEqual(16);
    });
  });

  test.describe('Test Case 5: Check architecture diagram scaling', () => {
    test('diagram scales appropriately at tablet width', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto(getPageUrl());
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      const boundingBox = await diagram.boundingBox();
      expect(boundingBox).not.toBeNull();
      // Diagram should scale to fit container
      expect(boundingBox!.width).toBeGreaterThanOrEqual(400);
    });

    test('diagram scales appropriately at desktop width', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      const boundingBox = await diagram.boundingBox();
      expect(boundingBox).not.toBeNull();
      // Diagram should scale to fit container (larger on desktop)
      expect(boundingBox!.width).toBeGreaterThanOrEqual(600);
    });

    test('diagram remains legible at large desktop width', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.largeDesktop);
      await page.goto(getPageUrl());
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      // Check diagram container has max-width constraint
      const diagramContainer = page.locator('.architecture-diagram-container');
      const boundingBox = await diagramContainer.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(850);
    });

    test('diagram has proper aspect ratio', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const diagram = page.locator('[data-testid="architecture-diagram"]');
      const boundingBox = await diagram.boundingBox();

      expect(boundingBox).not.toBeNull();

      // Aspect ratio should be approximately 2:1 (800/400 = 2)
      const aspectRatio = boundingBox!.width / boundingBox!.height;
      expect(aspectRatio).toBeGreaterThanOrEqual(1.5);
      expect(aspectRatio).toBeLessThanOrEqual(2.5);
    });

    test('architecture explanations display in grid on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const explanations = page.locator('.architecture-explanations');
      const gridTemplateColumns = await explanations.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have multiple columns on desktop
      const columnCount = gridTemplateColumns.split(' ').filter(col => parseFloat(col) > 0).length;
      expect(columnCount).toBeGreaterThanOrEqual(2);
    });
  });

  test.describe('Test Case 6: Test navigation on desktop', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
    });

    test('full navigation is visible without hamburger menu', async ({ page }) => {
      const navbar = page.locator('.navbar');
      await expect(navbar).toBeVisible();

      // All navigation links should be visible
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();

      expect(count).toBeGreaterThanOrEqual(5); // Features, Architecture, Quick Start, Configuration, GitHub

      for (let i = 0; i < count; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });

    test('navigation uses horizontal layout', async ({ page }) => {
      const navbar = page.locator('.navbar');
      const flexDirection = await navbar.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    });

    test('navigation links have proper gap spacing', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      const gap = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).gap;
      });
      // Standard gap of 2rem = 32px
      expect(parseFloat(gap)).toBeGreaterThanOrEqual(24);
    });

    test('logo is aligned to the left', async ({ page }) => {
      const logo = page.locator('.logo');
      const boundingBox = await logo.boundingBox();

      expect(boundingBox).not.toBeNull();
      // Logo should be positioned near the left edge (with padding)
      expect(boundingBox!.x).toBeLessThan(100);
    });

    test('navigation links are aligned to the right', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      const navbar = page.locator('.navbar');

      const navBB = await navLinks.boundingBox();
      const navbarBB = await navbar.boundingBox();

      expect(navBB).not.toBeNull();
      expect(navbarBB).not.toBeNull();

      // Navigation should be on the right side (navbar uses space-between)
      const rightEdge = navBB!.x + navBB!.width;
      const navbarRightEdge = navbarBB!.x + navbarBB!.width;

      // Links should be close to the right edge
      expect(navbarRightEdge - rightEdge).toBeLessThan(50);
    });

    test('all section links work correctly', async ({ page }) => {
      const sectionLinks = [
        { link: 'Features', section: '#features' },
        { link: 'Architecture', section: '#architecture' },
        { link: 'Quick Start', section: '#quickstart' },
        { link: 'Configuration', section: '#configuration' },
      ];

      for (const { link, section } of sectionLinks) {
        await page.goto(getPageUrl());
        await page.locator(`.nav-links a:has-text("${link}")`).click();

        // Check that the section is scrolled into view
        const sectionElement = page.locator(section);
        await expect(sectionElement).toBeVisible();
      }
    });
  });

  test.describe('Additional desktop responsiveness checks', () => {
    test('footer displays in row layout on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerContent = page.locator('.footer-content');
      const flexDirection = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    });

    test('footer uses space-between alignment', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(getPageUrl());
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerContent = page.locator('.footer-content');
      const justifyContent = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).justifyContent;
      });
      expect(justifyContent).toBe('space-between');
    });

    test('page has no horizontal overflow at any desktop size', async ({ page }) => {
      for (const viewport of [VIEWPORTS.tablet, VIEWPORTS.desktop, VIEWPORTS.largeDesktop]) {
        await page.setViewportSize(viewport);
        await page.goto(getPageUrl());

        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });

        expect(hasHorizontalScroll).toBe(false);
      }
    });

    test('hero title uses responsive font sizing', async ({ page }) => {
      const heroTitle = page.locator('.hero-content h1');

      // Check at tablet
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto(getPageUrl());
      const tabletFontSize = await heroTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Check at large desktop
      await page.setViewportSize(VIEWPORTS.largeDesktop);
      await page.goto(getPageUrl());
      const desktopFontSize = await heroTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Font should be larger on desktop due to clamp()
      expect(desktopFontSize).toBeGreaterThanOrEqual(tabletFontSize);
    });
  });
});

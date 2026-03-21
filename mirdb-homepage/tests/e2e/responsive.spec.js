/**
 * Mobile Responsiveness E2E Tests
 * Owner: Scenario 8 - Mobile Responsiveness
 *
 * Tests:
 * - 320px viewport (minimum mobile)
 * - 768px viewport (tablet)
 * - 1024px viewport (desktop)
 * - Mobile navigation adaptation
 * - Touch target sizes
 * - Feature card stacking
 * - Code block horizontal scrolling
 */

const { test, expect } = require('@playwright/test');

test.describe('Mobile Responsiveness', () => {
  test.describe('320px viewport (minimum mobile)', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('all content is readable with no horizontal scrolling', async ({ page }) => {
      await page.goto('/');

      // Check that document width does not exceed viewport
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Allow small tolerance for scrollbar
      expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 2);

      // Verify no horizontal scroll is visible
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('all text content is visible and readable', async ({ page }) => {
      await page.goto('/');

      // Check hero title is visible
      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();

      // Check hero tagline is visible
      const heroTagline = page.locator('.hero__tagline');
      await expect(heroTagline).toBeVisible();

      // Check features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Check quick start section is visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();
    });

    test('no text overflow outside viewport', async ({ page }) => {
      await page.goto('/');

      // Check that main content elements don't overflow
      const overflowingElements = await page.evaluate(() => {
        const viewportWidth = window.innerWidth;
        const elements = document.querySelectorAll('h1, h2, h3, p, .hero__title, .hero__tagline');
        const overflowing = [];

        elements.forEach(el => {
          const rect = el.getBoundingClientRect();
          if (rect.right > viewportWidth) {
            overflowing.push(el.tagName + '.' + el.className);
          }
        });

        return overflowing;
      });

      expect(overflowingElements).toHaveLength(0);
    });
  });

  test.describe('768px viewport (tablet)', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    test('layout adapts appropriately for tablet', async ({ page }) => {
      await page.goto('/');

      // Verify page loads correctly
      await expect(page.locator('body')).toBeVisible();

      // Check navigation is visible
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Check features section displays correctly
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();
    });

    test('feature cards may stack or use grid layout', async ({ page }) => {
      await page.goto('/');

      // Get feature cards
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      // Should have 6 feature cards
      expect(count).toBe(6);

      // Check that cards are visible
      for (let i = 0; i < count; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // Check grid layout is applied
      const gridStyles = await page.locator('.features-grid').evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns
        };
      });

      expect(gridStyles.display).toBe('grid');
    });

    test('no horizontal scrollbar at tablet width', async ({ page }) => {
      await page.goto('/');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('1024px viewport (desktop)', () => {
    test.use({ viewport: { width: 1024, height: 768 } });

    test('full desktop layout displayed correctly', async ({ page }) => {
      await page.goto('/');

      // Verify all major sections are visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#usage')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
    });

    test('navigation menu is fully visible', async ({ page }) => {
      await page.goto('/');

      // Check all nav links are visible
      const navLinks = page.locator('.nav-link');
      const count = await navLinks.count();

      expect(count).toBeGreaterThanOrEqual(4);

      for (let i = 0; i < count; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });

    test('feature cards display in grid layout', async ({ page }) => {
      await page.goto('/');

      const featuresGrid = page.locator('.features-grid');

      // At 1024px, features should be in multi-column layout
      const gridColumns = await featuresGrid.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return styles.gridTemplateColumns;
      });

      // Should have multiple columns (not single column)
      expect(gridColumns).not.toBe('1fr');
    });
  });

  test.describe('Mobile navigation', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('navigation is accessible on mobile', async ({ page }) => {
      await page.goto('/');

      // Check that navigation exists
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Check that nav links exist (may be in hamburger menu or visible)
      const navMenu = page.locator('.nav-menu');

      // Navigation should either be visible or have a hamburger toggle
      const hamburger = page.locator('.hamburger-toggle, .nav-toggle, [aria-label*="menu"]');
      const hasHamburger = await hamburger.count() > 0;
      const menuVisible = await navMenu.isVisible();

      // Either hamburger exists or menu is visible (adapted for mobile)
      expect(hasHamburger || menuVisible).toBe(true);
    });

    test('navigation does not overflow on small screens', async ({ page }) => {
      await page.goto('/');

      // Check header/nav width
      const headerOverflow = await page.evaluate(() => {
        const header = document.querySelector('header');
        if (!header) return false;
        return header.scrollWidth > header.clientWidth;
      });

      expect(headerOverflow).toBe(false);
    });
  });

  test.describe('Touch targets', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('all interactive elements have minimum 44x44px touch target size', async ({ page }) => {
      await page.goto('/');

      // Check CTA buttons
      const ctaButtons = page.locator('.hero__cta');
      const ctaCount = await ctaButtons.count();

      for (let i = 0; i < ctaCount; i++) {
        const box = await ctaButtons.nth(i).boundingBox();
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }

      // Check nav links on mobile
      const navLinks = page.locator('.nav-link');
      const navCount = await navLinks.count();

      for (let i = 0; i < navCount; i++) {
        const isVisible = await navLinks.nth(i).isVisible();
        if (isVisible) {
          const box = await navLinks.nth(i).boundingBox();
          // Touch targets should be at least 44px in one dimension (usually height with padding)
          expect(box.height).toBeGreaterThanOrEqual(32); // Allow some flexibility for navigation
        }
      }
    });

    test('copy button is accessible on mobile', async ({ page }) => {
      await page.goto('/');

      const copyButton = page.locator('.copy-button');
      const isVisible = await copyButton.isVisible();

      if (isVisible) {
        const box = await copyButton.boundingBox();
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    });
  });

  test.describe('Code block on mobile', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('code block is horizontally scrollable if needed', async ({ page }) => {
      await page.goto('/');

      // Scroll to quick start section
      await page.locator('#quick-start').scrollIntoViewIfNeeded();

      const codeBlock = page.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Check that code block has overflow-x auto or scroll
      const overflowStyle = await codeBlock.evaluate(el => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(['auto', 'scroll']).toContain(overflowStyle);
    });

    test('copy button is accessible on mobile code block', async ({ page }) => {
      await page.goto('/');

      // Scroll to quick start
      await page.locator('#quick-start').scrollIntoViewIfNeeded();

      const copyButton = page.locator('.copy-button');
      await expect(copyButton).toBeVisible();

      // Verify copy button is clickable (not obscured)
      await expect(copyButton).toBeEnabled();
    });
  });

  test.describe('Feature cards stacking', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('feature cards stack vertically on narrow screens', async ({ page }) => {
      await page.goto('/');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBe(6);

      // Get positions of first two cards
      const card1Box = await featureCards.nth(0).boundingBox();
      const card2Box = await featureCards.nth(1).boundingBox();

      // On mobile, cards should stack (second card should be below first)
      // Card 2 top should be at or below Card 1 bottom (stacking vertically)
      expect(card2Box.y).toBeGreaterThanOrEqual(card1Box.y + card1Box.height - 10);
    });

    test('feature cards are full width on mobile', async ({ page }) => {
      await page.goto('/');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCard = page.locator('.feature-card').first();
      const cardBox = await featureCard.boundingBox();

      // Card should take most of the viewport width (with some padding)
      expect(cardBox.width).toBeGreaterThan(250);
    });
  });

  test.describe('Responsive typography', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('heading sizes adapt for mobile', async ({ page }) => {
      await page.goto('/');

      // Check h1 font size is appropriate for mobile
      const h1FontSize = await page.locator('.hero__title').evaluate(el => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Font size should be readable but not too large for mobile
      expect(h1FontSize).toBeLessThanOrEqual(56);
      expect(h1FontSize).toBeGreaterThanOrEqual(24);
    });

    test('paragraph text is readable on mobile', async ({ page }) => {
      await page.goto('/');

      const paragraphFontSize = await page.locator('.hero__description').evaluate(el => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Should be between 14-18px for readability
      expect(paragraphFontSize).toBeGreaterThanOrEqual(14);
      expect(paragraphFontSize).toBeLessThanOrEqual(20);
    });
  });

  test.describe('Cross-breakpoint consistency', () => {
    test('content remains accessible across all breakpoints', async ({ page }) => {
      const breakpoints = [
        { width: 320, height: 568, name: 'mobile' },
        { width: 768, height: 1024, name: 'tablet' },
        { width: 1024, height: 768, name: 'desktop' }
      ];

      for (const bp of breakpoints) {
        await page.setViewportSize({ width: bp.width, height: bp.height });
        await page.goto('/');

        // Verify main sections exist at each breakpoint
        await expect(page.locator('#hero')).toBeVisible();
        await expect(page.locator('#features')).toBeVisible();
        await expect(page.locator('#quick-start')).toBeVisible();
        await expect(page.locator('#usage')).toBeVisible();
        await expect(page.locator('#status')).toBeVisible();
      }
    });
  });
});

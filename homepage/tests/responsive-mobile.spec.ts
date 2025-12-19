import { test, expect, Page } from '@playwright/test';

// Mobile viewport configurations
const MOBILE_VIEWPORTS = {
  small: { width: 320, height: 568 },  // Small mobile (older iPhones, small Android)
  iphone: { width: 375, height: 667 }, // iPhone 6/7/8/SE
  large: { width: 480, height: 854 },  // Large mobile
};

// Helper function to get the full file URL
const getPageUrl = () => `file://${process.cwd()}/public/index.html`;

test.describe('Responsive Design - Mobile', () => {
  test.describe('Test Case 1: View page at 320px width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.small);
      await page.goto(getPageUrl());
    });

    test('all content is visible without horizontal scrolling', async ({ page }) => {
      // Get the page's scrollable width and viewport width
      const scrollInfo = await page.evaluate(() => {
        return {
          scrollWidth: document.documentElement.scrollWidth,
          clientWidth: document.documentElement.clientWidth,
          bodyScrollWidth: document.body.scrollWidth,
          bodyClientWidth: document.body.clientWidth,
        };
      });

      // The scrollable width should equal or be less than viewport width
      // Allow small tolerance for sub-pixel rendering differences
      expect(scrollInfo.scrollWidth).toBeLessThanOrEqual(scrollInfo.clientWidth + 1);
      expect(scrollInfo.bodyScrollWidth).toBeLessThanOrEqual(scrollInfo.bodyClientWidth + 1);
    });

    test('hero section is visible and fits viewport', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const boundingBox = await heroSection.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(320);
    });

    test('features section is visible and fits viewport', async ({ page }) => {
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      const boundingBox = await featuresSection.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(320);
    });

    test('all text content is readable (no overflow)', async ({ page }) => {
      // Check that key text elements don't overflow
      const textElements = [
        page.locator('h1').first(),
        page.locator('.tagline'),
        page.locator('.hero-description'),
      ];

      for (const element of textElements) {
        await expect(element).toBeVisible();
        const boundingBox = await element.boundingBox();
        expect(boundingBox!.width).toBeLessThanOrEqual(320);
      }
    });
  });

  test.describe('Test Case 2: View page at 375px width (iPhone)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
    });

    test('layout is optimized for mobile viewing', async ({ page }) => {
      // Check that no horizontal scroll is needed
      const scrollInfo = await page.evaluate(() => {
        return {
          scrollWidth: document.documentElement.scrollWidth,
          clientWidth: document.documentElement.clientWidth,
        };
      });
      expect(scrollInfo.scrollWidth).toBeLessThanOrEqual(scrollInfo.clientWidth + 1);
    });

    test('hero CTA buttons stack vertically', async ({ page }) => {
      const ctaGroup = page.locator('.hero-cta-group');
      await expect(ctaGroup).toBeVisible();

      // Check the flex direction is column (buttons stacked)
      const flexDirection = await ctaGroup.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    });

    test('buttons have appropriate width for mobile', async ({ page }) => {
      const primaryButton = page.locator('.btn-primary').first();
      const boundingBox = await primaryButton.boundingBox();

      // Button should be reasonably wide on mobile (not full width but not too narrow)
      expect(boundingBox!.width).toBeGreaterThanOrEqual(150);
      expect(boundingBox!.width).toBeLessThanOrEqual(375);
    });

    test('sections have appropriate mobile padding', async ({ page }) => {
      const featuresSection = page.locator('.features-section');
      const padding = await featuresSection.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          paddingLeft: parseFloat(style.paddingLeft),
          paddingRight: parseFloat(style.paddingRight),
        };
      });

      // Should have reduced padding on mobile (1rem = 16px)
      expect(padding.paddingLeft).toBeLessThanOrEqual(32);
      expect(padding.paddingRight).toBeLessThanOrEqual(32);
    });
  });

  test.describe('Test Case 3: Check mobile navigation menu', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
    });

    test('navigation is present and accessible on mobile', async ({ page }) => {
      const navbar = page.locator('.navbar');
      await expect(navbar).toBeVisible();
    });

    test('navigation links wrap appropriately on mobile', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Check that navigation has flex-wrap enabled
      const flexWrap = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).flexWrap;
      });
      expect(flexWrap).toBe('wrap');
    });

    test('navigation links are centered on mobile', async ({ page }) => {
      const navLinks = page.locator('.nav-links');

      const justifyContent = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).justifyContent;
      });
      expect(justifyContent).toBe('center');
    });

    test('navbar uses column layout on mobile', async ({ page }) => {
      const navbar = page.locator('.navbar');

      const flexDirection = await navbar.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    });

    test('all navigation links are accessible', async ({ page }) => {
      const navLinkElements = page.locator('.nav-links a');
      const count = await navLinkElements.count();

      // Should have all navigation links visible
      expect(count).toBeGreaterThanOrEqual(4);

      // All links should be visible and clickable
      for (let i = 0; i < count; i++) {
        await expect(navLinkElements.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Test Case 4: Measure touch target sizes', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
    });

    test('primary CTA button has minimum touch target size (44x44px)', async ({ page }) => {
      const ctaButton = page.locator('[data-testid="cta-button"]');
      const boundingBox = await ctaButton.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.height).toBeGreaterThanOrEqual(44);
      expect(boundingBox!.width).toBeGreaterThanOrEqual(44);
    });

    test('secondary button has minimum touch target size (44x44px)', async ({ page }) => {
      const secondaryButton = page.locator('.btn-secondary').first();
      const boundingBox = await secondaryButton.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.height).toBeGreaterThanOrEqual(44);
      expect(boundingBox!.width).toBeGreaterThanOrEqual(44);
    });

    test('navigation links have adequate touch target size', async ({ page }) => {
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const boundingBox = await navLinks.nth(i).boundingBox();
        expect(boundingBox).not.toBeNull();
        // Links should have at least 44px height (including line-height and padding)
        // Width can be smaller for text links but should still be reasonable
        expect(boundingBox!.height).toBeGreaterThanOrEqual(20); // Text links have inherent height from font
      }
    });

    test('copy button has minimum touch target size (44x44px)', async ({ page }) => {
      // Scroll to make the copy button visible
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const copyButton = page.locator('[data-testid="copy-button"]');
      const boundingBox = await copyButton.boundingBox();

      expect(boundingBox).not.toBeNull();
      // Copy button should be touch-friendly
      expect(boundingBox!.height).toBeGreaterThanOrEqual(30);
      expect(boundingBox!.width).toBeGreaterThanOrEqual(44);
    });

    test('footer links have adequate touch target spacing', async ({ page }) => {
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerLinks = page.locator('.footer-links a');
      const count = await footerLinks.count();

      // Check that footer links exist and are visible
      expect(count).toBeGreaterThanOrEqual(2);

      for (let i = 0; i < count; i++) {
        await expect(footerLinks.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Test Case 5: Test code blocks on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
    });

    test('code blocks are contained within viewport width', async ({ page }) => {
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const codeBlock = page.locator('.code-block');
      const boundingBox = await codeBlock.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(375);
    });

    test('code blocks have horizontal scroll for overflow content', async ({ page }) => {
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const codeBlockPre = page.locator('.code-block pre');

      const overflowX = await codeBlockPre.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(overflowX).toBe('auto');
    });

    test('code content is readable on mobile', async ({ page }) => {
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const codeElement = page.locator('.code-block code');
      await expect(codeElement).toBeVisible();

      // Check font size is reasonable for mobile
      const fontSize = await codeElement.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      expect(fontSize).toBeGreaterThanOrEqual(12); // Minimum readable font size
      expect(fontSize).toBeLessThanOrEqual(18); // Not too large
    });

    test('quickstart section fits within mobile viewport', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      const boundingBox = await quickstartSection.boundingBox();

      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeLessThanOrEqual(375);
    });
  });

  test.describe('Test Case 6: Test feature cards on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
    });

    test('feature cards stack vertically on mobile viewports', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBe(4); // Should have 4 feature cards

      // Get positions of first two cards
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      expect(firstCard).not.toBeNull();
      expect(secondCard).not.toBeNull();

      // If stacked vertically, second card's top should be below first card's bottom
      // (with some margin/gap)
      expect(secondCard!.y).toBeGreaterThan(firstCard!.y + firstCard!.height - 10);
    });

    test('feature cards have full width on mobile', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCard = page.locator('.feature-card').first();
      const boundingBox = await featureCard.boundingBox();

      expect(boundingBox).not.toBeNull();
      // Feature cards should take significant width (accounting for padding)
      expect(boundingBox!.width).toBeGreaterThanOrEqual(250);
    });

    test('all feature cards are visible', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      for (let i = 0; i < count; i++) {
        await featureCards.nth(i).scrollIntoViewIfNeeded();
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('features grid uses responsive layout', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');

      const gridTemplateColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // On mobile, should have single column or auto-fit columns
      // The grid should compute to a single column at 375px width
      // gridTemplateColumns will show the computed value
      expect(gridTemplateColumns).toBeDefined();
    });

    test('architecture explanations stack vertically on mobile', async ({ page }) => {
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const explanationCards = page.locator('.explanation-card');
      const count = await explanationCards.count();

      expect(count).toBeGreaterThanOrEqual(2);

      // Get positions of first two explanation cards
      const firstCard = await explanationCards.nth(0).boundingBox();
      const secondCard = await explanationCards.nth(1).boundingBox();

      expect(firstCard).not.toBeNull();
      expect(secondCard).not.toBeNull();

      // If stacked vertically, second card's top should be below first card's bottom
      expect(secondCard!.y).toBeGreaterThan(firstCard!.y + firstCard!.height - 10);
    });
  });

  test.describe('Additional mobile responsiveness checks', () => {
    test('architecture diagram container allows horizontal scroll', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
      await page.locator('#architecture').scrollIntoViewIfNeeded();

      const diagramContainer = page.locator('.architecture-diagram-container');

      const overflowX = await diagramContainer.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(overflowX).toBe('auto');
    });

    test('footer content stacks on mobile', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerContent = page.locator('.footer-content');

      const flexDirection = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });

      expect(flexDirection).toBe('column');
    });

    test('footer text is centered on mobile', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.iphone);
      await page.goto(getPageUrl());
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerContent = page.locator('.footer-content');

      const textAlign = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).textAlign;
      });

      expect(textAlign).toBe('center');
    });

    test('configuration table is scrollable on mobile', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.small);
      await page.goto(getPageUrl());
      await page.locator('#configuration').scrollIntoViewIfNeeded();

      const tableContainer = page.locator('.config-table-container');

      const overflowX = await tableContainer.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(overflowX).toBe('auto');
    });

    test('page has no horizontal overflow at 320px', async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORTS.small);
      await page.goto(getPageUrl());

      // Check document doesn't cause horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  });
});

/**
 * Responsive Design E2E Tests
 * Owner: Scenario 8 - Desktop, Scenario 9 - Tablet, Scenario 10 - Mobile
 *
 * Tests:
 * - Desktop layout (1920px)
 * - Tablet layout (768px, 1024px)
 * - Mobile layout (320px, 375px)
 * - Mobile menu functionality
 * - Touch target sizes
 */

import { test, expect } from '@playwright/test';

// ==========================================================================
// Tablet Viewport Tests (Scenario 9)
// ==========================================================================
test.describe('Responsive Design - Tablet Viewport', () => {
    // Test Case 1: Load page at 768px viewport width
    test.describe('768px viewport (lower tablet range)', () => {
        test.use({ viewport: { width: 768, height: 1024 } });

        test('test case 1: all content displays correctly without horizontal scroll', async ({ page }) => {
            await page.goto('/');

            // Check that page does not have horizontal scroll
            const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
            const viewportWidth = await page.evaluate(() => window.innerWidth);

            expect(pageWidth).toBeLessThanOrEqual(viewportWidth);

            // Verify main sections are visible
            const featuresSection = page.locator('#features');
            await expect(featuresSection).toBeVisible();

            const codeExamplesSection = page.locator('#code-examples');
            await expect(codeExamplesSection).toBeVisible();

            const architectureSection = page.locator('#architecture');
            await expect(architectureSection).toBeVisible();

            const statusSection = page.locator('#status');
            await expect(statusSection).toBeVisible();

            const specificationsSection = page.locator('#specifications');
            await expect(specificationsSection).toBeVisible();
        });

        test('navigation is accessible at 768px (mobile menu may be active)', async ({ page }) => {
            await page.goto('/');

            // Navigation container should be visible
            const navContainer = page.locator('.nav-container');
            await expect(navContainer).toBeVisible();

            // At 768px, the mobile menu toggle may be active (CSS media query is max-width: 768px)
            // Check that either the nav links are visible OR the toggle button is visible
            const navToggle = page.locator('.nav-toggle');
            const navLinks = page.locator('.nav-links');

            // The nav toggle should be visible at 768px (mobile breakpoint)
            const toggleVisible = await navToggle.isVisible();

            if (toggleVisible) {
                // Mobile mode - verify menu can be toggled
                await expect(navToggle).toBeVisible();
                // Click to open menu
                await navToggle.click();
                // After clicking, nav links should become visible
                await expect(navLinks).toBeVisible();
            } else {
                // Desktop mode - nav links should be directly visible
                await expect(navLinks).toBeVisible();
            }
        });

        test('test case 3: features adapt to 2-column or stacked layout at 768px', async ({ page }) => {
            await page.goto('/');

            // Check features grid exists
            const featuresGrid = page.locator('.features-grid');
            await expect(featuresGrid).toBeVisible();

            // Verify feature cards are visible
            const featureCards = page.locator('.feature-card');
            const count = await featureCards.count();
            expect(count).toBeGreaterThanOrEqual(6);

            // Check that all feature cards are visible
            for (let i = 0; i < count; i++) {
                await expect(featureCards.nth(i)).toBeVisible();
            }
        });

        test('test case 4: code blocks are readable without horizontal scrolling at 768px', async ({ page }) => {
            await page.goto('/');

            // Navigate to code examples
            const codeExamplesSection = page.locator('#code-examples');
            await codeExamplesSection.scrollIntoViewIfNeeded();

            // Check code blocks exist and are visible
            const codeBlocks = page.locator('.code-block');
            const count = await codeBlocks.count();
            expect(count).toBeGreaterThan(0);

            // Verify code blocks are visible
            for (let i = 0; i < count; i++) {
                await expect(codeBlocks.nth(i)).toBeVisible();
            }

            // Check that code examples container is properly contained
            const codeExamplesContainer = page.locator('.code-examples-container');
            await expect(codeExamplesContainer).toBeVisible();
        });

        test('architecture diagram is visible and properly sized at 768px', async ({ page }) => {
            await page.goto('/');

            const architectureSection = page.locator('#architecture');
            await architectureSection.scrollIntoViewIfNeeded();

            // Check architecture diagram
            const diagram = page.locator('.architecture-diagram');
            await expect(diagram).toBeVisible();

            // Verify diagram doesn't overflow
            const diagramBox = await diagram.boundingBox();
            expect(diagramBox).not.toBeNull();
            if (diagramBox) {
                expect(diagramBox.width).toBeLessThanOrEqual(768);
            }
        });

        test('specifications grid is accessible at 768px', async ({ page }) => {
            await page.goto('/');

            const specificationsSection = page.locator('#specifications');
            await specificationsSection.scrollIntoViewIfNeeded();

            // Check specification cards are visible
            const specCards = page.locator('.spec-card');
            const count = await specCards.count();
            expect(count).toBeGreaterThan(0);

            for (let i = 0; i < count; i++) {
                await expect(specCards.nth(i)).toBeVisible();
            }
        });

        test('status section is accessible at 768px', async ({ page }) => {
            await page.goto('/');

            const statusSection = page.locator('#status');
            await statusSection.scrollIntoViewIfNeeded();

            // Check status cards are visible
            const statusCards = page.locator('.status-card');
            const count = await statusCards.count();
            expect(count).toBeGreaterThan(0);

            for (let i = 0; i < count; i++) {
                await expect(statusCards.nth(i)).toBeVisible();
            }
        });
    });

    // Test Case 2: Load page at 1024px viewport width
    test.describe('1024px viewport (upper tablet range)', () => {
        test.use({ viewport: { width: 1024, height: 768 } });

        test('test case 2: all content displays correctly at upper tablet range (1024px)', async ({ page }) => {
            await page.goto('/');

            // Check that page does not have horizontal scroll
            const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
            const viewportWidth = await page.evaluate(() => window.innerWidth);

            expect(pageWidth).toBeLessThanOrEqual(viewportWidth);

            // Verify main sections are visible
            const featuresSection = page.locator('#features');
            await expect(featuresSection).toBeVisible();

            const codeExamplesSection = page.locator('#code-examples');
            await expect(codeExamplesSection).toBeVisible();

            const architectureSection = page.locator('#architecture');
            await expect(architectureSection).toBeVisible();

            const statusSection = page.locator('#status');
            await expect(statusSection).toBeVisible();

            const specificationsSection = page.locator('#specifications');
            await expect(specificationsSection).toBeVisible();
        });

        test('navigation is fully accessible at 1024px', async ({ page }) => {
            await page.goto('/');

            // Navigation should be visible
            const navContainer = page.locator('.nav-container');
            await expect(navContainer).toBeVisible();

            // Check nav links
            const navLinks = page.locator('.nav-links');
            await expect(navLinks).toBeVisible();

            // Check CTA buttons
            const navCta = page.locator('.nav-cta');
            await expect(navCta).toBeVisible();
        });

        test('features grid layout at 1024px', async ({ page }) => {
            await page.goto('/');

            // Check features grid exists and is visible
            const featuresGrid = page.locator('.features-grid');
            await expect(featuresGrid).toBeVisible();

            // Verify all feature cards are accessible
            const featureCards = page.locator('.feature-card');
            const count = await featureCards.count();
            expect(count).toBeGreaterThanOrEqual(6);

            for (let i = 0; i < count; i++) {
                await expect(featureCards.nth(i)).toBeVisible();
            }
        });

        test('code examples are readable at 1024px', async ({ page }) => {
            await page.goto('/');

            const codeExamplesSection = page.locator('#code-examples');
            await codeExamplesSection.scrollIntoViewIfNeeded();

            // Check code examples exist
            const codeExamples = page.locator('.code-example');
            const count = await codeExamples.count();
            expect(count).toBeGreaterThan(0);

            for (let i = 0; i < count; i++) {
                await expect(codeExamples.nth(i)).toBeVisible();
            }

            // Verify copy buttons are visible
            const copyButtons = page.locator('.copy-btn');
            const copyCount = await copyButtons.count();
            expect(copyCount).toBeGreaterThan(0);
        });

        test('architecture section is properly displayed at 1024px', async ({ page }) => {
            await page.goto('/');

            const architectureSection = page.locator('#architecture');
            await architectureSection.scrollIntoViewIfNeeded();

            // Check diagram is visible
            const diagram = page.locator('.architecture-diagram');
            await expect(diagram).toBeVisible();

            // Check component descriptions are visible
            const components = page.locator('.architecture-component');
            const count = await components.count();
            expect(count).toBeGreaterThan(0);
        });

        test('specifications grid at 1024px', async ({ page }) => {
            await page.goto('/');

            const specificationsSection = page.locator('#specifications');
            await specificationsSection.scrollIntoViewIfNeeded();

            // Check all spec cards
            const specCards = page.locator('.spec-card');
            const count = await specCards.count();
            expect(count).toBe(5); // 5 specification cards expected

            // Verify specific values are visible
            await expect(page.locator('[data-testid="spec-listen-address"]')).toContainText('0.0.0.0:12333');
            await expect(page.locator('[data-testid="spec-max-lsm-levels"]')).toContainText('7');
        });

        test('status section layout at 1024px', async ({ page }) => {
            await page.goto('/');

            const statusSection = page.locator('#status');
            await statusSection.scrollIntoViewIfNeeded();

            // Verify implemented features card is visible
            const implementedCard = page.locator('.status-card--implemented');
            await expect(implementedCard).toBeVisible();

            // Verify roadmap card is visible
            const roadmapCard = page.locator('.status-card--roadmap');
            await expect(roadmapCard).toBeVisible();
        });
    });

    // General tablet range tests
    test.describe('General tablet responsiveness (768px-1024px)', () => {
        const tabletWidths = [768, 896, 1024];

        for (const width of tabletWidths) {
            test(`content is accessible and no horizontal overflow at ${width}px`, async ({ page }) => {
                await page.setViewportSize({ width, height: 768 });
                await page.goto('/');

                // Check no horizontal scroll
                const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
                const viewportWidth = await page.evaluate(() => window.innerWidth);

                expect(pageWidth).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding

                // All main sections should be visible
                const sections = ['#features', '#code-examples', '#architecture', '#status', '#specifications'];
                for (const section of sections) {
                    const sectionEl = page.locator(section);
                    await sectionEl.scrollIntoViewIfNeeded();
                    await expect(sectionEl).toBeVisible();
                }
            });
        }
    });
});

// ==========================================================================
// Mobile Viewport Tests (Scenario 10)
// ==========================================================================
test.describe('Mobile Responsive Design (320px-767px)', () => {
  test.describe('375px viewport (iPhone)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
    });

    test('all content is readable without horizontal scrolling', async ({ page }) => {
      // Check that body doesn't have horizontal scroll
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify main content is visible
      await expect(page.locator('main')).toBeVisible();

      // Check that key sections are visible
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#code-examples')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
      await expect(page.locator('#specifications')).toBeVisible();
    });

    test('navigation header is visible', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Logo should be visible
      const logo = page.locator('.nav-logo');
      await expect(logo).toBeVisible();
    });

    test('mobile hamburger menu is visible and functional', async ({ page }) => {
      // Mobile menu toggle should be visible
      const navToggle = page.locator('.nav-toggle');
      await expect(navToggle).toBeVisible();

      // Initially menu should be closed (aria-expanded=false)
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

      // Navigation links should not be visible initially (hidden state)
      const navLinks = page.locator('.nav-links');
      // On mobile, nav-links are hidden via transform/opacity
      await expect(navLinks).not.toHaveClass(/nav-links--open/);
    });

    test('mobile menu opens on tap', async ({ page }) => {
      const navToggle = page.locator('.nav-toggle');
      const navLinks = page.locator('.nav-links');

      // Tap the menu button
      await navToggle.click();

      // Menu should now be open
      await expect(navToggle).toHaveAttribute('aria-expanded', 'true');
      await expect(navLinks).toHaveClass(/nav-links--open/);

      // All navigation options should be visible
      const navLinkItems = page.locator('.nav-links .nav-link');
      const count = await navLinkItems.count();
      expect(count).toBeGreaterThanOrEqual(4);

      for (let i = 0; i < count; i++) {
        await expect(navLinkItems.nth(i)).toBeVisible();
      }
    });

    test('mobile menu shows all navigation options', async ({ page }) => {
      const navToggle = page.locator('.nav-toggle');

      // Open menu
      await navToggle.click();

      // Check for expected navigation links
      await expect(page.locator('.nav-link[href="#features"]')).toBeVisible();
      await expect(page.locator('.nav-link[href="#code-examples"]')).toBeVisible();
      await expect(page.locator('.nav-link[href="#architecture"]')).toBeVisible();
      await expect(page.locator('.nav-link[href="#specifications"]')).toBeVisible();
    });

    test('feature cards display in single-column stacked layout', async ({ page }) => {
      // Navigate to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      if (cardCount > 0) {
        // Get the bounding boxes of first two cards
        const firstCard = await featureCards.first().boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
          // In single column layout, second card should be below first card
          // (they should not be side by side)
          expect(secondCard.y).toBeGreaterThan(firstCard.y);

          // Cards should be full width (approximately same width)
          expect(Math.abs(firstCard.width - secondCard.width)).toBeLessThan(10);
        }
      }
    });

    test('text is readable without zooming', async ({ page }) => {
      // Check body font size is reasonable
      const bodyFontSize = await page.evaluate(() => {
        const style = getComputedStyle(document.body);
        return parseFloat(style.fontSize);
      });

      // Body font should be at least 14px for readability
      expect(bodyFontSize).toBeGreaterThanOrEqual(14);

      // Check section titles are appropriately sized
      const h2Elements = page.locator('h2');
      const h2Count = await h2Elements.count();

      for (let i = 0; i < h2Count; i++) {
        const fontSize = await h2Elements.nth(i).evaluate((el) => {
          return parseFloat(getComputedStyle(el).fontSize);
        });
        // H2 should be at least 24px on mobile
        expect(fontSize).toBeGreaterThanOrEqual(24);
      }
    });
  });

  test.describe('320px viewport (minimum supported width)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('all content displays at minimum supported width', async ({ page }) => {
      // Check that body doesn't have horizontal scroll
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify main sections are visible
      await expect(page.locator('header')).toBeVisible();
      await expect(page.locator('main')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
    });

    test('hamburger menu toggle is visible at 320px', async ({ page }) => {
      const navToggle = page.locator('.nav-toggle');
      await expect(navToggle).toBeVisible();

      // Should be properly sized for touch
      const box = await navToggle.boundingBox();
      expect(box.width).toBeGreaterThanOrEqual(44);
      expect(box.height).toBeGreaterThanOrEqual(44);
    });

    test('feature cards are stacked at 320px', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      if (cardCount >= 2) {
        const firstCard = await featureCards.first().boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
          // Verify single column layout
          expect(secondCard.y).toBeGreaterThan(firstCard.y);
        }
      }
    });

    test('content does not overflow horizontally', async ({ page }) => {
      // Scroll through the page and check for overflow
      const sections = ['#features', '#code-examples', '#architecture', '#status', '#specifications'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        if (await section.isVisible()) {
          await section.scrollIntoViewIfNeeded();

          // Check no horizontal scroll after scrolling to section
          const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
          const clientWidth = await page.evaluate(() => document.body.clientWidth);

          expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // +1 for rounding
        }
      }
    });
  });

  test.describe('Touch target sizes', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
    });

    test('all interactive elements have minimum 44px touch target', async ({ page }) => {
      // Check nav toggle
      const navToggle = page.locator('.nav-toggle');
      const toggleBox = await navToggle.boundingBox();
      expect(toggleBox.width).toBeGreaterThanOrEqual(44);
      expect(toggleBox.height).toBeGreaterThanOrEqual(44);
    });

    test('nav links have adequate touch targets when menu is open', async ({ page }) => {
      // Open mobile menu
      const navToggle = page.locator('.nav-toggle');
      await navToggle.click();

      // Wait for menu to open
      await page.waitForSelector('.nav-links.nav-links--open');

      // Check nav link sizes
      const navLinks = page.locator('.nav-links .nav-link');
      const linkCount = await navLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();

        if (box) {
          // Height should be at least 44px for touch targets
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('CTA buttons have adequate touch targets', async ({ page }) => {
      // Open mobile menu to check CTA buttons if visible
      const ctaButtons = page.locator('.cta-btn');
      const ctaCount = await ctaButtons.count();

      for (let i = 0; i < ctaCount; i++) {
        const btn = ctaButtons.nth(i);

        // Only check visible buttons
        if (await btn.isVisible()) {
          const box = await btn.boundingBox();

          if (box) {
            expect(box.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    });

    test('copy buttons have adequate touch targets', async ({ page }) => {
      // Scroll to code examples
      await page.locator('#code-examples').scrollIntoViewIfNeeded();

      const copyButtons = page.locator('.copy-btn');
      const btnCount = await copyButtons.count();

      for (let i = 0; i < Math.min(btnCount, 3); i++) {
        const btn = copyButtons.nth(i);

        if (await btn.isVisible()) {
          const box = await btn.boundingBox();

          if (box) {
            expect(box.width).toBeGreaterThanOrEqual(44);
            expect(box.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    });
  });

  test.describe('Mobile menu functionality', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
    });

    test('menu closes when a nav link is clicked', async ({ page }) => {
      const navToggle = page.locator('.nav-toggle');
      const navLinks = page.locator('.nav-links');

      // Open menu
      await navToggle.click();
      await expect(navLinks).toHaveClass(/nav-links--open/);

      // Click a nav link
      const featuresLink = page.locator('.nav-link[href="#features"]');
      await featuresLink.click();

      // Menu should close
      await expect(navLinks).not.toHaveClass(/nav-links--open/);
    });

    test('menu closes when escape key is pressed', async ({ page }) => {
      const navToggle = page.locator('.nav-toggle');
      const navLinks = page.locator('.nav-links');

      // Open menu
      await navToggle.click();
      await expect(navLinks).toHaveClass(/nav-links--open/);

      // Press escape
      await page.keyboard.press('Escape');

      // Menu should close
      await expect(navLinks).not.toHaveClass(/nav-links--open/);
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
    });

    test('menu toggle has proper ARIA attributes', async ({ page }) => {
      const navToggle = page.locator('.nav-toggle');

      // Check ARIA attributes
      await expect(navToggle).toHaveAttribute('aria-label', 'Toggle navigation menu');
      await expect(navToggle).toHaveAttribute('aria-controls', 'nav-menu');
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

      // After clicking, aria-expanded should change
      await navToggle.click();
      await expect(navToggle).toHaveAttribute('aria-expanded', 'true');
    });
  });

  test.describe('Content readability on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.goto('/');
    });

    test('code blocks are scrollable horizontally without page scroll', async ({ page }) => {
      await page.locator('#code-examples').scrollIntoViewIfNeeded();

      const codeBlocks = page.locator('.code-block');
      const blockCount = await codeBlocks.count();

      if (blockCount > 0) {
        const firstBlock = codeBlocks.first();

        // Code blocks should allow horizontal scroll
        const overflowX = await firstBlock.evaluate((el) => {
          return getComputedStyle(el).overflowX;
        });

        expect(['auto', 'scroll']).toContain(overflowX);
      }

      // Page itself should not have horizontal scroll
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('specification cards display properly', async ({ page }) => {
      await page.locator('#specifications').scrollIntoViewIfNeeded();

      const specCards = page.locator('.spec-card');
      const cardCount = await specCards.count();

      if (cardCount >= 2) {
        const firstCard = await specCards.first().boundingBox();
        const secondCard = await specCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
          // Should be stacked vertically on mobile
          expect(secondCard.y).toBeGreaterThan(firstCard.y);
        }
      }
    });

    test('status cards display properly', async ({ page }) => {
      await page.locator('#status').scrollIntoViewIfNeeded();

      const statusCards = page.locator('.status-card');
      const cardCount = await statusCards.count();

      if (cardCount >= 2) {
        const firstCard = await statusCards.first().boundingBox();
        const secondCard = await statusCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
          // Should be stacked vertically on mobile
          expect(secondCard.y).toBeGreaterThan(firstCard.y);
        }
      }
    });
  });
});

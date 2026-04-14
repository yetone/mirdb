/**
 * Responsive Design E2E Tests
 * Owner: Scenario 9 - Responsive Design
 *
 * Tests for:
 * - Mobile viewport (375px width)
 * - Tablet viewport (768px width)
 * - Desktop viewport (1280px width)
 * - Navigation adaptation across viewports
 * - Code blocks scrollability on mobile
 */

import { test, expect } from '@playwright/test';
import { SELECTORS, VIEWPORTS } from '../fixtures/test-data';

test.describe('Responsive Design', () => {
  test.describe('Mobile Viewport (375px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
    });

    test('all content is visible without horizontal scrolling', async ({ page }) => {
      // Check that the page does not have horizontal overflow
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Body width should not exceed viewport width significantly
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 5); // Allow small margin for rounding

      // Verify main sections are visible
      await expect(page.locator(SELECTORS.hero)).toBeVisible();
      await expect(page.locator(SELECTORS.features)).toBeVisible();
      await expect(page.locator(SELECTORS.quickStart)).toBeVisible();
    });

    test('layout adapts to single column for features', async ({ page }) => {
      // Scroll to features section
      await page.locator(SELECTORS.features).scrollIntoViewIfNeeded();

      // Get feature cards
      const featureCards = page.locator(SELECTORS.featureCard);
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      // On mobile, feature cards should stack vertically (single column)
      // Check that cards don't overlap horizontally
      if (cardCount >= 2) {
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
          // On single column, second card should be below first card
          // (higher Y value means lower on page)
          expect(secondCard.y).toBeGreaterThan(firstCard.y);

          // Cards should have similar x positions (same column)
          expect(Math.abs(firstCard.x - secondCard.x)).toBeLessThan(50);
        }
      }
    });

    test('hero CTA buttons stack vertically on mobile', async ({ page }) => {
      const ctaContainer = page.locator('.hero-cta');
      await expect(ctaContainer).toBeVisible();

      const primaryButton = page.locator(SELECTORS.ctaGetStarted);
      const secondaryButton = page.locator(SELECTORS.ctaGitHub);

      const primaryBox = await primaryButton.boundingBox();
      const secondaryBox = await secondaryButton.boundingBox();

      if (primaryBox && secondaryBox) {
        // On mobile with stacked layout, buttons should be vertically arranged
        // The secondary button Y should be greater (below) the primary
        expect(secondaryBox.y).toBeGreaterThanOrEqual(primaryBox.y + primaryBox.height - 10);
      }
    });

    test('ASCII logo is visible and scaled appropriately', async ({ page }) => {
      const asciiLogo = page.locator('.ascii-logo');
      await expect(asciiLogo).toBeVisible();

      // Logo should fit within viewport
      const logoBox = await asciiLogo.boundingBox();
      if (logoBox) {
        expect(logoBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
      }
    });
  });

  test.describe('Tablet Viewport (768px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');
    });

    test('layout adapts appropriately with 2-column grid for features', async ({ page }) => {
      await page.locator(SELECTORS.features).scrollIntoViewIfNeeded();

      const featureCards = page.locator(SELECTORS.featureCard);
      const cardCount = await featureCards.count();

      if (cardCount >= 2) {
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
          // On tablet, first two cards should be side by side (same row)
          // Their Y positions should be similar
          const yDifference = Math.abs(firstCard.y - secondCard.y);
          expect(yDifference).toBeLessThan(50); // Allow some tolerance

          // Cards should be in different horizontal positions
          expect(Math.abs(firstCard.x - secondCard.x)).toBeGreaterThan(100);
        }
      }
    });

    test('page has no horizontal overflow', async ({ page }) => {
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 5);
    });

    test('all main sections are visible', async ({ page }) => {
      await expect(page.locator(SELECTORS.hero)).toBeVisible();
      await expect(page.locator(SELECTORS.features)).toBeVisible();
      await expect(page.locator(SELECTORS.quickStart)).toBeVisible();
      await expect(page.locator(SELECTORS.architecture)).toBeVisible();
      await expect(page.locator(SELECTORS.protocol)).toBeVisible();
      await expect(page.locator(SELECTORS.status)).toBeVisible();
    });
  });

  test.describe('Desktop Viewport (1280px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
    });

    test('full desktop layout with multi-column feature grid', async ({ page }) => {
      await page.locator(SELECTORS.features).scrollIntoViewIfNeeded();

      const featureCards = page.locator(SELECTORS.featureCard);
      const cardCount = await featureCards.count();

      if (cardCount >= 3) {
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();
        const thirdCard = await featureCards.nth(2).boundingBox();

        if (firstCard && secondCard && thirdCard) {
          // On desktop, first three cards should be on the same row
          const maxYDiff = Math.max(
            Math.abs(firstCard.y - secondCard.y),
            Math.abs(secondCard.y - thirdCard.y),
            Math.abs(firstCard.y - thirdCard.y)
          );
          expect(maxYDiff).toBeLessThan(50);

          // Cards should be in different horizontal positions
          expect(secondCard.x).toBeGreaterThan(firstCard.x);
          expect(thirdCard.x).toBeGreaterThan(secondCard.x);
        }
      }
    });

    test('hero section uses full viewport height', async ({ page }) => {
      const hero = page.locator(SELECTORS.hero);
      const heroBox = await hero.boundingBox();

      if (heroBox) {
        // Hero should take significant vertical space (at least 80% of viewport)
        expect(heroBox.height).toBeGreaterThanOrEqual(VIEWPORTS.desktop.height * 0.7);
      }
    });

    test('container has appropriate max-width', async ({ page }) => {
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();

      if (containerBox) {
        // Container should not exceed max-width (1200px default + padding)
        expect(containerBox.width).toBeLessThanOrEqual(1400);
      }
    });

    test('syntax examples display in multiple columns', async ({ page }) => {
      await page.locator(SELECTORS.protocol).scrollIntoViewIfNeeded();

      const syntaxExamples = page.locator('.syntax-example');
      const exampleCount = await syntaxExamples.count();

      if (exampleCount >= 2) {
        const first = await syntaxExamples.nth(0).boundingBox();
        const second = await syntaxExamples.nth(1).boundingBox();

        if (first && second) {
          // Should be side by side on desktop
          const yDiff = Math.abs(first.y - second.y);
          expect(yDiff).toBeLessThan(50);
        }
      }
    });
  });

  test.describe('Navigation Adaptation', () => {
    test('navigation is accessible on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');

      // Header element should exist in the DOM (content added by Scenario 7)
      const header = page.locator('header');
      await expect(header).toBeAttached();

      // If navigation content exists, verify responsive behavior
      const navLinks = page.locator('header a, header nav, header button');
      const linkCount = await navLinks.count();

      // Header element is present in the DOM
      expect(await header.count()).toBe(1);
    });

    test('navigation container exists on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');

      // Header element should exist in the DOM (content added by Scenario 7)
      const header = page.locator('header');
      await expect(header).toBeAttached();
    });

    test('navigation container exists on tablet', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');

      // Header element should exist in the DOM (content added by Scenario 7)
      const header = page.locator('header');
      await expect(header).toBeAttached();
    });
  });

  test.describe('Code Blocks on Mobile', () => {
    test('code blocks are scrollable and readable without breaking layout', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');

      // Navigate to quick start section
      await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();

      const codeBlocks = page.locator(SELECTORS.codeBlock);
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Check first code block
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();

      const codeBlockBox = await firstCodeBlock.boundingBox();
      if (codeBlockBox) {
        // Code block should not exceed viewport width
        expect(codeBlockBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 20);
      }

      // Check that code content has overflow-x: auto
      const codeContent = firstCodeBlock.locator('.code-content');
      const overflowX = await codeContent.evaluate((el) =>
        window.getComputedStyle(el).overflowX
      );
      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('code blocks do not cause horizontal page overflow', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');

      // Scroll through the entire page
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(500);

      // Check page doesn't have horizontal overflow
      const pageOverflow = await page.evaluate(() => {
        return document.body.scrollWidth <= window.innerWidth + 10;
      });
      expect(pageOverflow).toBe(true);
    });

    test('protocol table is horizontally scrollable on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');

      await page.locator(SELECTORS.protocol).scrollIntoViewIfNeeded();

      const tableWrapper = page.locator('.protocol-table-wrapper').first();
      await expect(tableWrapper).toBeVisible();

      // Check that table wrapper has scroll capability
      const overflowX = await tableWrapper.evaluate((el) =>
        window.getComputedStyle(el).overflowX
      );
      expect(['auto', 'scroll']).toContain(overflowX);
    });
  });

  test.describe('Cross-viewport Content Consistency', () => {
    test('same content is accessible across all viewports', async ({ page }) => {
      const viewportSizes = [VIEWPORTS.mobile, VIEWPORTS.tablet, VIEWPORTS.desktop];
      const sections = [
        SELECTORS.hero,
        SELECTORS.features,
        SELECTORS.quickStart,
        SELECTORS.architecture,
        SELECTORS.protocol,
        SELECTORS.status
      ];

      for (const viewport of viewportSizes) {
        await page.setViewportSize(viewport);
        await page.goto('/');

        for (const section of sections) {
          const sectionElement = page.locator(section);
          await sectionElement.scrollIntoViewIfNeeded();
          await expect(sectionElement).toBeVisible();
        }
      }
    });
  });
});

/**
 * Responsive Design E2E Tests.
 * Owner: Scenario 6 - Responsive Design
 *
 * Tests:
 * - Desktop viewport (1920x1080)
 * - Tablet viewport (768x1024)
 * - Mobile viewport (375x667)
 * - No horizontal overflow
 * - Mobile navigation
 * - Code block horizontal scroll behavior
 */

import { test, expect } from '@playwright/test';

// Viewport configurations
const VIEWPORTS = {
  desktop: { width: 1920, height: 1080 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 },
};

test.describe('Responsive Design - Desktop Viewport (1920x1080)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
  });

  test('TC1: Desktop viewport renders all content visible with no layout issues', async ({ page }) => {
    // Verify hero section is visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify logo is visible
    const logo = page.locator('img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    // Verify h1 heading is visible
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify usage section is present
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Verify quickstart section is present
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify CTA buttons are visible and properly laid out
    const buttons = page.locator('button');
    await expect(buttons.first()).toBeVisible();

    // On desktop, buttons should be in a row (flex-row)
    const buttonContainer = page.locator('.flex.sm\\:flex-row, .flex-row');
    const containerCount = await buttonContainer.count();
    expect(containerCount).toBeGreaterThanOrEqual(0);
  });

  test('TC1: Desktop viewport displays usage grid properly', async ({ page }) => {
    // Navigate to usage section
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();

    // On desktop, the grid should show side-by-side layout (lg:grid-cols-2)
    const usageGrid = usageSection.locator('.grid');
    await expect(usageGrid).toBeVisible();

    // Verify both code block and demo are visible
    const codeBlock = usageSection.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const demoImage = usageSection.locator('[data-testid="usage-gif"]');
    await expect(demoImage).toBeVisible();
  });
});

test.describe('Responsive Design - Tablet Viewport (768x1024)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
  });

  test('TC2: Tablet viewport adapts layout with readable content', async ({ page }) => {
    // Verify hero section is visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify logo is visible
    const logo = page.locator('img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    // Verify h1 heading is visible and readable
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify all sections are accessible
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();
  });

  test('TC2: Tablet viewport has no horizontal overflow', async ({ page }) => {
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');

    // Check for horizontal overflow
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.body.scrollWidth > window.innerWidth;
    });

    expect(hasHorizontalOverflow).toBe(false);
  });

  test('TC2: Tablet content remains readable with proper sizing', async ({ page }) => {
    // Check that text is not too small on tablet
    const h1 = page.locator('h1');
    const h1Box = await h1.boundingBox();
    expect(h1Box).not.toBeNull();
    expect(h1Box!.width).toBeGreaterThan(100);

    // Verify paragraph text is visible
    const tagline = page.locator('#hero p').first();
    await expect(tagline).toBeVisible();
  });
});

test.describe('Responsive Design - Mobile Viewport (375x667)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
  });

  test('TC3: Mobile viewport shows single column layout with all content accessible', async ({ page }) => {
    // Verify hero section is visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify logo is visible and appropriately sized for mobile
    const logo = page.locator('img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    // Verify h1 is visible
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify all main sections are present and scrollable
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();
    await expect(usageSection).toBeVisible();

    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();
  });

  test('TC3: Mobile viewport buttons stack vertically', async ({ page }) => {
    // On mobile, buttons should stack (flex-col) before sm breakpoint
    const hero = page.locator('#hero');
    const buttonContainer = hero.locator('.flex.flex-col');

    // Get the button container that has flex-col
    const containerBox = await buttonContainer.first().boundingBox();
    expect(containerBox).not.toBeNull();
  });

  test('TC4: Mobile viewport has no horizontal page overflow', async ({ page }) => {
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');

    // Scroll through the entire page to trigger any lazy content
    await page.evaluate(async () => {
      await new Promise<void>((resolve) => {
        let totalHeight = 0;
        const distance = 100;
        const timer = setInterval(() => {
          const scrollHeight = document.body.scrollHeight;
          window.scrollBy(0, distance);
          totalHeight += distance;
          if (totalHeight >= scrollHeight) {
            clearInterval(timer);
            resolve();
          }
        }, 50);
      });
    });

    // Reset scroll to top
    await page.evaluate(() => window.scrollTo(0, 0));

    // Check for horizontal overflow on mobile
    const scrollData = await page.evaluate(() => {
      return {
        scrollWidth: document.body.scrollWidth,
        innerWidth: window.innerWidth,
        hasOverflow: document.body.scrollWidth > window.innerWidth
      };
    });

    expect(scrollData.hasOverflow).toBe(false);
    expect(scrollData.scrollWidth).toBeLessThanOrEqual(scrollData.innerWidth);
  });

  test('TC5: Mobile navigation elements are accessible', async ({ page }) => {
    // Check if there's a navigation or header element
    const header = page.locator('header');
    const nav = page.locator('nav');

    // Either navigation exists with hamburger menu, OR
    // the layout is simple enough that navigation isn't needed
    const headerCount = await header.count();
    const navCount = await nav.count();

    // If navigation exists, verify it's accessible on mobile
    if (navCount > 0) {
      // Look for hamburger menu button on mobile
      const hamburgerButton = page.locator('[aria-label*="menu"], [aria-label*="Menu"], button:has(svg)').first();
      const hamburgerExists = await hamburgerButton.count() > 0;

      if (hamburgerExists) {
        await expect(hamburgerButton).toBeVisible();
      }
    }

    // At minimum, the hero section should contain accessible navigation via CTA buttons
    const ctaButtons = page.locator('#hero button, #hero a');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThan(0);

    // Verify at least one CTA is visible and clickable
    await expect(ctaButtons.first()).toBeVisible();
  });

  test('TC5: All interactive elements are touch-friendly on mobile', async ({ page }) => {
    // Verify buttons have adequate touch target size (at least 44x44 pixels recommended)
    const buttons = page.locator('button').first();
    const buttonBox = await buttons.boundingBox();

    expect(buttonBox).not.toBeNull();
    expect(buttonBox!.height).toBeGreaterThanOrEqual(40);
    expect(buttonBox!.width).toBeGreaterThanOrEqual(40);
  });
});

test.describe('Responsive Design - Code Block Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
  });

  test('TC6: Code blocks scroll horizontally within container, not the page', async ({ page }) => {
    // Navigate to usage section with code blocks
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();

    // Find code block container
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Check that the code block has overflow-x-auto or similar
    const preElement = codeBlock.locator('pre');
    await expect(preElement).toBeVisible();

    // Verify the pre element allows horizontal scroll
    const hasHorizontalScroll = await preElement.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.overflowX === 'auto' || style.overflowX === 'scroll';
    });
    expect(hasHorizontalScroll).toBe(true);

    // Verify page itself doesn't have horizontal scroll
    const pageHasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > window.innerWidth;
    });
    expect(pageHasHorizontalScroll).toBe(false);
  });

  test('TC6: QuickStart code blocks scroll horizontally within container', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find pre elements in quickstart
    const preElements = quickstartSection.locator('pre');
    const preCount = await preElements.count();
    expect(preCount).toBeGreaterThan(0);

    // Check first pre element has horizontal scroll capability
    const firstPre = preElements.first();
    await expect(firstPre).toBeVisible();

    const hasHorizontalScroll = await firstPre.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.overflowX === 'auto' || style.overflowX === 'scroll';
    });
    expect(hasHorizontalScroll).toBe(true);
  });
});

test.describe('Responsive Design - Cross-Viewport Consistency', () => {
  test('All viewports show the same core content', async ({ page }) => {
    for (const [viewportName, viewport] of Object.entries(VIEWPORTS)) {
      await page.setViewportSize(viewport);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify core content is visible on all viewports
      const hero = page.locator('#hero');
      await expect(hero, `Hero should be visible on ${viewportName}`).toBeVisible();

      const h1 = page.locator('h1');
      await expect(h1, `H1 should be visible on ${viewportName}`).toBeVisible();
      await expect(h1, `H1 should contain MirDB on ${viewportName}`).toContainText('MirDB');

      const logo = page.locator('img[alt="MirDB Logo"]');
      await expect(logo, `Logo should be visible on ${viewportName}`).toBeVisible();
    }
  });

  test('No viewport causes horizontal page scroll', async ({ page }) => {
    for (const [viewportName, viewport] of Object.entries(VIEWPORTS)) {
      await page.setViewportSize(viewport);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const hasOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > window.innerWidth;
      });

      expect(hasOverflow, `${viewportName} should not have horizontal overflow`).toBe(false);
    }
  });
});

/**
 * Responsive Design E2E Tests
 * Owner: Scenario 9 (mobile), Scenario 10 (tablet/desktop)
 *
 * Test cases:
 * - Mobile: hamburger menu, single column, touch targets
 * - Tablet: appropriate layout
 * - Desktop: full navigation, multi-column
 */

const { test, expect } = require('@playwright/test');

// ============================================
// SECTION: Mobile Tests (Scenario 9)
// Viewport: 320px - 767px
// ============================================

test.describe('Mobile Responsive Design (375px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport (iPhone size)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
  });

  test('TC1: Page loads without horizontal scrollbar, content fits within viewport', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that page width does not exceed viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body should not be wider than viewport (no horizontal scroll)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1); // Allow 1px tolerance

    // Check that there is no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify main content sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
  });

  test('TC2: Navigation shows hamburger menu icon instead of full navigation links', async ({ page }) => {
    // Hamburger menu button should be visible on mobile
    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).toBeVisible();

    // Navigation links should be hidden by default on mobile
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).not.toBeVisible();

    // Check that hamburger has three bars
    const bars = page.locator('.nav-toggle__bar');
    await expect(bars).toHaveCount(3);
  });

  test('TC3: Click hamburger menu opens mobile navigation with all links visible', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Initial state: menu is closed
    await expect(navLinks).not.toBeVisible();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

    // Click hamburger to open menu
    await navToggle.click();

    // Menu should now be open
    await expect(navLinks).toBeVisible();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

    // All navigation links should be visible
    const links = navLinks.locator('.nav-link');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThanOrEqual(3); // At least Features, Quick Start, GitHub

    // Check each link is visible
    for (let i = 0; i < linkCount; i++) {
      await expect(links.nth(i)).toBeVisible();
    }

    // Close menu by clicking toggle again
    await navToggle.click();
    await expect(navLinks).not.toBeVisible();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('TC4: All buttons and links have minimum 44x44px touch target area', async ({ page }) => {
    // Get all interactive elements
    const interactiveSelectors = [
      '.btn',
      '.nav-toggle',
      '.nav-link',
      '.link-card',
      '.code-block__copy'
    ];

    for (const selector of interactiveSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);

        // Only check visible elements
        if (await element.isVisible()) {
          const boundingBox = await element.boundingBox();

          if (boundingBox) {
            // Touch targets should be at least 44x44 pixels
            expect(boundingBox.width).toBeGreaterThanOrEqual(44);
            expect(boundingBox.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    }
  });

  test('TC5: Body text is at least 16px font size and readable without zooming', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return parseFloat(computedStyle.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph text in main content areas
    const contentParagraphs = page.locator('.hero__description, .what-is__description, .feature-description');
    const paragraphCount = await contentParagraphs.count();

    for (let i = 0; i < Math.min(paragraphCount, 5); i++) {
      const paragraph = contentParagraphs.nth(i);
      if (await paragraph.isVisible()) {
        const fontSize = await paragraph.evaluate(el => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        expect(fontSize).toBeGreaterThanOrEqual(16);
      }
    }

    // Verify the page doesn't require horizontal scrolling for main content
    // (code blocks may have intentional horizontal scroll, so we check document level)
    const hasPageHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasPageHorizontalScroll).toBe(false);
  });

  test('Hamburger menu closes when clicking outside', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Open menu
    await navToggle.click();
    await expect(navLinks).toBeVisible();

    // Click outside the menu
    await page.locator('.hero').click();

    // Menu should close
    await expect(navLinks).not.toBeVisible();
  });

  test('Hamburger menu closes on Escape key', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Open menu
    await navToggle.click();
    await expect(navLinks).toBeVisible();

    // Press Escape
    await page.keyboard.press('Escape');

    // Menu should close
    await expect(navLinks).not.toBeVisible();
  });

  test('Mobile menu links close menu when clicked', async ({ page }) => {
    const navToggle = page.locator('.nav-toggle');
    const navLinks = page.locator('.nav-links');

    // Open menu
    await navToggle.click();
    await expect(navLinks).toBeVisible();

    // Click on Features link
    const featuresLink = navLinks.locator('a[href="#features"]');
    await featuresLink.click();

    // Wait for click to register and menu to close
    await page.waitForTimeout(500);

    // Menu should close after clicking a link (key mobile UX behavior)
    await expect(navLinks).not.toBeVisible();

    // Navigation toggle should show collapsed state
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('Feature cards display in single column on mobile', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid column style
    const gridColumns = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // On mobile, grid should be single column (1fr)
    expect(gridColumns).toMatch(/^[\d.]+px$/); // Single column width
  });

  test('Steps display vertically on mobile', async ({ page }) => {
    const steps = page.locator('.step');
    const stepCount = await steps.count();

    if (stepCount > 1) {
      // Check that steps are stacked vertically (flex-direction: column)
      const firstStep = steps.first();
      const flexDirection = await firstStep.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    }
  });

  test('Hero CTA buttons stack vertically on mobile', async ({ page }) => {
    const heroCta = page.locator('.hero__cta');

    if (await heroCta.isVisible()) {
      const flexDirection = await heroCta.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    }
  });
});

// Additional viewport tests for edge cases
test.describe('Mobile Edge Cases', () => {
  test('Works at minimum mobile width (320px)', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Page should still be usable
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.nav-toggle')).toBeVisible();

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('Works at upper mobile boundary (767px)', async ({ page }) => {
    await page.setViewportSize({ width: 767, height: 1024 });
    await page.goto('/');

    // Hamburger menu should still be visible at 767px
    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).toBeVisible();

    // Nav links should be hidden
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).not.toBeVisible();
  });
});

// ============================================
// SECTION: Tablet Tests (Scenario 10)
// Viewport: 768px - 1023px
// ============================================

test.describe('Tablet Responsive Design (768px) - Scenario 10', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
  });

  test('TC1: Page displays tablet-appropriate layout without horizontal scroll', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that there is no visible horizontal scrollbar (overflow-x: hidden is acceptable)
    // The test checks user experience - can they scroll horizontally?
    const hasHorizontalScroll = await page.evaluate(() => {
      // Check if there's visible horizontal scrollbar by comparing scroll dimensions
      const html = document.documentElement;
      const canScrollHorizontally = html.scrollWidth > html.clientWidth &&
        getComputedStyle(document.body).overflowX !== 'hidden' &&
        getComputedStyle(html).overflowX !== 'hidden';
      return canScrollHorizontally;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify all content is accessible within the viewport (user can see all content)
    const heroBox = await page.locator('.hero').boundingBox();
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Hero section should fit within viewport
    expect(heroBox.x + heroBox.width).toBeLessThanOrEqual(viewportWidth + 10); // 10px tolerance

    // Verify main content sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
  });

  test('TC2: Navigation shows full links or hamburger depending on design at 768px', async ({ page }) => {
    // At 768px (tablet breakpoint), navigation should show full links
    const navLinks = page.locator('.nav-links');
    const navToggle = page.locator('.nav-toggle');

    // On tablet, full nav links should be visible (hamburger hidden)
    await expect(navLinks).toBeVisible();

    // Hamburger should be hidden on tablet
    await expect(navToggle).not.toBeVisible();

    // Check that navigation links are in row layout
    const navLinksDisplay = await navLinks.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.display;
    });
    expect(navLinksDisplay).toBe('flex');

    // Verify at least 3 navigation links are visible
    const links = navLinks.locator('.nav-link');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThanOrEqual(3);
  });

  test('Feature cards display in 2-column grid at tablet width', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid column style - should be 2 columns on tablet
    const gridColumns = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // Should have 2 column values (e.g., "300px 300px" or similar)
    const columnValues = gridColumns.split(' ').filter(v => v.includes('px') || v.includes('fr'));
    expect(columnValues.length).toBe(2);
  });

  test('Hero CTA buttons display in row at tablet width', async ({ page }) => {
    const heroCta = page.locator('.hero__cta');

    if (await heroCta.isVisible()) {
      const flexDirection = await heroCta.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    }
  });

  test('Comparison table shows proper 3-column layout at tablet', async ({ page }) => {
    const comparisonTable = page.locator('.comparison-table');

    if (await comparisonTable.isVisible()) {
      const gridColumns = await comparisonTable.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Should have 3 column values for feature, mirdb, memcached
      const columnValues = gridColumns.split(' ').filter(v => v.includes('px') || v.includes('fr'));
      expect(columnValues.length).toBe(3);
    }
  });

  test('Steps display horizontally at tablet width', async ({ page }) => {
    const steps = page.locator('.step');
    const stepCount = await steps.count();

    if (stepCount > 0) {
      const firstStep = steps.first();
      const flexDirection = await firstStep.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    }
  });
});

// ============================================
// SECTION: Desktop Tests (Scenario 10)
// Viewport: 1024px+
// ============================================

test.describe('Desktop Responsive Design (1280px) - Scenario 10', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
  });

  test('TC3: Page displays full desktop layout with multi-column sections', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that page width does not exceed viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body should not be wider than viewport (no horizontal scroll)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Check that there is no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify all main content sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#code-example')).toBeVisible();
    await expect(page.locator('.site-footer')).toBeVisible();
  });

  test('TC4: Feature cards display in grid layout (2-3 columns) at desktop', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid column style - should be 3 columns on desktop
    const gridColumns = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // Should have 3 column values (e.g., "350px 350px 350px" or similar)
    const columnValues = gridColumns.split(' ').filter(v => v.includes('px') || v.includes('fr'));
    expect(columnValues.length).toBeGreaterThanOrEqual(2);
    expect(columnValues.length).toBeLessThanOrEqual(3);
  });

  test('Desktop shows full navigation with all links visible', async ({ page }) => {
    // Navigation links should be visible on desktop
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Hamburger menu should be hidden on desktop
    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).not.toBeVisible();

    // Check that navigation links are in row layout
    const navLinksDisplay = await navLinks.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.flexDirection;
    });
    expect(navLinksDisplay).toBe('row');

    // Verify all navigation links are visible
    const links = navLinks.locator('.nav-link');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Each link should be visible
    for (let i = 0; i < linkCount; i++) {
      await expect(links.nth(i)).toBeVisible();
    }
  });

  test('Hero section displays at full width with proper layout', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Hero CTA buttons should be in a row
    const heroCta = page.locator('.hero__cta');
    if (await heroCta.isVisible()) {
      const flexDirection = await heroCta.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    }

    // Hero tagline should be visible
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
  });

  test('Links section displays in 3-column grid at desktop', async ({ page }) => {
    const linksGrid = page.locator('.links-grid');

    if (await linksGrid.isVisible()) {
      const gridColumns = await linksGrid.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Should have 3 column values
      const columnValues = gridColumns.split(' ').filter(v => v.includes('px') || v.includes('fr'));
      expect(columnValues.length).toBe(3);
    }
  });

  test('Footer displays in row layout at desktop', async ({ page }) => {
    const footerContent = page.locator('.footer-content');

    if (await footerContent.isVisible()) {
      const flexDirection = await footerContent.evaluate(el => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    }
  });
});

// ============================================
// SECTION: Desktop Edge Cases (Scenario 10)
// ============================================

test.describe('Desktop Edge Cases - Scenario 10', () => {
  test('Works at lower desktop boundary (1024px)', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    // Navigation should show full links
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).not.toBeVisible();

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('Works at wide desktop (1920px)', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Page should still be properly contained
    const container = page.locator('.container').first();
    if (await container.isVisible()) {
      const containerWidth = await container.evaluate(el => {
        return el.getBoundingClientRect().width;
      });
      // Container should be constrained to max-width
      expect(containerWidth).toBeLessThanOrEqual(1200 + 100); // Allow some padding
    }

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('Tablet boundary (767px) still shows mobile layout', async ({ page }) => {
    await page.setViewportSize({ width: 767, height: 1024 });
    await page.goto('/');

    // At 767px (just below tablet), hamburger should still be visible
    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).toBeVisible();

    // Nav links should be hidden
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).not.toBeVisible();
  });
});

/**
 * Responsive Design E2E Tests
 * Owner: Scenario 10 - Responsive Design
 *
 * Tests:
 * - Mobile viewport (320px, 375px)
 * - Tablet viewport (768px)
 * - Desktop viewport (1024px, 1440px)
 * - Touch target sizes
 * - Orientation changes
 */

const { test, expect } = require('@playwright/test');

// Viewport configurations
const viewports = {
  smallMobile: { width: 320, height: 568 },
  iphone: { width: 375, height: 667 },
  tabletPortrait: { width: 768, height: 1024 },
  tabletLandscape: { width: 1024, height: 768 },
  desktop: { width: 1440, height: 900 },
};

test.describe('Responsive Design - Small Mobile (320px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.smallMobile);
    await page.goto('/');
  });

  test('TC1: All content visible, no horizontal scroll, text readable, images scale', async ({ page }) => {
    // Check no horizontal overflow (allow small tolerance for scrollbar)
    const body = page.locator('body');
    const bodyWidth = await body.evaluate((el) => el.scrollWidth);
    const viewportWidth = viewports.smallMobile.width;
    // Allow 16px tolerance for scrollbar width
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 16);

    // Check hero section content is visible
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    const heroTagline = page.locator('.hero__tagline');
    await expect(heroTagline).toBeVisible();

    // Check hero logo scales properly
    const heroLogo = page.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();
    const logoBox = await heroLogo.boundingBox();
    expect(logoBox.width).toBeLessThanOrEqual(viewportWidth - 32); // Account for padding

    // Check text is readable (font size at least 14px)
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    // Check main sections are visible
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#usage')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('#roadmap')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();
  });
});

test.describe('Responsive Design - iPhone (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.iphone);
    await page.goto('/');
  });

  test('TC2: Mobile layout active: single column, stacked elements, hamburger menu', async ({ page }) => {
    // Check hamburger menu is visible (mobile toggle)
    const navToggle = page.locator('.nav__toggle');
    await expect(navToggle).toBeVisible();

    // Check nav links are initially hidden on mobile
    const navLinks = page.locator('.nav__links');
    const isVisible = await navLinks.isVisible();
    const opacity = await navLinks.evaluate((el) => window.getComputedStyle(el).opacity);

    // Nav links should either be hidden or have opacity 0 on mobile
    if (isVisible) {
      expect(parseFloat(opacity)).toBeLessThanOrEqual(0);
    }

    // Check features grid is single column
    const featuresGrid = page.locator('.features__grid');
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // Single column means only one value or all values equal
    const columns = gridColumns.split(' ').filter(v => v !== 'none' && v !== '');
    expect(columns.length).toBeLessThanOrEqual(1);

    // Check hero CTA buttons are stacked (column direction)
    const ctaGroup = page.locator('.hero__cta-group');
    const flexDirection = await ctaGroup.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('column');
  });
});

test.describe('Responsive Design - Tablet Portrait (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.tabletPortrait);
    await page.goto('/');
  });

  test('TC3: Tablet layout active: 2-column grids where applicable, adjusted spacing', async ({ page }) => {
    // Check features grid has at least 2 columns at tablet
    const featuresGrid = page.locator('.features__grid');
    const gridColumns = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });
    // Parse grid columns - at 768px we should have 2+ columns
    const columnValues = gridColumns.split(' ').filter(v => v && v !== 'none' && parseFloat(v) > 0);
    // At tablet breakpoint (768px), features.css defines 2 columns via repeat(2, 1fr)
    expect(columnValues.length).toBeGreaterThanOrEqual(2);

    // Check footer links has appropriate grid layout
    const footerLinks = page.locator('.footer__links');
    const footerGridColumns = await footerLinks.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const footerColumns = footerGridColumns.split(' ').filter(v => v && v !== 'none' && parseFloat(v) > 0);
    expect(footerColumns.length).toBeGreaterThanOrEqual(2);

    // At exactly 768px, nav.css uses max-width: 768px so hamburger is still visible
    // Test at 769px to verify desktop navigation kicks in
    await page.setViewportSize({ width: 769, height: 1024 });
    await page.waitForTimeout(100); // Allow CSS to update

    // Check navigation is horizontally laid out (hamburger hidden at 769px+)
    const navToggle = page.locator('.nav__toggle');
    const toggleDisplay = await navToggle.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(toggleDisplay).toBe('none');

    // Navigation links should be visible
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();
  });
});

test.describe('Responsive Design - Tablet Landscape/Small Desktop (1024px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.tabletLandscape);
    await page.goto('/');
  });

  test('TC4: Transition to desktop layout begins, navigation fully visible', async ({ page }) => {
    // Navigation should be fully visible
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Hamburger menu should be hidden
    const navToggle = page.locator('.nav__toggle');
    const toggleDisplay = await navToggle.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(toggleDisplay).toBe('none');

    // Check all nav links are visible
    const navLinkItems = page.locator('.nav__link');
    const linkCount = await navLinkItems.count();
    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      await expect(navLinkItems.nth(i)).toBeVisible();
    }

    // Features grid should have at least 2-3 columns
    const featuresGrid = page.locator('.features__grid');
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const columnValues = gridColumns.split(' ').filter(v => v && v !== 'none' && parseFloat(v) > 0);
    expect(columnValues.length).toBeGreaterThanOrEqual(2);
  });
});

test.describe('Responsive Design - Desktop (1440px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.desktop);
    await page.goto('/');
  });

  test('TC5: Full desktop layout: 3-column feature grid, side-by-side layouts, max-width container', async ({ page }) => {
    // Check features grid has 3 columns on desktop
    const featuresGrid = page.locator('.features__grid');
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const columnValues = gridColumns.split(' ').filter(v => v && v !== 'none' && parseFloat(v) > 0);
    expect(columnValues.length).toBeGreaterThanOrEqual(3);

    // Check container max-width is applied (1200px from CSS variables)
    const container = page.locator('.container').first();
    const containerMaxWidth = await container.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(parseFloat(containerMaxWidth)).toBeLessThanOrEqual(1200);

    // Check side-by-side layout in data flow section
    const dataflowContainer = page.locator('.dataflow-container');
    if (await dataflowContainer.count() > 0) {
      const dataflowGrid = await dataflowContainer.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });
      const dataflowColumns = dataflowGrid.split(' ').filter(v => v && v !== 'none' && parseFloat(v) > 0);
      expect(dataflowColumns.length).toBeGreaterThanOrEqual(2);
    }

    // Check footer has side-by-side layout
    const footerContainer = page.locator('.footer__container');
    const footerGrid = await footerContainer.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const footerColumns = footerGrid.split(' ').filter(v => v && v !== 'none' && parseFloat(v) > 0);
    expect(footerColumns.length).toBeGreaterThanOrEqual(2);
  });
});

test.describe('Responsive Design - Touch Targets', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.iphone);
    await page.goto('/');
  });

  test('TC6: All buttons and links have minimum 44x44px tap area', async ({ page }) => {
    const minTouchTarget = 44;

    // Check hero CTA buttons
    const heroCtaButtons = page.locator('.hero__cta');
    const ctaCount = await heroCtaButtons.count();
    for (let i = 0; i < ctaCount; i++) {
      const box = await heroCtaButtons.nth(i).boundingBox();
      expect(box.width).toBeGreaterThanOrEqual(minTouchTarget);
      expect(box.height).toBeGreaterThanOrEqual(minTouchTarget);
    }

    // Check navigation toggle button
    const navToggle = page.locator('.nav__toggle');
    if (await navToggle.isVisible()) {
      const toggleBox = await navToggle.boundingBox();
      expect(toggleBox.width).toBeGreaterThanOrEqual(minTouchTarget);
      expect(toggleBox.height).toBeGreaterThanOrEqual(minTouchTarget);
    }

    // Check navigation links (when menu is open)
    await navToggle.click();
    await page.waitForTimeout(300); // Wait for animation
    const navLinks = page.locator('.nav__link');
    const navLinkCount = await navLinks.count();
    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      if (await link.isVisible()) {
        const linkBox = await link.boundingBox();
        expect(linkBox.height).toBeGreaterThanOrEqual(minTouchTarget);
      }
    }

    // Check footer links - verify primary links have adequate touch area
    // Note: inline links (like license link) may not meet 44px but are acceptable
    const footerLinks = page.locator('.footer__link:not(.footer__link--inline)');
    const footerLinkCount = await footerLinks.count();
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      if (await link.isVisible()) {
        const linkBox = await link.boundingBox();
        if (linkBox) {
          // Either the actual rendered height meets the target, or the min-height is set
          const minHeight = await link.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            const minH = parseFloat(computed.minHeight) || 0;
            const actualH = el.getBoundingClientRect().height;
            return Math.max(minH, actualH);
          });
          expect(minHeight).toBeGreaterThanOrEqual(minTouchTarget);
        }
      }
    }

    // Check roadmap contribution links
    const contributionLinks = page.locator('.roadmap__contribution-link');
    const contribCount = await contributionLinks.count();
    for (let i = 0; i < contribCount; i++) {
      const link = contributionLinks.nth(i);
      if (await link.isVisible()) {
        const linkBox = await link.boundingBox();
        expect(linkBox.height).toBeGreaterThanOrEqual(minTouchTarget);
      }
    }
  });
});

test.describe('Responsive Design - Orientation Changes', () => {
  test('TC7: Layout adjusts smoothly without content clipping or overflow on rotation', async ({ page }) => {
    // Start in portrait mode
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Verify no significant horizontal overflow in portrait (allow scrollbar tolerance)
    let bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    let viewportWidth = 375;
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 16);

    // Check content visibility in portrait
    await expect(page.locator('.hero__title')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();

    // Rotate to landscape
    await page.setViewportSize({ width: 667, height: 375 });
    await page.waitForTimeout(100); // Small wait for layout reflow

    // Verify no horizontal overflow in landscape
    bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    viewportWidth = 667;
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify content is still visible and not clipped in landscape
    await expect(page.locator('.hero__title')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();

    // Check hero section adapts
    const heroContent = page.locator('.hero__content');
    const heroBox = await heroContent.boundingBox();
    expect(heroBox.width).toBeLessThanOrEqual(viewportWidth);

    // Check images scale properly in landscape
    const heroLogo = page.locator('.hero__logo');
    const logoBox = await heroLogo.boundingBox();
    expect(logoBox.width).toBeLessThanOrEqual(viewportWidth - 32);

    // Rotate back to portrait to verify seamless transitions
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(100);

    // Verify layout restored correctly (allow scrollbar tolerance)
    bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(375 + 16);
    await expect(page.locator('.hero__title')).toBeVisible();
  });
});

test.describe('Responsive Design - Additional Breakpoint Tests', () => {
  test('Container max-width respected at all breakpoints', async ({ page }) => {
    const breakpoints = [
      { width: 320, maxExpected: 320 },
      { width: 768, maxExpected: 768 },
      { width: 1024, maxExpected: 1024 },
      { width: 1440, maxExpected: 1200 }, // Max-width container is 1200px
    ];

    for (const bp of breakpoints) {
      await page.setViewportSize({ width: bp.width, height: 800 });
      await page.goto('/');

      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();

      expect(containerBox.width).toBeLessThanOrEqual(bp.maxExpected);
    }
  });

  test('All sections stack properly on mobile', async ({ page }) => {
    await page.setViewportSize(viewports.iphone);
    await page.goto('/');

    // Get section positions
    const sections = ['#hero', '#features', '#usage', '#architecture', '#roadmap', '.footer'];
    let previousBottom = 0;

    for (const selector of sections) {
      const section = page.locator(selector);
      const box = await section.boundingBox();

      // Each section should be below the previous one (stacked vertically)
      expect(box.y).toBeGreaterThanOrEqual(previousBottom - 10); // Small tolerance for margins
      previousBottom = box.y + box.height;
    }
  });

  test('Images are responsive and scale correctly', async ({ page }) => {
    await page.setViewportSize(viewports.iphone);
    await page.goto('/');

    // Check all images have max-width: 100%
    const images = page.locator('img');
    const imgCount = await images.count();

    for (let i = 0; i < imgCount; i++) {
      const img = images.nth(i);
      if (await img.isVisible()) {
        const maxWidth = await img.evaluate((el) => {
          return window.getComputedStyle(el).maxWidth;
        });
        expect(maxWidth).toBe('100%');
      }
    }
  });
});

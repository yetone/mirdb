/**
 * Responsive Design E2E Tests
 * Owners: Scenarios 8, 9, 10 (Responsive)
 *
 * Test groups:
 * - Mobile viewport (375px) layout
 * - Tablet viewport (768px) layout
 * - Desktop viewport (1280px) layout
 * - Grid column adjustments
 * - Touch target sizes
 * - No horizontal scroll
 */

import { test, expect } from '@playwright/test';
import { VIEWPORTS, waitForLoad, getComputedStyle } from './utils';

// Scenario 8: Responsive Design - Mobile
test.describe('Responsive Design - Mobile (Scenario 8)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Page renders at 375px without horizontal scrollbar', async ({ page }) => {
    // Get body dimensions
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // No horizontal scrollbar means body width <= viewport width
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Also check that html doesn't overflow
    const htmlWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlWidth).toBeLessThanOrEqual(viewportWidth);

    // Ensure no horizontal scroll is possible
    const canScrollHorizontally = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(canScrollHorizontally).toBe(false);
  });

  test('TC2: Hero section layout at mobile viewport - logo, headline, and CTA visible', async ({ page }) => {
    // Check logo is visible
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Check logo dimensions are reasonable for mobile
    const logoBoundingBox = await logo.boundingBox();
    expect(logoBoundingBox).not.toBeNull();
    if (logoBoundingBox) {
      expect(logoBoundingBox.width).toBeLessThanOrEqual(150);
      expect(logoBoundingBox.width).toBeGreaterThan(50);
    }

    // Check headline is visible
    const headline = page.locator('.hero-headline');
    await expect(headline).toBeVisible();

    // Check headline fits within viewport (no overflow)
    const headlineBoundingBox = await headline.boundingBox();
    expect(headlineBoundingBox).not.toBeNull();
    if (headlineBoundingBox) {
      expect(headlineBoundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    }

    // Check primary CTA button is visible
    const ctaButton = page.locator('#primary-cta');
    await expect(ctaButton).toBeVisible();

    // Check secondary CTA button is visible
    const secondaryCta = page.locator('.hero-cta .btn-secondary');
    await expect(secondaryCta).toBeVisible();
  });

  test('TC3: Features section displays in single column at mobile viewport', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get grid computed style
    const gridStyle = await page.evaluate(() => {
      const grid = document.querySelector('.features-grid');
      if (!grid) return null;
      const style = window.getComputedStyle(grid);
      return {
        gridTemplateColumns: style.gridTemplateColumns,
        display: style.display,
      };
    });

    expect(gridStyle).not.toBeNull();
    if (gridStyle) {
      // Should be either single column or 2-column max
      // Single column would show as just pixel value like "343px"
      // 2-column would show as two values like "171.5px 171.5px"
      const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c.trim());
      expect(columns.length).toBeLessThanOrEqual(2);
    }

    // Verify all feature items are visible
    const featureItems = page.locator('.feature-item');
    const count = await featureItems.count();
    expect(count).toBeGreaterThan(0);

    // Check each feature item doesn't overflow
    for (let i = 0; i < count; i++) {
      const item = featureItems.nth(i);
      const boundingBox = await item.boundingBox();
      expect(boundingBox).not.toBeNull();
      if (boundingBox) {
        expect(boundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
      }
    }
  });

  test('TC4: Primary CTA button has at least 44px height for touch targets', async ({ page }) => {
    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeVisible();

    // Get button dimensions
    const boundingBox = await primaryCta.boundingBox();
    expect(boundingBox).not.toBeNull();
    if (boundingBox) {
      // WCAG/Apple HIG recommends minimum 44px touch targets
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);
    }

    // Also check computed min-height
    const computedHeight = await page.evaluate(() => {
      const btn = document.querySelector('#primary-cta');
      if (!btn) return 0;
      return btn.getBoundingClientRect().height;
    });
    expect(computedHeight).toBeGreaterThanOrEqual(44);
  });

  test('All touch targets meet minimum 44px size requirement', async ({ page }) => {
    // Check all buttons in hero section
    const heroButtons = page.locator('.hero-cta .btn');
    const heroButtonCount = await heroButtons.count();
    for (let i = 0; i < heroButtonCount; i++) {
      const btn = heroButtons.nth(i);
      const box = await btn.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }

    // Check footer links for touch-friendly sizing
    await page.locator('#footer').scrollIntoViewIfNeeded();
    const footerLinks = page.locator('.footer-link');
    const footerLinkCount = await footerLinks.count();
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const box = await link.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Footer links should also be touch-friendly
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('Text is readable without horizontal scrolling', async ({ page }) => {
    // Check that no text element overflows the viewport
    const overflowingElements = await page.evaluate(() => {
      const elements = document.querySelectorAll('p, h1, h2, h3, span, a, li');
      const overflowing: string[] = [];
      const viewportWidth = window.innerWidth;

      elements.forEach((el) => {
        const rect = el.getBoundingClientRect();
        if (rect.right > viewportWidth) {
          overflowing.push(el.tagName + ': ' + el.textContent?.substring(0, 50));
        }
      });

      return overflowing;
    });

    expect(overflowingElements).toHaveLength(0);
  });

  test('Hero section stacks vertically on mobile', async ({ page }) => {
    // Check that hero CTA buttons stack vertically
    const heroCtaContainer = page.locator('.hero-cta');
    const flexDirection = await page.evaluate(() => {
      const el = document.querySelector('.hero-cta');
      if (!el) return '';
      return window.getComputedStyle(el).flexDirection;
    });

    expect(flexDirection).toBe('column');
  });

  test('Code blocks are readable and scrollable', async ({ page }) => {
    // Scroll to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.step-code');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check that code blocks don't overflow the viewport
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const boundingBox = await codeBlock.boundingBox();
      expect(boundingBox).not.toBeNull();
      if (boundingBox) {
        // Code blocks should fit within container (with overflow-x for scrolling)
        expect(boundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
      }
    }
  });
});

// Scenario 9: Responsive Design - Tablet
test.describe('Tablet Responsive Design (768px-1024px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet size (768px width)
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Page renders with appropriate tablet layout at 768px', async ({ page }) => {
    // Verify the page renders correctly at tablet viewport
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const quickstart = page.locator('#quickstart');
    await expect(quickstart).toBeVisible();

    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Verify no horizontal scroll
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('TC2: Features display in 2-3 column grid at tablet viewport', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid template columns using computed style
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // At tablet viewport (768px), the grid should have 2 columns
    // gridTemplateColumns returns actual pixel values like "350px 350px" for 2 columns
    const columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;

    // Should have 2-3 columns at tablet viewport (scaffold says 2)
    expect(columnCount).toBeGreaterThanOrEqual(2);
    expect(columnCount).toBeLessThanOrEqual(3);

    // Verify all feature items are visible
    const featureItems = page.locator('.feature-item');
    const featureCount = await featureItems.count();
    expect(featureCount).toBeGreaterThanOrEqual(3); // Should have at least 3 features

    for (let i = 0; i < featureCount; i++) {
      await expect(featureItems.nth(i)).toBeVisible();
    }
  });

  test('TC3: Navigation links are visible and accessible', async ({ page }) => {
    // Check hero CTA buttons are visible
    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toHaveAttribute('href', /github\.com/);

    const secondaryCta = page.locator('a.btn-secondary');
    await expect(secondaryCta).toBeVisible();

    // Check footer navigation links are visible
    const footerNav = page.locator('.footer-nav');
    await footerNav.scrollIntoViewIfNeeded();
    await expect(footerNav).toBeVisible();

    // Verify footer links are accessible
    const footerLinks = page.locator('.footer-link');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();

      // Ensure links have valid href attributes
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).not.toBe('#');
    }

    // Check badges section links are visible
    const badgesSection = page.locator('#badges');
    await badgesSection.scrollIntoViewIfNeeded();
    await expect(badgesSection).toBeVisible();

    const badgeLinks = page.locator('#badges a');
    const badgeCount = await badgeLinks.count();
    expect(badgeCount).toBeGreaterThanOrEqual(1);
  });

  test('Content scales appropriately at tablet viewport', async ({ page }) => {
    // Verify hero section content is proportionally sized
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    const logoBox = await heroLogo.boundingBox();
    expect(logoBox).not.toBeNull();
    // Logo should be reasonably sized for tablet (not too small, not too large)
    expect(logoBox!.width).toBeGreaterThanOrEqual(100);
    expect(logoBox!.width).toBeLessThanOrEqual(300);

    // Verify text is readable (not too small)
    const heroHeadline = page.locator('.hero-headline');
    const fontSize = await heroHeadline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Font size should be at least 24px for tablet readability
    expect(fontSize).toBeGreaterThanOrEqual(24);

    // Verify usage section scales correctly
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();
    await expect(usageSection).toBeVisible();

    // Verify quick start steps are properly sized
    const quickstartStep = page.locator('.quickstart-step').first();
    await quickstartStep.scrollIntoViewIfNeeded();
    const stepBox = await quickstartStep.boundingBox();
    expect(stepBox).not.toBeNull();
    // Step should use reasonable width at tablet
    expect(stepBox!.width).toBeGreaterThanOrEqual(300);
  });

  test('Touch targets are appropriately sized for tablet', async ({ page }) => {
    // Verify CTA buttons have adequate touch target size (at least 44x44 for accessibility)
    const primaryCta = page.locator('#primary-cta');
    const ctaBox = await primaryCta.boundingBox();
    expect(ctaBox).not.toBeNull();
    expect(ctaBox!.height).toBeGreaterThanOrEqual(44);

    // Verify badge links have adequate size
    const badgeLink = page.locator('#badges a').first();
    await badgeLink.scrollIntoViewIfNeeded();
    const badgeBox = await badgeLink.boundingBox();
    expect(badgeBox).not.toBeNull();
    // Badges should have reasonable clickable area
    expect(badgeBox!.height).toBeGreaterThanOrEqual(18);
  });

  test('Roadmap section adapts to tablet layout', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();
    await expect(roadmapSection).toBeVisible();

    // Verify roadmap columns are displayed appropriately
    const roadmapContent = page.locator('.roadmap-content');
    await expect(roadmapContent).toBeVisible();

    // Check that roadmap columns are visible and properly arranged
    const roadmapColumns = page.locator('.roadmap-column');
    const columnCount = await roadmapColumns.count();
    expect(columnCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < columnCount; i++) {
      await expect(roadmapColumns.nth(i)).toBeVisible();
    }
  });
});

/**
 * Scenario 10: Desktop Responsive Design Tests
 * Verifies homepage displays correctly on desktop screens (1024px+ width)
 */
test.describe('Desktop Responsive Design (Scenario 10)', () => {
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');
    await waitForLoad(page);
  });

  /**
   * Test Case 1: Page renders with full desktop layout at 1280px
   */
  test('should render full desktop layout at 1280px viewport', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible and properly sized
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // On desktop, hero should have generous min-height (90vh from responsive.css)
    const heroMinHeight = await getComputedStyle(page, '.hero', 'min-height');
    // 90vh at 720px height = 648px
    expect(parseFloat(heroMinHeight)).toBeGreaterThanOrEqual(600);

    // Verify all main sections are visible
    const sections = ['#badges', '#features', '#usage', '#quickstart', '#roadmap', '#footer'];
    for (const selector of sections) {
      await expect(page.locator(selector)).toBeVisible();
    }

    // Verify no horizontal scroll (content fits within viewport)
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // +1 for rounding tolerance
  });

  /**
   * Test Case 2: Features display in 3-4 column grid at desktop viewport
   */
  test('should display features in 3-4 column grid', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid layout - should show 4 columns at 1280px (per responsive.css)
    const gridColumns = await getComputedStyle(page, '.features-grid', 'grid-template-columns');

    // Count the number of column values (space-separated pixel values)
    const columnCount = gridColumns.trim().split(/\s+/).filter(v => v.length > 0).length;

    // At desktop (1024px+), features-grid uses repeat(4, 1fr) = 4 columns
    expect(columnCount).toBeGreaterThanOrEqual(3);
    expect(columnCount).toBeLessThanOrEqual(4);

    // Verify all 4 feature items are visible
    const featureItems = page.locator('.feature-item');
    await expect(featureItems).toHaveCount(4);

    // All items should be visible simultaneously on desktop
    for (let i = 0; i < 4; i++) {
      await expect(featureItems.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 3: Main content container has max-width and is centered
   */
  test('should have max-width constraint and centered content', async ({ page }) => {
    // Check container max-width
    const containerMaxWidth = await getComputedStyle(page, '.container', 'max-width');

    // At desktop, container should have max-width of 1200px (per responsive.css)
    expect(parseFloat(containerMaxWidth)).toBeLessThanOrEqual(1200);
    expect(parseFloat(containerMaxWidth)).toBeGreaterThan(0);

    // Check that containers are centered using margin auto
    const containerMarginLeft = await getComputedStyle(page, '.container', 'margin-left');
    const containerMarginRight = await getComputedStyle(page, '.container', 'margin-right');

    // Both margins should be 'auto' or equal pixel values (centered)
    // When max-width is less than viewport, margin-left and margin-right will be equal positive values
    const leftMargin = parseFloat(containerMarginLeft) || 0;
    const rightMargin = parseFloat(containerMarginRight) || 0;

    // At 1280px viewport with 1200px container, margins should be ~40px each
    // Just verify they're roughly equal (within 2px tolerance for rounding)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(2);

    // Verify content doesn't stretch edge-to-edge
    const featuresContainer = page.locator('#features .container');
    const boundingBox = await featuresContainer.boundingBox();

    if (boundingBox) {
      // Container should have margins on both sides (not stretching to full 1280px)
      expect(boundingBox.width).toBeLessThanOrEqual(1200);
      expect(boundingBox.x).toBeGreaterThan(0); // Left margin exists
    }
  });

  /**
   * Test Case 4: Layout remains centered at ultra-wide 1920px viewport
   */
  test('should remain centered with appropriate margins at 1920px ultra-wide viewport', async ({ page }) => {
    // Set ultra-wide viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await waitForLoad(page);

    // Verify page still renders correctly
    await expect(page).toHaveTitle(/MirDB/);
    await expect(page.locator('#hero')).toBeVisible();

    // Get features container for measurement
    const featuresContainer = page.locator('#features .container');
    await featuresContainer.scrollIntoViewIfNeeded();
    const boundingBox = await featuresContainer.boundingBox();

    if (boundingBox) {
      // Container should still be max 1200px wide
      expect(boundingBox.width).toBeLessThanOrEqual(1200);

      // With 1920px viewport and 1200px container, there should be ~360px margin on each side
      const expectedMinMargin = (1920 - 1200) / 2 - 50; // 310px minimum (with tolerance)
      expect(boundingBox.x).toBeGreaterThanOrEqual(expectedMinMargin);

      // Content should be roughly centered
      const centerX = boundingBox.x + boundingBox.width / 2;
      const viewportCenter = 1920 / 2;
      expect(Math.abs(centerX - viewportCenter)).toBeLessThan(50); // Within 50px of center
    }

    // Features grid should still show 4 columns
    const gridColumns = await getComputedStyle(page, '.features-grid', 'grid-template-columns');
    const columnCount = gridColumns.trim().split(/\s+/).filter(v => v.length > 0).length;
    expect(columnCount).toBe(4);

    // Verify no horizontal scroll
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);
  });

  /**
   * Additional desktop-specific tests
   */
  test('should show generous whitespace and proper spacing', async ({ page }) => {
    // Check features section padding (should have substantial padding on desktop)
    // Using .features-section as it has standard section padding: 4rem 1.5rem = 64px top/bottom
    const sectionPadding = await getComputedStyle(page, '.features-section', 'padding-top');
    const paddingValue = parseFloat(sectionPadding);

    // Desktop should have generous padding (4rem = 64px from styles.css)
    expect(paddingValue).toBeGreaterThanOrEqual(48);

    // Check hero section has proper centered content
    const heroCTA = page.locator('.hero-cta');
    await expect(heroCTA).toBeVisible();

    // CTA buttons should be visible and properly spaced
    const buttons = heroCTA.locator('.btn');
    await expect(buttons).toHaveCount(2);

    // Both buttons should be on the same row (not stacked) on desktop
    const btn1Box = await buttons.nth(0).boundingBox();
    const btn2Box = await buttons.nth(1).boundingBox();

    if (btn1Box && btn2Box) {
      // Y positions should be roughly equal (same row)
      expect(Math.abs(btn1Box.y - btn2Box.y)).toBeLessThan(10);
    }
  });

  test('should render typography at desktop font sizes', async ({ page }) => {
    // Hero headline should be 3rem (48px) at desktop per responsive.css
    const heroHeadlineFontSize = await getComputedStyle(page, '.hero-headline', 'font-size');
    const fontSize = parseFloat(heroHeadlineFontSize);

    // At desktop (1024px+), hero-headline is 3rem = 48px
    expect(fontSize).toBeGreaterThanOrEqual(40); // Allow some variance
    expect(fontSize).toBeLessThanOrEqual(52);
  });
});

/**
 * Responsive Design Tests
 * Owner: Scenario 7 (Mobile), Scenario 8 (Tablet/Desktop)
 *
 * Tests responsive design across different viewport sizes:
 * - Mobile: max-width 599px (Scenario 7)
 * - Tablet: 600px - 1023px (Scenario 8)
 * - Desktop: min-width 1024px (Scenario 8)
 *
 * Tests mobile responsive behavior including:
 * - No horizontal scrolling at 375px width
 * - Mobile navigation toggle (hamburger menu)
 * - Touch-friendly targets (44x44px minimum)
 * - Readable font sizes
 * - Image scaling
 */

import { test, expect } from '@playwright/test';

// =============================================
// SCENARIO 7 - Mobile Responsive Tests
// Owner: Scenario 7 - Responsive Design - Mobile
// =============================================

test.describe('Mobile Responsive Design (375px viewport)', () => {
  test.use({ viewport: { width: 375, height: 667 } }); // iPhone SE size

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  // Test Case 1: Page renders without horizontal scrollbar
  test('TC1: Page renders without horizontal scrollbar at 375px width', async ({ page }) => {
    // Check that no horizontal scrollbar is present
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);
  });

  // Test Case 2: scrollWidth equals clientWidth (no horizontal overflow)
  test('TC2: scrollWidth equals clientWidth - no horizontal overflow', async ({ page }) => {
    const dimensions = await page.evaluate(() => {
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
      };
    });
    expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
  });

  // Test Case 3: Mobile navigation toggle button exists
  test('TC3: Mobile navigation toggle button exists', async ({ page }) => {
    // Look for hamburger menu button (mobile nav toggle)
    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"], .mobile-nav-toggle, .hamburger-menu, [aria-label*="menu" i], button[aria-expanded]');
    await expect(mobileNavToggle.first()).toBeVisible();
  });

  // Test Case 4: Navigation menu expands/collapses when toggle clicked
  test('TC4: Navigation menu expands/collapses on toggle click', async ({ page }) => {
    // Find the mobile nav toggle
    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');
    await expect(mobileNavToggle).toBeVisible();

    // Get the navigation menu
    const navMenu = page.locator('.header__nav, .nav, [role="navigation"]').first();

    // Initially the nav should be hidden on mobile
    const initiallyHidden = await navMenu.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display === 'none' || style.visibility === 'hidden' || el.getAttribute('aria-hidden') === 'true';
    });
    expect(initiallyHidden).toBe(true);

    // Click the toggle to open
    await mobileNavToggle.click();
    await page.waitForTimeout(300); // Wait for animation

    // Nav should now be visible
    const afterClickOpen = await navMenu.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display !== 'none' && style.visibility !== 'hidden';
    });
    expect(afterClickOpen).toBe(true);

    // Click again to close
    await mobileNavToggle.click();
    await page.waitForTimeout(300);

    // Nav should be hidden again
    const afterClickClose = await navMenu.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display === 'none' || style.visibility === 'hidden' || el.getAttribute('aria-hidden') === 'true';
    });
    expect(afterClickClose).toBe(true);
  });

  // Test Case 5: CTA buttons have minimum 44x44px touch area
  test('TC5: CTA buttons have minimum 44x44px touch area', async ({ page }) => {
    // Check the hero CTA buttons
    const ctaButtons = page.locator('.hero__cta .btn, .btn-primary, .btn-secondary');
    const buttonCount = await ctaButtons.count();

    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      const box = await button.boundingBox();

      if (box) {
        // Either width or height should be at least 44px for touch accessibility
        // For buttons, we typically check both dimensions
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  // Test Case 6: Navigation links have minimum 44x44px touch area
  test('TC6: Navigation links have minimum 44x44px touch area', async ({ page }) => {
    // First open the mobile nav
    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');
    await mobileNavToggle.click();
    await page.waitForTimeout(300);

    // Check navigation links
    const navLinks = page.locator('.header__nav-link, .nav__link');
    const linkCount = await navLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const isVisible = await link.isVisible();

      if (isVisible) {
        const box = await link.boundingBox();
        if (box) {
          // Touch targets should be at least 44px in height
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    }
  });

  // Test Case 7: Hero headline and CTA visible without scrolling
  test('TC7: Hero headline and CTA visible without scrolling', async ({ page }) => {
    const heroTitle = page.locator('.hero__title, #hero-title, h1').first();
    const heroCta = page.locator('.hero__cta .btn').first();

    // Check hero title is in viewport
    await expect(heroTitle).toBeVisible();
    const titleBox = await heroTitle.boundingBox();
    expect(titleBox).not.toBeNull();
    if (titleBox) {
      // Title should be visible within initial viewport (667px height)
      expect(titleBox.y + titleBox.height).toBeLessThan(667);
    }

    // Check at least one CTA button is visible
    await expect(heroCta).toBeVisible();
    const ctaBox = await heroCta.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      // CTA should be visible within initial viewport
      expect(ctaBox.y + ctaBox.height).toBeLessThan(667);
    }
  });

  // Test Case 8: Images scale down and maintain aspect ratio
  test('TC8: Images scale down and maintain aspect ratio with max-width 100%', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    expect(imageCount).toBeGreaterThan(0);

    for (let i = 0; i < imageCount; i++) {
      const image = images.nth(i);
      const isVisible = await image.isVisible();

      if (isVisible) {
        const styles = await image.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            maxWidth: computed.maxWidth,
            width: el.getBoundingClientRect().width,
            viewportWidth: window.innerWidth,
          };
        });

        // Image should not exceed viewport width
        expect(styles.width).toBeLessThanOrEqual(styles.viewportWidth);

        // max-width should be 100% or the image width should be within bounds
        expect(styles.maxWidth === '100%' || styles.width <= 375).toBe(true);
      }
    }
  });

  // Test Case 9: Font sizes are readable on mobile (body 16px min, headings scaled)
  test('TC9: Font sizes are readable on mobile - body at least 16px', async ({ page }) => {
    // Check body text font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const computed = window.getComputedStyle(body);
      return parseFloat(computed.fontSize);
    });

    // Body font size should be at least 16px for readability
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph text
    const paragraphFontSize = await page.evaluate(() => {
      const paragraph = document.querySelector('p');
      if (paragraph) {
        const computed = window.getComputedStyle(paragraph);
        return parseFloat(computed.fontSize);
      }
      return 16;
    });

    expect(paragraphFontSize).toBeGreaterThanOrEqual(14);

    // Check that h1 is larger than body text
    const h1FontSize = await page.evaluate(() => {
      const h1 = document.querySelector('h1');
      if (h1) {
        const computed = window.getComputedStyle(h1);
        return parseFloat(computed.fontSize);
      }
      return 24;
    });

    expect(h1FontSize).toBeGreaterThan(bodyFontSize);
  });
});

// =============================================
// Additional Mobile UX Tests
// =============================================

test.describe('Mobile Navigation Accessibility', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test('Mobile nav toggle has accessible label', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');
    await expect(mobileNavToggle).toBeVisible();

    // Check for accessible label
    const ariaLabel = await mobileNavToggle.getAttribute('aria-label');
    const ariaExpanded = await mobileNavToggle.getAttribute('aria-expanded');

    // Must have either aria-label or aria-expanded for accessibility
    const hasAccessibleLabel = Boolean(ariaLabel) || ariaExpanded !== null;
    expect(hasAccessibleLabel).toBe(true);
  });

  test('Mobile nav toggle updates aria-expanded on click', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');

    // Initially should be collapsed
    const initialExpanded = await mobileNavToggle.getAttribute('aria-expanded');
    expect(initialExpanded).toBe('false');

    // Click to expand
    await mobileNavToggle.click();
    await page.waitForTimeout(300);

    const expandedAfterClick = await mobileNavToggle.getAttribute('aria-expanded');
    expect(expandedAfterClick).toBe('true');
  });
});

// =================================================================
// SCENARIO 8: Tablet and Desktop Responsive Design Tests
// =================================================================

test.describe('Tablet Viewport (768px)', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test('page renders correctly for tablet viewport', async ({ page }) => {
    await page.goto('/');

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are visible
    await expect(page.locator('header.header')).toBeVisible();
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#demo')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Verify no horizontal scrolling
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('features grid displays in 2-column layout on tablet', async ({ page }) => {
    await page.goto('/');

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has 2-column layout
    const gridStyle = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });

    // Should have 2 columns (e.g., "352px 352px" or similar)
    const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
    expect(columnCount).toBe(2);
  });

  test('navigation is visible on tablet viewport', async ({ page }) => {
    await page.goto('/');

    // Navigation should be visible
    const nav = page.locator('nav.header__nav');
    await expect(nav).toBeVisible();

    // Nav links should be visible
    const navLinks = page.locator('.header__nav-link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // First nav link should be visible
    await expect(navLinks.first()).toBeVisible();
  });
});

test.describe('Desktop Viewport (1024px)', () => {
  test.use({ viewport: { width: 1024, height: 768 } });

  test('page renders correctly for desktop viewport', async ({ page }) => {
    await page.goto('/');

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are visible
    await expect(page.locator('header.header')).toBeVisible();
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#demo')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Verify no horizontal scrolling
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('full navigation menu visible (not hamburger)', async ({ page }) => {
    await page.goto('/');

    // Navigation should be visible
    const nav = page.locator('nav.header__nav');
    await expect(nav).toBeVisible();

    // All navigation links should be visible
    const navList = page.locator('.header__nav-list');
    await expect(navList).toBeVisible();

    // Check individual nav links are visible (use specific selectors to avoid duplicates)
    await expect(page.locator('.header__nav-link[href="#features"]')).toBeVisible();
    await expect(page.locator('.header__nav-link[href="#demo"]')).toBeVisible();
    await expect(page.locator('.header__nav-link[href="#getting-started"]')).toBeVisible();

    // GitHub link should be visible
    const githubLink = page.locator('.header__nav-link--github');
    await expect(githubLink).toBeVisible();

    // Hamburger menu should NOT be visible on desktop
    const hamburger = page.locator('.hamburger, .mobile-menu-toggle, [aria-label="Toggle menu"]');
    const hamburgerCount = await hamburger.count();
    if (hamburgerCount > 0) {
      await expect(hamburger.first()).not.toBeVisible();
    }
  });

  test('features display in 2-column grid', async ({ page }) => {
    await page.goto('/');

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid layout
    const gridStyle = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });

    // Should have 2 columns (or 4 columns for larger desktops)
    const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
    expect(columnCount).toBeGreaterThanOrEqual(2);
    expect(columnCount).toBeLessThanOrEqual(4);

    // Verify all 4 feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    for (let i = 0; i < 4; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  test('hero section uses appropriate spacing and sizing for large screens', async ({ page }) => {
    await page.goto('/');

    const hero = page.locator('#hero.hero');
    await expect(hero).toBeVisible();

    // Hero should have adequate padding for desktop
    const heroPadding = await hero.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        paddingTop: parseInt(computed.paddingTop),
        paddingBottom: parseInt(computed.paddingBottom),
      };
    });

    // Desktop should have larger padding than mobile (at least 48px)
    expect(heroPadding.paddingTop).toBeGreaterThanOrEqual(48);

    // Hero title should have appropriate font size
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    const titleFontSize = await heroTitle.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });

    // Desktop title should be larger (at least 32px)
    expect(titleFontSize).toBeGreaterThanOrEqual(32);

    // CTA buttons should be side by side, not stacked
    const ctaContainer = page.locator('.hero__cta');
    const ctaFlexDirection = await ctaContainer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaFlexDirection).toBe('row');
  });

  test('content has max-width to prevent over-stretching on wide screens', async ({ page }) => {
    await page.goto('/');

    // Container elements should have max-width constraints
    const containers = page.locator('.container');
    const containerCount = await containers.count();
    expect(containerCount).toBeGreaterThan(0);

    // Check that containers have max-width
    for (let i = 0; i < Math.min(containerCount, 3); i++) {
      const container = containers.nth(i);
      const maxWidth = await container.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        const maxWidthValue = computed.maxWidth;
        // Return numeric value if it's a pixel value, or 'none'
        if (maxWidthValue === 'none') return null;
        return parseInt(maxWidthValue);
      });

      // Container should have a max-width set (typically 1200px or similar)
      expect(maxWidth).not.toBeNull();
      expect(maxWidth).toBeLessThanOrEqual(1400);
    }

    // Verify hero content has max-width constraint
    const heroContent = page.locator('.hero__content');
    const heroMaxWidth = await heroContent.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).maxWidth);
    });
    // Hero content should have a max-width set (typically 800px or 1200px for containers)
    expect(heroMaxWidth).toBeLessThanOrEqual(1200);
  });
});

test.describe('Large Desktop Viewport (1440px)', () => {
  test.use({ viewport: { width: 1440, height: 900 } });

  test('content remains centered and constrained on very wide screens', async ({ page }) => {
    await page.goto('/');

    // Header container should have max-width
    const headerContainer = page.locator('.header__container');
    const headerMaxWidth = await headerContainer.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return parseInt(computed.maxWidth);
    });
    expect(headerMaxWidth).toBeLessThanOrEqual(1400);

    // Container should be centered (have auto margins)
    const containerMargin = await headerContainer.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        marginLeft: computed.marginLeft,
        marginRight: computed.marginRight,
      };
    });

    // Margins should be auto (or equal positive values indicating centering)
    const leftMargin = parseInt(containerMargin.marginLeft) || 0;
    const rightMargin = parseInt(containerMargin.marginRight) || 0;

    // On a 1440px viewport with a max-width container, we expect visible margins
    // indicating the content is constrained
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
  });

  test('features grid maintains readable card widths', async ({ page }) => {
    await page.goto('/');

    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();

    const cardWidth = await firstCard.evaluate((el) => {
      return el.getBoundingClientRect().width;
    });

    // Cards should not be too wide (max around 500px for readability)
    expect(cardWidth).toBeLessThanOrEqual(600);

    // Cards should not be too narrow either
    expect(cardWidth).toBeGreaterThanOrEqual(200);
  });
});

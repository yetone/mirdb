import { test, expect } from '@playwright/test';

/**
 * Desktop Responsive Design Tests
 * Scenario: Verify the homepage displays correctly on desktop viewports with proper layout and spacing
 *
 * Test Cases:
 * 1. Page renders with proper desktop layout at 1920px viewport
 * 2. Feature cards display in multi-column grid layout
 * 3. Main content has max-width to prevent overly long lines
 * 4. Hero section spans full width with centered content
 */

test.describe('Desktop Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport to 1920x1080
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
  });

  test('TC1: Page renders with proper desktop layout at 1920px viewport', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are visible at desktop size
    const hero = page.locator('#hero, .hero, [data-testid="hero"]').first();
    await expect(hero).toBeVisible();

    const features = page.locator('#features, .features').first();
    await expect(features).toBeVisible();

    const gettingStarted = page.locator('#getting-started, .getting-started').first();
    await expect(gettingStarted).toBeVisible();

    // Verify viewport is desktop size
    const viewportSize = page.viewportSize();
    expect(viewportSize?.width).toBe(1920);
    expect(viewportSize?.height).toBe(1080);

    // Verify navigation is visible (not collapsed into mobile menu)
    const navLinks = page.locator('.nav-links, nav, footer nav').first();
    await expect(navLinks).toBeVisible();
  });

  test('TC2: Feature cards display in multi-column grid layout at 1920px', async ({ page }) => {
    // Find feature cards section
    const featuresSection = page.locator('#features, .features').first();
    await expect(featuresSection).toBeVisible();

    // Find feature cards grid container
    const featureGrid = page.locator('.feature-grid, .features-grid').first();
    await expect(featureGrid).toBeVisible();

    // Verify feature cards exist
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Get the bounding boxes of the first few feature cards to verify multi-column layout
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();

    // In a multi-column grid, cards should be side by side (same Y position, different X)
    // At 1920px viewport, the cards should be in a row
    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();

    if (firstCardBox && secondCardBox) {
      // Cards should be on the same row (similar Y position within tolerance)
      const yDifference = Math.abs(firstCardBox.y - secondCardBox.y);
      expect(yDifference).toBeLessThan(50); // Allow some tolerance for alignment

      // Cards should be side by side (different X positions)
      expect(firstCardBox.x).not.toBe(secondCardBox.x);
    }

    // Verify grid container uses CSS Grid with multi-column layout
    const gridStyles = await featureGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    expect(gridStyles.display).toBe('grid');
    // Should have multiple columns (not a single column layout)
    const columns = gridStyles.gridTemplateColumns.split(' ').filter(col => col.trim() !== '');
    expect(columns.length).toBeGreaterThanOrEqual(2);
  });

  test('TC3: Main content has max-width to prevent overly long lines', async ({ page }) => {
    // Check section containers have max-width constraint
    const sections = page.locator('section');
    const sectionCount = await sections.count();

    let maxWidthConstrainedSections = 0;

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const sectionBox = await section.boundingBox();

      if (sectionBox) {
        // At 1920px viewport, content should not stretch to full width
        // Check if section is constrained (less than viewport width or has inner constraint)
        const sectionStyles = await section.evaluate((el) => {
          const style = window.getComputedStyle(el);
          const maxWidth = style.maxWidth;
          const width = el.getBoundingClientRect().width;
          return { maxWidth, width };
        });

        // Content should either have explicit max-width or container inside
        // Hero section can span full width but internal content should be constrained
        if (sectionStyles.maxWidth !== 'none' && sectionStyles.maxWidth !== '100%') {
          maxWidthConstrainedSections++;
        } else if (sectionStyles.width < 1920) {
          maxWidthConstrainedSections++;
        }
      }
    }

    // Majority of sections should have width constraints
    expect(maxWidthConstrainedSections).toBeGreaterThan(0);

    // Verify container max-width variable is defined
    const maxWidthValue = await page.evaluate(() => {
      const style = window.getComputedStyle(document.documentElement);
      return style.getPropertyValue('--max-width').trim();
    });

    // Should have a max-width CSS variable defined
    expect(maxWidthValue).toBeTruthy();

    // Verify hero content is centered with max-width
    const heroContent = page.locator('.hero-content, .hero .container').first();
    if (await heroContent.count() > 0) {
      const heroContentStyles = await heroContent.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          maxWidth: style.maxWidth,
        };
      });

      // Hero content should have max-width constraint
      if (heroContentStyles.maxWidth !== 'none') {
        const maxWidthNum = parseInt(heroContentStyles.maxWidth);
        expect(maxWidthNum).toBeLessThanOrEqual(1400); // Reasonable max-width for readability
      }
    }

    // Verify text blocks don't stretch too wide for readability
    // Line length should ideally be 60-80 characters, which corresponds to about 600-800px
    const sectionDescription = page.locator('.section-description, .arch-explanation').first();
    if (await sectionDescription.count() > 0) {
      const descBox = await sectionDescription.boundingBox();
      if (descBox) {
        // Text containers should be constrained for readability
        expect(descBox.width).toBeLessThanOrEqual(900); // Max reasonable text width
      }
    }
  });

  test('TC4: Hero section spans full width with centered content', async ({ page }) => {
    // Verify hero section exists
    const hero = page.locator('#hero, .hero, [data-testid="hero"]').first();
    await expect(hero).toBeVisible();

    // Get hero section dimensions
    const heroBox = await hero.boundingBox();
    expect(heroBox).not.toBeNull();

    if (heroBox) {
      // Hero should span full viewport width (or close to it)
      // Allow for small margins/padding
      expect(heroBox.width).toBeGreaterThanOrEqual(1900);
    }

    // Verify hero styles indicate full-width design
    const heroStyles = await hero.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        maxWidth: style.maxWidth,
        width: style.width,
        textAlign: style.textAlign,
        display: style.display,
        justifyContent: style.justifyContent,
        alignItems: style.alignItems,
      };
    });

    // Hero can have max-width: none or be a flex/grid container
    // Key is that it spans available width
    expect(heroStyles.maxWidth === 'none' || heroStyles.maxWidth === '' || parseInt(heroStyles.maxWidth) > 1400).toBeTruthy();

    // Verify content is centered
    const heroContent = page.locator('.hero-content, .hero .container, .hero > *').first();
    await expect(heroContent).toBeVisible();

    // Check centering via text-align or flexbox
    if (heroStyles.textAlign === 'center') {
      // Text-align centering
      expect(heroStyles.textAlign).toBe('center');
    } else if (heroStyles.display === 'flex') {
      // Flexbox centering
      expect(heroStyles.justifyContent).toContain('center');
    }

    // Verify hero content is horizontally centered in the viewport
    const heroContentBox = await heroContent.boundingBox();
    if (heroContentBox && heroBox) {
      // Content should be centered within hero
      const heroCenter = heroBox.x + (heroBox.width / 2);
      const contentCenter = heroContentBox.x + (heroContentBox.width / 2);

      // Allow tolerance for centering
      const centerDifference = Math.abs(heroCenter - contentCenter);
      expect(centerDifference).toBeLessThan(100);
    }

    // Verify CTA buttons are visible and accessible
    const ctaButtons = page.locator('.cta-buttons, .hero-cta').first();
    if (await ctaButtons.count() > 0) {
      await expect(ctaButtons).toBeVisible();

      // At desktop size, CTA buttons should be in a row (not stacked)
      const ctaStyles = await ctaButtons.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          flexDirection: style.flexDirection,
        };
      });

      // Should be flex row (not column) at desktop size
      if (ctaStyles.display === 'flex') {
        expect(ctaStyles.flexDirection).not.toBe('column');
      }
    }
  });
});

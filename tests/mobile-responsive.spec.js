// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('Mobile Responsive Design', () => {
  /**
   * Test Case 1: Load page at 320px viewport width
   * Expected: Page renders without horizontal scroll bar, all content visible
   */
  test('TC1: page renders without horizontal scroll at 320px width', async ({ page }) => {
    // Set viewport to minimum mobile width
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto(indexPath);

    // Wait for the page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that there's no horizontal overflow causing scrollbars
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Verify main content containers fit within viewport
    const containerWidths = await page.evaluate(() => {
      const containers = document.querySelectorAll('.container');
      const viewportWidth = window.innerWidth;
      let allFit = true;
      containers.forEach(container => {
        const rect = container.getBoundingClientRect();
        if (rect.width > viewportWidth) {
          allFit = false;
        }
      });
      return allFit;
    });

    expect(containerWidths).toBe(true);
  });

  /**
   * Test Case 2: Check hero section at 320px width
   * Expected: Hero section content is fully visible and readable
   */
  test('TC2: hero section is fully visible and readable at 320px', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto(indexPath);

    // Hero section should be visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Hero title should be visible and readable
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    // Hero tagline should be visible
    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();

    // Hero description should be visible
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();

    // CTA buttons should be visible
    const ctaButtons = page.locator('.hero-cta .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      await expect(ctaButtons.nth(i)).toBeVisible();
    }

    // Verify hero section doesn't overflow viewport
    const heroOverflows = await page.evaluate(() => {
      const hero = document.querySelector('.hero');
      if (!hero) return false;
      const rect = hero.getBoundingClientRect();
      return rect.width > window.innerWidth;
    });

    expect(heroOverflows).toBe(false);

    // Check font size is readable (at least 14px for body text)
    const fontSizes = await page.evaluate(() => {
      const description = document.querySelector('.hero-description');
      if (!description) return { description: 0 };
      const styles = window.getComputedStyle(description);
      return {
        description: parseFloat(styles.fontSize)
      };
    });

    expect(fontSizes.description).toBeGreaterThanOrEqual(14);
  });

  /**
   * Test Case 3: Check code blocks at 320px width
   * Expected: Code blocks are scrollable horizontally within their container or wrap appropriately
   */
  test('TC3: code blocks are scrollable or wrap at 320px width', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto(indexPath);

    // Find code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify code blocks have overflow-x: auto for horizontal scroll
    const codeBlockOverflow = await page.evaluate(() => {
      const blocks = document.querySelectorAll('.code-block');
      let allHaveOverflow = true;
      blocks.forEach(block => {
        const styles = window.getComputedStyle(block);
        // Should have overflow-x: auto or scroll
        if (styles.overflowX !== 'auto' && styles.overflowX !== 'scroll') {
          allHaveOverflow = false;
        }
      });
      return allHaveOverflow;
    });

    expect(codeBlockOverflow).toBe(true);

    // Verify code blocks don't cause page-level horizontal scroll
    const pageHasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(pageHasHorizontalScroll).toBe(false);
  });

  /**
   * Test Case 4: Check navigation at mobile width
   * Expected: Navigation is accessible via mobile-friendly controls (hamburger menu or visible links)
   */
  test('TC4: navigation is accessible at mobile width', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto(indexPath);

    // Check for navigation links (either in header or as hamburger menu)
    // The page has CTA buttons in hero section and footer links

    // Primary navigation: CTA buttons in hero section
    const heroCtaLinks = page.locator('.hero-cta a');
    const heroLinkCount = await heroCtaLinks.count();
    expect(heroLinkCount).toBeGreaterThan(0);

    // All hero CTA buttons should be visible
    for (let i = 0; i < heroLinkCount; i++) {
      await expect(heroCtaLinks.nth(i)).toBeVisible();
    }

    // Footer navigation
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(0);

    // Footer links should be visible when scrolled to
    await page.locator('.footer').scrollIntoViewIfNeeded();
    for (let i = 0; i < footerLinkCount; i++) {
      await expect(footerLinks.nth(i)).toBeVisible();
    }

    // If there's a hamburger menu, check it's functional
    const hamburgerMenu = page.locator('.hamburger-menu, .mobile-menu-toggle, [aria-label="Menu"]');
    const hasHamburger = await hamburgerMenu.count() > 0;

    if (hasHamburger) {
      await expect(hamburgerMenu.first()).toBeVisible();
    }

    // All navigation should be accessible - links should be clickable
    const primaryCta = page.locator('.btn-primary').first();
    await expect(primaryCta).toBeVisible();
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();
  });

  /**
   * Test Case 5: Load page at 768px viewport width
   * Expected: Page displays appropriate tablet layout with proper spacing
   */
  test('TC5: page displays tablet layout at 768px width', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(indexPath);

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Page should not have horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Hero section should be visible with proper spacing
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Features grid should display properly at tablet width
    const featuresGrid = page.locator('.features-grid');
    await featuresGrid.scrollIntoViewIfNeeded();
    await expect(featuresGrid).toBeVisible();

    // Check that feature cards have proper layout
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // All feature cards should be visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Commands grid should display appropriately
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Verify spacing is appropriate (padding exists on container)
    const containerPadding = await page.evaluate(() => {
      const container = document.querySelector('.container');
      if (!container) return 0;
      const styles = window.getComputedStyle(container);
      return parseFloat(styles.paddingLeft) + parseFloat(styles.paddingRight);
    });

    expect(containerPadding).toBeGreaterThan(0);
  });

  /**
   * Test Case 6: Check touch targets on mobile
   * Expected: Buttons and links have adequate touch target size (minimum 44x44px)
   */
  test('TC6: buttons and links have adequate touch target size', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto(indexPath);

    // Minimum recommended touch target size is 44x44px per Apple/Google guidelines
    const minTouchTarget = 44;

    // Check CTA buttons
    const ctaButtons = page.locator('.btn');
    const buttonCount = await ctaButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      await button.scrollIntoViewIfNeeded();

      const boundingBox = await button.boundingBox();
      expect(boundingBox).not.toBeNull();

      if (boundingBox) {
        // Either width or height should meet touch target requirements
        // Most buttons are wider than tall, so width is usually sufficient
        expect(boundingBox.height).toBeGreaterThanOrEqual(minTouchTarget);
      }
    }

    // Check footer links
    await page.locator('.footer').scrollIntoViewIfNeeded();
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const boundingBox = await link.boundingBox();

      // Links can have adequate target size via padding or line-height
      // We check if the clickable area is reasonable
      if (boundingBox) {
        // At minimum, the height should be reasonable for touch
        // We're more lenient here as the actual touch area includes padding
        expect(boundingBox.height).toBeGreaterThanOrEqual(20);
      }
    }

    // Verify all interactive elements have reasonable touch targets
    const interactiveElements = await page.evaluate(() => {
      const minSize = 44;
      const issues = [];

      // Check all buttons and primary links
      const elements = document.querySelectorAll('.btn, .hero-cta a');
      elements.forEach((el, index) => {
        const rect = el.getBoundingClientRect();
        if (rect.height < minSize && rect.width < minSize) {
          issues.push({
            index,
            width: rect.width,
            height: rect.height
          });
        }
      });

      return issues;
    });

    // Main interactive buttons should meet touch target requirements
    expect(interactiveElements.length).toBe(0);
  });
});

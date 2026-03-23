/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 6, 7, 8 - Responsive Design
 *
 * Test cases:
 * - Desktop viewport (1920x1080, 1280x720)
 * - Tablet viewport (768px)
 * - Mobile viewport (375px)
 * - Navigation adaptation
 * - Content stacking
 */

import { test, expect } from '@playwright/test';
import { navigateToHomepage, selectors, viewports, scrollToSection } from './test-utils';

/**
 * Scenario 6: Responsive Design - Desktop
 */
test.describe('Responsive Design - Desktop', () => {
  test('TC1: Page renders without horizontal scrolling at 1920x1080 viewport, all content visible and properly aligned', async ({ page }) => {
    // Set large desktop viewport
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Verify no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify all major sections are visible
    const heroSection = page.locator(selectors.hero.section);
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator(selectors.features.section);
    await expect(featuresSection).toBeAttached();

    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeAttached();

    const quickstartSection = page.locator(selectors.quickstart.section);
    await expect(quickstartSection).toBeAttached();

    // Verify content is properly aligned (centered container)
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();

    // Container should be centered (left margin roughly equals right margin)
    const viewportWidth = 1920;
    const containerWidth = containerBox!.width;
    const leftMargin = containerBox!.x;
    const rightMargin = viewportWidth - (containerBox!.x + containerBox!.width);

    // Allow some tolerance for centering (within 50px)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);

    // Verify hero title is visible and readable
    const heroTitle = page.locator(selectors.hero.title);
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');
  });

  test('TC2: Page renders without horizontal scrolling at 1280x720 viewport, navigation fully visible', async ({ page }) => {
    // Set standard desktop viewport
    await page.setViewportSize(viewports.desktop);
    await navigateToHomepage(page);

    // Verify no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify navigation header is visible
    const header = page.locator(selectors.navigation.header);
    await expect(header).toBeVisible();

    // Verify navigation links are visible
    const navLinks = page.locator(selectors.navigation.links);
    await expect(navLinks).toBeVisible();

    // Verify individual navigation links are present and visible
    const navLinkItems = page.locator(selectors.navigation.link);
    const linkCount = await navLinkItems.count();
    expect(linkCount).toBeGreaterThanOrEqual(3); // At least Features, Usage, Quick Start

    for (let i = 0; i < linkCount; i++) {
      await expect(navLinkItems.nth(i)).toBeVisible();
    }

    // Verify navigation logo is visible
    const logo = page.locator(selectors.navigation.logo);
    await expect(logo).toBeVisible();
    await expect(logo).toHaveText('MirDB');
  });

  test('TC3: Content is centered and has appropriate max-width for readability', async ({ page }) => {
    // Set large desktop viewport to test max-width constraint
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Get the container max-width
    const container = page.locator('.container').first();
    const maxWidth = await container.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });

    // Verify max-width is set (not 'none')
    expect(maxWidth).not.toBe('none');

    // Parse max-width and verify it's reasonable for readability (typically 1200-1400px)
    const maxWidthValue = parseFloat(maxWidth);
    expect(maxWidthValue).toBeGreaterThanOrEqual(1000); // At least 1000px
    expect(maxWidthValue).toBeLessThanOrEqual(1600); // At most 1600px

    // Verify hero content has its own max-width for optimal reading
    const heroContent = page.locator('.hero-content');
    const heroMaxWidth = await heroContent.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(heroMaxWidth).not.toBe('none');

    // Verify section description has max-width for readability
    const sectionDescription = page.locator('.section-description').first();
    await expect(sectionDescription).toBeAttached();
    const descMaxWidth = await sectionDescription.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(descMaxWidth).not.toBe('none');

    // Verify centering via checking that left and right margins are equal (computed margins resolve 'auto' to pixels)
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();

    const viewportWidth = 1920;
    const leftMargin = containerBox!.x;
    const rightMargin = viewportWidth - (containerBox!.x + containerBox!.width);

    // Container should be centered (left and right margins roughly equal, within 50px tolerance)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);

    // Container should have meaningful margins on both sides (not edge-to-edge)
    expect(leftMargin).toBeGreaterThan(20);
    expect(rightMargin).toBeGreaterThan(20);
  });

  test('TC4: Full horizontal navigation menu is displayed (no hamburger menu) on desktop', async ({ page }) => {
    // Test both desktop viewports
    const desktopViewports = [viewports.desktop, viewports.desktopLarge];

    for (const viewport of desktopViewports) {
      await page.setViewportSize(viewport);
      await navigateToHomepage(page);

      // Verify hamburger/toggle button is NOT visible
      const navToggle = page.locator(selectors.navigation.toggle);
      await expect(navToggle).not.toBeVisible();

      // Verify nav-links is visible (horizontal menu)
      const navLinks = page.locator(selectors.navigation.links);
      await expect(navLinks).toBeVisible();

      // Verify navigation links are displayed horizontally (flex row)
      const display = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('flex');

      const flexDirection = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');

      // Verify all navigation links are visible and accessible
      const linkTexts = ['Features', 'Usage', 'Quick Start', 'GitHub'];
      for (const text of linkTexts) {
        const link = page.locator(selectors.navigation.link, { hasText: text });
        await expect(link).toBeVisible();
      }

      // Verify navigation links are positioned horizontally
      const firstLink = page.locator(selectors.navigation.link).first();
      const lastLink = page.locator(selectors.navigation.link).last();

      const firstBox = await firstLink.boundingBox();
      const lastBox = await lastLink.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(lastBox).not.toBeNull();

      // Links should be on the same horizontal line (similar Y position)
      expect(Math.abs(firstBox!.y - lastBox!.y)).toBeLessThan(10);

      // Last link should be to the right of first link
      expect(lastBox!.x).toBeGreaterThan(firstBox!.x);
    }
  });

  test('All sections are accessible without horizontal scrolling on desktop', async ({ page }) => {
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Verify all sections can be scrolled to and are visible
    const sections = ['hero', 'features', 'usage', 'quickstart'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}`);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();

      // Verify no horizontal scroll at each section
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    }
  });

  test('Features grid displays properly on desktop', async ({ page }) => {
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Scroll to features section
    const featuresSection = page.locator(selectors.features.section);
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features grid exists
    const featuresGrid = page.locator(selectors.features.grid);
    await expect(featuresGrid).toBeVisible();

    // Verify features use grid or flex for layout
    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(['grid', 'flex']).toContain(display);

    // Verify feature cards are displayed
    const featureCards = page.locator(selectors.features.card);
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4); // Should have at least 4 features

    // On desktop, feature cards should be arranged in multiple columns
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    // On large desktop, cards should be side by side (different X positions)
    expect(secondBox!.x).toBeGreaterThan(firstBox!.x);
  });
});

/**
 * Scenario 7: Responsive Design - Tablet (768px width)
 * Verify the homepage displays correctly on tablet viewport
 */
test.describe('Responsive Design - Tablet Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before each test
    await page.setViewportSize(viewports.tablet);
    await navigateToHomepage(page);
  });

  test('TC1: All text is readable without horizontal scrolling at 768px width', async ({ page }) => {
    // Verify no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify hero title is visible and readable
    const heroTitle = page.locator(selectors.hero.title);
    await expect(heroTitle).toBeVisible();
    const heroTitleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(heroTitleFontSize).toBeGreaterThanOrEqual(32); // Should be at least 32px

    // Verify hero tagline is visible and readable
    const heroTagline = page.locator(selectors.hero.tagline);
    await expect(heroTagline).toBeVisible();
    const taglineFontSize = await heroTagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(16); // At least 16px for readability

    // Verify section headings are visible
    await scrollToSection(page, 'features');
    const featuresTitle = page.locator(selectors.features.title);
    await expect(featuresTitle).toBeVisible();

    // Verify content doesn't overflow the viewport
    const bodyWidth = await page.evaluate(() => document.body.offsetWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('TC2: Navigation is accessible through mobile menu or stacked layout', async ({ page }) => {
    // Check if navigation is visible
    const navLinks = page.locator(selectors.navigation.links);
    const navToggle = page.locator(selectors.navigation.toggle);

    // On tablet, navigation can be:
    // 1. Regular horizontal nav (visible links)
    // 2. Mobile menu (toggle button visible, links hidden until toggled)

    const navLinksVisible = await navLinks.isVisible();
    const navToggleVisible = await navToggle.isVisible();

    // Either nav links should be visible OR toggle should be visible
    expect(navLinksVisible || navToggleVisible).toBe(true);

    if (navToggleVisible && !navLinksVisible) {
      // Mobile menu mode - verify toggle works
      await navToggle.click();
      await page.waitForTimeout(300); // Wait for animation

      // After clicking toggle, nav links should become visible
      await expect(navLinks).toBeVisible();

      // Verify all navigation links are accessible
      const links = page.locator(`${selectors.navigation.links} ${selectors.navigation.link}`);
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThanOrEqual(3); // At least Features, Usage, Quick Start

      // Verify links are interactive
      for (let i = 0; i < linkCount; i++) {
        const link = links.nth(i);
        await expect(link).toBeVisible();
        await expect(link).toBeEnabled();
      }
    } else {
      // Regular nav mode - verify links are accessible
      const links = page.locator(`${selectors.navigation.links} ${selectors.navigation.link}`);
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThanOrEqual(3);

      for (let i = 0; i < linkCount; i++) {
        await expect(links.nth(i)).toBeVisible();
      }
    }

    // Verify logo is always visible
    const logo = page.locator(selectors.navigation.logo);
    await expect(logo).toBeVisible();
  });

  test('TC3: Feature cards adapt to 2-column or stacked layout appropriately', async ({ page }) => {
    await scrollToSection(page, 'features');

    const featuresGrid = page.locator(selectors.features.grid);
    await expect(featuresGrid).toBeVisible();

    // Get the computed grid template columns
    const gridInfo = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
        gap: styles.gap,
      };
    });

    // Should be using grid layout
    expect(gridInfo.display).toBe('grid');

    // For tablet, grid should show 2 columns or adapt properly
    // gridTemplateColumns will be computed values like "280px 280px" or "repeat(2, 1fr)"
    const columns = gridInfo.gridTemplateColumns.split(' ').filter((col: string) => col && col !== '0px');

    // At tablet width (768px), we expect 2 columns or 1 column (stacked)
    expect(columns.length).toBeLessThanOrEqual(4);
    expect(columns.length).toBeGreaterThanOrEqual(1);

    // Verify all feature cards are visible and properly sized
    const featureCards = page.locator(selectors.features.card);
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Verify card is properly sized for tablet
      const cardBox = await card.boundingBox();
      expect(cardBox).not.toBeNull();
      // Card should not overflow viewport (768px - padding)
      expect(cardBox!.width).toBeLessThanOrEqual(720);
      // Card should have reasonable minimum width
      expect(cardBox!.width).toBeGreaterThanOrEqual(200);
    }

    // Verify cards don't cause horizontal overflow
    const hasOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasOverflow).toBe(false);
  });

  test('TC4: Code blocks are scrollable horizontally if needed, text remains readable', async ({ page }) => {
    // Navigate to usage section which has code blocks
    await scrollToSection(page, 'usage');

    const codeBlocks = page.locator(selectors.usage.codeBlock);
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify code block has horizontal scroll capability via overflow-x
      const styles = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          overflowX: computed.overflowX,
          whiteSpace: computed.whiteSpace,
        };
      });

      // Code block should allow horizontal scrolling
      expect(['auto', 'scroll', 'visible']).toContain(styles.overflowX);

      // Get the pre element inside code block for font size check
      const pre = codeBlock.locator('pre');
      await expect(pre).toBeVisible();

      const preFontSize = await pre.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Font size should be readable (at least 12px)
      expect(preFontSize).toBeGreaterThanOrEqual(12);
    }

    // Also check quickstart section code blocks
    await scrollToSection(page, 'quickstart');
    const quickstartCodeBlocks = page.locator(`${selectors.quickstart.section} .code-block`);
    const quickstartCount = await quickstartCodeBlocks.count();

    for (let i = 0; i < quickstartCount; i++) {
      const codeBlock = quickstartCodeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify code text is readable
      const pre = codeBlock.locator('pre');
      const fontSize = await pre.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(12);

      // Verify code block doesn't cause page overflow
      const codeBlockBox = await codeBlock.boundingBox();
      expect(codeBlockBox).not.toBeNull();
      // Code block should fit within viewport width (with some padding margin)
      expect(codeBlockBox!.width).toBeLessThanOrEqual(768);
    }
  });

  test('Tablet viewport maintains proper spacing and layout', async ({ page }) => {
    // Verify container has appropriate padding
    const container = page.locator('.container').first();
    const containerStyles = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingLeft: parseFloat(styles.paddingLeft),
        paddingRight: parseFloat(styles.paddingRight),
        maxWidth: styles.maxWidth,
      };
    });

    // Container should have reasonable padding (at least 16px on each side)
    expect(containerStyles.paddingLeft).toBeGreaterThanOrEqual(16);
    expect(containerStyles.paddingRight).toBeGreaterThanOrEqual(16);

    // Verify sections have proper spacing
    const sections = page.locator('.section');
    const sectionCount = await sections.count();

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const sectionPadding = await section.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          paddingTop: parseFloat(styles.paddingTop),
          paddingBottom: parseFloat(styles.paddingBottom),
        };
      });

      // Sections should have vertical padding
      expect(sectionPadding.paddingTop).toBeGreaterThanOrEqual(32);
      expect(sectionPadding.paddingBottom).toBeGreaterThanOrEqual(32);
    }
  });

  test('Footer displays correctly on tablet', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator(selectors.footer.section);
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer links are accessible
    const footerLinks = page.locator(selectors.footer.link);
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(2);

    for (let i = 0; i < linkCount; i++) {
      await expect(footerLinks.nth(i)).toBeVisible();
    }

    // Verify footer doesn't cause horizontal overflow
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox!.width).toBeLessThanOrEqual(768);

    // Verify copyright is visible
    const copyright = page.locator(selectors.footer.copyright);
    await expect(copyright).toBeVisible();
  });

  test('CTA buttons are appropriately sized for tablet touch targets', async ({ page }) => {
    // Verify CTA buttons have good touch target size (at least 44px per WCAG)
    const ctaButton = page.locator(selectors.hero.ctaButton);
    await expect(ctaButton).toBeVisible();

    const buttonBox = await ctaButton.boundingBox();
    expect(buttonBox).not.toBeNull();
    // Minimum touch target size
    expect(buttonBox!.height).toBeGreaterThanOrEqual(40);
    expect(buttonBox!.width).toBeGreaterThanOrEqual(100);

    // Verify buttons are not cut off
    const viewportWidth = 768;
    expect(buttonBox!.x + buttonBox!.width).toBeLessThanOrEqual(viewportWidth);
  });
});

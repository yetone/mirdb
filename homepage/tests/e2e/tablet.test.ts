/**
 * E2E tests for tablet responsive design (768px-1023px).
 * Owner: Scenario 8 - Responsive Design - Tablet
 *
 * Tests:
 * - Homepage renders correctly at 768px viewport width
 * - Layout uses tablet-specific breakpoint styles
 * - Features grid displays in 2-column layout
 * - Hero content is centered and readable, CTAs properly sized
 * - All sections are properly formatted at tablet width
 */

import { test, expect } from '@playwright/test';

// Tablet viewport sizes
const TABLET_MIN = { width: 768, height: 1024 }; // Minimum tablet width (iPad portrait)
const TABLET_LANDSCAPE = { width: 1023, height: 768 }; // Max tablet width before desktop

test.describe('Responsive Design - Tablet (768px-1023px)', () => {
  test.describe('Test Case 1: 768px Viewport Width - Layout Uses Tablet Breakpoint Styles', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_MIN);
      await page.goto('/mirdb/');
    });

    test('homepage renders correctly at 768px tablet width', async ({ page }) => {
      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Verify all main sections are visible and can be scrolled to
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];
      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Verify footer is accessible
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('no horizontal scrollbar at 768px tablet viewport', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('tablet layout adapts differently from mobile (uses md: breakpoint)', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // At 768px (md breakpoint), the layout should differ from mobile
      // Features grid should have 2 columns instead of 1
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      await expect(featuresGrid).toBeVisible();

      // Get grid computed styles to verify 2-column layout
      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          gridTemplateColumns: style.gridTemplateColumns
        };
      });

      expect(gridStyle.display).toBe('grid');
      // At md breakpoint (768px), should have 2 columns
      // gridTemplateColumns will show actual pixel values for 2 columns
      const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
      expect(columnCount).toBe(2);
    });

    test('all sections have proper padding at tablet width', async ({ page }) => {
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();

        const sectionBox = await section.boundingBox();
        expect(sectionBox).toBeTruthy();
        // Section should use most of the viewport width with some padding
        expect(sectionBox!.width).toBeLessThanOrEqual(768);
        expect(sectionBox!.width).toBeGreaterThan(600); // Not too narrow
      }
    });
  });

  test.describe('Test Case 2: Features Grid 2-Column Layout', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_MIN);
      await page.goto('/mirdb/');
    });

    test('features display in 2-column grid layout at tablet width', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Get the features grid
      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      await expect(featuresGrid).toBeVisible();

      // Get all feature cards
      const featureCards = featuresGrid.locator('> *');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(6); // Should have 6 feature cards

      // Verify 2-column layout by checking card positions
      // Cards should be arranged in pairs horizontally
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();
      const thirdCardBox = await featureCards.nth(2).boundingBox();

      expect(firstCardBox).toBeTruthy();
      expect(secondCardBox).toBeTruthy();
      expect(thirdCardBox).toBeTruthy();

      // First and second cards should be on the same row (similar Y position)
      expect(Math.abs(firstCardBox!.y - secondCardBox!.y)).toBeLessThan(10);

      // First and second cards should have different X positions (side by side)
      expect(secondCardBox!.x).toBeGreaterThan(firstCardBox!.x);

      // Third card should be on a different row (different Y position from first)
      expect(thirdCardBox!.y).toBeGreaterThan(firstCardBox!.y + firstCardBox!.height / 2);
    });

    test('feature cards have appropriate width for 2-column layout', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      const featureCards = featuresGrid.locator('> *');

      const firstCard = featureCards.first();
      const cardBox = await firstCard.boundingBox();
      expect(cardBox).toBeTruthy();

      // Each card should be roughly half the grid width (accounting for gap)
      // At 768px viewport with container padding, each card should be ~300-350px wide
      expect(cardBox!.width).toBeGreaterThan(250);
      expect(cardBox!.width).toBeLessThan(400);
    });

    test('feature card content is readable at tablet width', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Check feature titles are visible
      const featureTitles = page.locator('#features h3');
      const titleCount = await featureTitles.count();
      expect(titleCount).toBeGreaterThan(0);

      for (let i = 0; i < Math.min(titleCount, 3); i++) {
        await expect(featureTitles.nth(i)).toBeVisible();
      }

      // Check feature descriptions are visible
      const featureDescriptions = page.locator('#features .text-gray-600, #features .dark\\:text-gray-400');
      const descCount = await featureDescriptions.count();
      expect(descCount).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 3: Hero Section at Tablet Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_MIN);
      await page.goto('/mirdb/');
    });

    test('hero content is centered and readable at tablet width', async ({ page }) => {
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Logo should be visible and appropriately sized
      const logo = page.locator('[data-testid="hero-logo"] img');
      await expect(logo).toBeVisible();
      const logoBox = await logo.boundingBox();
      expect(logoBox).toBeTruthy();
      expect(logoBox!.width).toBeLessThanOrEqual(768);

      // Check logo is centered (approximately)
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      const logoCenter = logoBox!.x + logoBox!.width / 2;
      const heroCenter = heroBox!.x + heroBox!.width / 2;
      // Logo should be within 50px of center
      expect(Math.abs(logoCenter - heroCenter)).toBeLessThan(50);
    });

    test('hero tagline is properly sized and readable', async ({ page }) => {
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();

      const taglineBox = await tagline.boundingBox();
      expect(taglineBox).toBeTruthy();
      expect(taglineBox!.width).toBeGreaterThan(300); // Wide enough to read
      expect(taglineBox!.width).toBeLessThanOrEqual(768);

      // Verify tagline text is complete
      const taglineText = await tagline.textContent();
      expect(taglineText).toContain('Persistent');
      expect(taglineText).toContain('Key-Value');
    });

    test('CTA buttons are properly sized and positioned at tablet width', async ({ page }) => {
      const ctaContainer = page.locator('[data-testid="hero-cta-buttons"]');
      await expect(ctaContainer).toBeVisible();

      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      const githubButton = page.locator('[data-testid="github-button"]');

      await expect(getStartedButton).toBeVisible();
      await expect(githubButton).toBeVisible();

      const getStartedBox = await getStartedButton.boundingBox();
      const githubBox = await githubButton.boundingBox();

      expect(getStartedBox).toBeTruthy();
      expect(githubBox).toBeTruthy();

      // Buttons should have adequate size for tapping
      expect(getStartedBox!.width).toBeGreaterThanOrEqual(100);
      expect(getStartedBox!.height).toBeGreaterThanOrEqual(44);
      expect(githubBox!.width).toBeGreaterThanOrEqual(100);
      expect(githubBox!.height).toBeGreaterThanOrEqual(44);

      // At tablet width (sm: breakpoint 640px is passed), buttons should be in a row
      // since we're at 768px which is above sm (640px)
      // flex-col sm:flex-row means at 768px they should be horizontal
      expect(Math.abs(getStartedBox!.y - githubBox!.y)).toBeLessThan(10);
    });

    test('hero terminal preview displays correctly at tablet width', async ({ page }) => {
      const terminal = page.locator('#hero .terminal');
      if (await terminal.count() > 0) {
        await expect(terminal).toBeVisible();

        const terminalBox = await terminal.boundingBox();
        expect(terminalBox).toBeTruthy();
        // Terminal should fit within viewport
        expect(terminalBox!.width).toBeLessThanOrEqual(768);
        expect(terminalBox!.width).toBeGreaterThan(300);
      }
    });
  });

  test.describe('All Sections Properly Formatted at Tablet Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_MIN);
      await page.goto('/mirdb/');
    });

    test('examples section is readable at tablet width', async ({ page }) => {
      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();
      await expect(examplesSection).toBeVisible();

      // Section heading should be visible
      const heading = page.locator('#examples h2');
      await expect(heading).toBeVisible();

      // Tab buttons should be visible and tappable
      const tabButtons = page.locator('#examples .tab-button');
      const tabCount = await tabButtons.count();
      expect(tabCount).toBeGreaterThan(0);

      for (let i = 0; i < tabCount; i++) {
        const tabBox = await tabButtons.nth(i).boundingBox();
        expect(tabBox).toBeTruthy();
        expect(tabBox!.height).toBeGreaterThanOrEqual(36);
      }

      // Code blocks should be visible
      const codeBlocks = page.locator('#examples .terminal-body, #examples pre');
      const blockCount = await codeBlocks.count();
      expect(blockCount).toBeGreaterThan(0);
    });

    test('architecture section diagram scales correctly at tablet width', async ({ page }) => {
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();

      // Check for diagram (SVG or img)
      const diagram = page.locator('#architecture img, #architecture svg');
      if (await diagram.count() > 0) {
        const diagramBox = await diagram.first().boundingBox();
        expect(diagramBox).toBeTruthy();
        // Diagram should scale within tablet viewport
        expect(diagramBox!.width).toBeLessThanOrEqual(768);
      }

      // Architecture content should be readable
      const archContent = page.locator('#architecture p, #architecture li');
      const contentCount = await archContent.count();
      expect(contentCount).toBeGreaterThan(0);
    });

    test('installation section tabs and code blocks work at tablet width', async ({ page }) => {
      const installationSection = page.locator('#installation');
      await installationSection.scrollIntoViewIfNeeded();
      await expect(installationSection).toBeVisible();

      // Tab buttons should be present and functional
      const tabButtons = page.locator('#installation .install-tab-button');
      const tabCount = await tabButtons.count();
      expect(tabCount).toBeGreaterThan(1);

      // Click second tab
      await tabButtons.nth(1).click();
      await page.waitForTimeout(200);

      // Verify tab switched
      await expect(tabButtons.nth(1)).toHaveAttribute('aria-selected', 'true');

      // Code blocks should be visible
      const codeBlocks = page.locator('#installation .terminal-body, #installation pre');
      const blockCount = await codeBlocks.count();
      expect(blockCount).toBeGreaterThan(0);
    });

    test('footer displays correctly at tablet width', async ({ page }) => {
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Footer links should be accessible
      const footerLinks = footer.locator('a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Footer should span appropriate width
      const footerBox = await footer.boundingBox();
      expect(footerBox).toBeTruthy();
      expect(footerBox!.width).toBeLessThanOrEqual(768);
    });
  });

  test.describe('Tablet Landscape (1023px - Max Tablet Width)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_LANDSCAPE);
      await page.goto('/mirdb/');
    });

    test('layout still uses tablet breakpoint at 1023px (before lg:)', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // At 1023px (just below lg: 1024px breakpoint), should still have 2-column grid
      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      await featuresGrid.scrollIntoViewIfNeeded();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Should still be 2 columns at 1023px
      const columnCount = gridStyle.split(' ').length;
      expect(columnCount).toBe(2);
    });

    test('no horizontal scrollbar at landscape tablet width', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('all content accessible via scrolling at landscape tablet', async ({ page }) => {
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();

        // Verify section headings are visible
        const heading = section.locator('h1, h2').first();
        if (await heading.count() > 0) {
          await expect(heading).toBeVisible();
        }
      }

      // Verify footer
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  });

  test.describe('Interactive Elements at Tablet Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_MIN);
      await page.goto('/mirdb/');
    });

    test('CTA button navigation works at tablet width', async ({ page }) => {
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await expect(getStartedButton).toBeVisible();
      await expect(getStartedButton).toHaveAttribute('href', '#installation');

      // Click Get Started button
      await getStartedButton.click();
      await page.waitForTimeout(500);

      // Should navigate to installation section
      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();
    });

    test('tab switching works in examples section', async ({ page }) => {
      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();

      const tabButtons = page.locator('#examples .tab-button');
      const secondTab = tabButtons.nth(1);

      // Click second tab
      await secondTab.click();
      await page.waitForTimeout(200);

      // Verify tab is active
      await expect(secondTab).toHaveAttribute('aria-selected', 'true');

      // Verify corresponding panel is visible
      const secondPanelId = await secondTab.getAttribute('aria-controls');
      if (secondPanelId) {
        const secondPanel = page.locator(`#${secondPanelId}`);
        await expect(secondPanel).toBeVisible();
      }
    });

    test('copy buttons are accessible at tablet width', async ({ page }) => {
      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();

      // Find copy buttons
      const copyButtons = page.locator('#examples .copy-button, #examples button[aria-label*="Copy"]');
      const copyCount = await copyButtons.count();

      if (copyCount > 0) {
        const firstCopyBtn = copyButtons.first();
        await expect(firstCopyBtn).toBeVisible();

        // Button should have adequate size
        const btnBox = await firstCopyBtn.boundingBox();
        expect(btnBox).toBeTruthy();
        expect(btnBox!.width).toBeGreaterThanOrEqual(24);
        expect(btnBox!.height).toBeGreaterThanOrEqual(24);
      }
    });
  });

  test.describe('Responsive Breakpoint Transition Tests', () => {
    test('layout changes correctly from mobile to tablet breakpoint', async ({ page }) => {
      // Start at mobile width
      await page.setViewportSize({ width: 767, height: 1024 });
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Check mobile layout (1 column)
      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      await featuresGrid.scrollIntoViewIfNeeded();

      let gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });
      let columnCount = gridStyle.split(' ').length;
      expect(columnCount).toBe(1);

      // Switch to tablet width
      await page.setViewportSize(TABLET_MIN);
      await page.waitForTimeout(300); // Allow layout to recalculate

      // Check tablet layout (2 columns)
      gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });
      columnCount = gridStyle.split(' ').length;
      expect(columnCount).toBe(2);
    });

    test('renders correctly across multiple tablet widths', async ({ page }) => {
      const tabletWidths = [768, 800, 900, 1000, 1023];

      for (const width of tabletWidths) {
        await page.setViewportSize({ width, height: 1024 });
        await page.goto('/mirdb/');
        await page.waitForLoadState('networkidle');

        // No horizontal scroll at any tablet width
        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });
        expect(hasHorizontalScroll).toBe(false);

        // Features should have 2 columns at all tablet widths
        const featuresGrid = page.locator('#features .features-grid, #features .grid');
        await featuresGrid.scrollIntoViewIfNeeded();

        const gridStyle = await featuresGrid.evaluate((el) => {
          return window.getComputedStyle(el).gridTemplateColumns;
        });
        const columnCount = gridStyle.split(' ').length;
        expect(columnCount).toBe(2);
      }
    });
  });
});

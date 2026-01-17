import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Desktop Viewport Responsive Design
 *
 * This test suite verifies that the homepage displays correctly on desktop viewports.
 * It tests full desktop layout at 1280px, proper containment at 1920px, and
 * content max-width for readability.
 *
 * Requirements: NFR-2 (Fully responsive design supporting mobile, tablet, and desktop viewports)
 */

// Desktop viewport dimensions
const DESKTOP_1280 = {
  width: 1280,
  height: 800,
};

const DESKTOP_1920 = {
  width: 1920,
  height: 1080,
};

test.describe('Responsive Design - Desktop Viewport', () => {
  test.describe('Test Case 1: Full desktop layout at 1280px width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_1280);
      await page.goto('/');
    });

    test('All sections are visible and utilize desktop space effectively', async ({ page }) => {
      // Verify viewport is set to desktop width
      const viewportSize = page.viewportSize();
      expect(viewportSize?.width).toBe(1280);

      // Verify hero section is visible with full layout
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify hero content has appropriate width
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();
      const heroContentBox = await heroContent.boundingBox();
      expect(heroContentBox).toBeTruthy();
      // Hero content should use reasonable width at desktop
      expect(heroContentBox!.width).toBeGreaterThan(400);
      expect(heroContentBox!.width).toBeLessThanOrEqual(800); // max-width: 800px in CSS

      // Verify main heading is properly displayed
      const heading = page.locator('h1');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('MirDB');

      // Verify tagline is visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // Verify description is visible
      const description = page.locator('.description');
      await expect(description).toBeVisible();

      // Verify CTA buttons are displayed horizontally (side by side)
      const getStartedBtn = page.locator('[data-link="get-started"]');
      const githubBtn = page.locator('[data-link="github-hero"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();

      const getStartedBox = await getStartedBtn.boundingBox();
      const githubBox = await githubBtn.boundingBox();
      expect(getStartedBox).toBeTruthy();
      expect(githubBox).toBeTruthy();

      // Buttons should be on the same row (similar Y position)
      const yTolerance = 30;
      expect(Math.abs(getStartedBox!.y - githubBox!.y)).toBeLessThan(yTolerance);

      // Verify all main sections are present and visible
      const sections = ['#features', '#commands', '#code-example', '#architecture', '#getting-started', '#configuration'];
      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Verify footer is visible
      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('Features grid displays multi-column layout at 1280px', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Get features grid
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(6);

      // Verify multi-column layout (at 1280px with minmax(300px, 1fr), should have 3 columns)
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);
      const thirdCard = featureCards.nth(2);
      const fourthCard = featureCards.nth(3);

      await expect(firstCard).toBeVisible();
      await expect(secondCard).toBeVisible();
      await expect(thirdCard).toBeVisible();
      await expect(fourthCard).toBeVisible();

      const firstCardBox = await firstCard.boundingBox();
      const secondCardBox = await secondCard.boundingBox();
      const thirdCardBox = await thirdCard.boundingBox();
      const fourthCardBox = await fourthCard.boundingBox();

      expect(firstCardBox).toBeTruthy();
      expect(secondCardBox).toBeTruthy();
      expect(thirdCardBox).toBeTruthy();
      expect(fourthCardBox).toBeTruthy();

      // Verify horizontal arrangement (cards should be side by side)
      // First, second, and third cards should be on the same row
      const yTolerance = 20;
      expect(Math.abs(firstCardBox!.y - secondCardBox!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(secondCardBox!.y - thirdCardBox!.y)).toBeLessThan(yTolerance);

      // Fourth card should be on the next row
      expect(fourthCardBox!.y).toBeGreaterThan(firstCardBox!.y + firstCardBox!.height - yTolerance);

      // Verify cards have reasonable width at desktop
      expect(firstCardBox!.width).toBeGreaterThan(250);
      expect(firstCardBox!.width).toBeLessThan(450);
    });

    test('Commands grid displays multi-column layout at 1280px', async ({ page }) => {
      // Navigate to commands section
      const commandsSection = page.locator('#commands');
      await commandsSection.scrollIntoViewIfNeeded();
      await expect(commandsSection).toBeVisible();

      // Get commands grid
      const commandsGrid = page.locator('.commands-grid');
      await expect(commandsGrid).toBeVisible();

      // Get command cards
      const commandCards = page.locator('.command-card');
      const cardCount = await commandCards.count();
      expect(cardCount).toBe(7); // SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND

      // Verify multiple columns (at 1280px, should have 4+ columns for command cards)
      const firstCard = commandCards.nth(0);
      const secondCard = commandCards.nth(1);
      const thirdCard = commandCards.nth(2);
      const fourthCard = commandCards.nth(3);

      const firstCardBox = await firstCard.boundingBox();
      const secondCardBox = await secondCard.boundingBox();
      const thirdCardBox = await thirdCard.boundingBox();
      const fourthCardBox = await fourthCard.boundingBox();

      expect(firstCardBox).toBeTruthy();
      expect(secondCardBox).toBeTruthy();
      expect(thirdCardBox).toBeTruthy();
      expect(fourthCardBox).toBeTruthy();

      // At 1280px with minmax(200px, 1fr), first 4 cards should be on same row
      const yTolerance = 20;
      expect(Math.abs(firstCardBox!.y - secondCardBox!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(secondCardBox!.y - thirdCardBox!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(thirdCardBox!.y - fourthCardBox!.y)).toBeLessThan(yTolerance);
    });
  });

  test.describe('Test Case 2: Layout centered and contained at 1920px width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_1920);
      await page.goto('/');
    });

    test('Content remains centered and does not stretch excessively', async ({ page }) => {
      // Verify viewport is set to large desktop width
      const viewportSize = page.viewportSize();
      expect(viewportSize?.width).toBe(1920);

      // Check container centering
      const containers = page.locator('.container');
      const containerCount = await containers.count();
      expect(containerCount).toBeGreaterThan(0);

      // Verify first container (hero) is centered
      const heroContainer = page.locator('.hero .container');
      await expect(heroContainer).toBeVisible();
      const heroContainerBox = await heroContainer.boundingBox();
      expect(heroContainerBox).toBeTruthy();

      // Container should be centered: left margin should roughly equal viewport width - container width - left position
      const expectedLeftMargin = (1920 - heroContainerBox!.width) / 2;
      const actualLeftMargin = heroContainerBox!.x;
      // Allow some tolerance for browser rendering
      expect(Math.abs(actualLeftMargin - expectedLeftMargin)).toBeLessThan(50);

      // Container width should not exceed max-width (1200px)
      expect(heroContainerBox!.width).toBeLessThanOrEqual(1200 + 40); // 1200px + padding

      // Verify hero content is centered within container
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();
      const heroContentBox = await heroContent.boundingBox();
      expect(heroContentBox).toBeTruthy();

      // Hero content should be centered
      const heroCenter = heroContentBox!.x + heroContentBox!.width / 2;
      const viewportCenter = 1920 / 2;
      expect(Math.abs(heroCenter - viewportCenter)).toBeLessThan(100);

      // Verify features section container is also centered
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      const featuresContainer = page.locator('#features .container');
      await expect(featuresContainer).toBeVisible();
      const featuresContainerBox = await featuresContainer.boundingBox();
      expect(featuresContainerBox).toBeTruthy();

      // Features container should also be constrained and centered
      expect(featuresContainerBox!.width).toBeLessThanOrEqual(1200 + 40);
      const featuresCenter = featuresContainerBox!.x + featuresContainerBox!.width / 2;
      expect(Math.abs(featuresCenter - viewportCenter)).toBeLessThan(100);
    });

    test('Layout does not have excessive whitespace or stretch', async ({ page }) => {
      // Verify hero section layout
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Hero should span full width
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox!.width).toBe(1920);

      // But content should be contained
      const heroContent = page.locator('.hero-content');
      const heroContentBox = await heroContent.boundingBox();
      expect(heroContentBox).toBeTruthy();
      // Hero content max-width is 800px
      expect(heroContentBox!.width).toBeLessThanOrEqual(800);

      // Verify features grid is properly contained
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();
      const gridBox = await featuresGrid.boundingBox();
      expect(gridBox).toBeTruthy();

      // Grid should be within container bounds
      expect(gridBox!.width).toBeLessThanOrEqual(1200);

      // Feature cards should have reasonable max width (not stretched across entire screen)
      const firstCard = page.locator('.feature-card').first();
      const firstCardBox = await firstCard.boundingBox();
      expect(firstCardBox).toBeTruthy();
      // With 3 columns in a 1200px container with gaps, each card should be ~350-400px
      expect(firstCardBox!.width).toBeLessThan(450);
      expect(firstCardBox!.width).toBeGreaterThan(250);
    });

    test('Footer content is centered at 1920px', async ({ page }) => {
      // Navigate to footer
      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify footer spans full width
      const footerBox = await footer.boundingBox();
      expect(footerBox).toBeTruthy();
      expect(footerBox!.width).toBe(1920);

      // Verify footer content is centered
      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();
      const footerContentBox = await footerContent.boundingBox();
      expect(footerContentBox).toBeTruthy();

      // Footer content should be centered
      const contentCenter = footerContentBox!.x + footerContentBox!.width / 2;
      const viewportCenter = 1920 / 2;
      expect(Math.abs(contentCenter - viewportCenter)).toBeLessThan(100);
    });
  });

  test.describe('Test Case 3: Content max-width for readability', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_1280);
      await page.goto('/');
    });

    test('Container has max-width of 1200px', async ({ page }) => {
      // Check container max-width
      const container = page.locator('.container').first();
      await expect(container).toBeVisible();

      const containerMaxWidth = await container.evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      expect(containerMaxWidth).toBe('1200px');
    });

    test('Hero content has max-width of 800px for optimal readability', async ({ page }) => {
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();

      const heroMaxWidth = await heroContent.evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      expect(heroMaxWidth).toBe('800px');
    });

    test('Section descriptions have reasonable max-width', async ({ page }) => {
      // Check section description max-width
      const sectionDescription = page.locator('.section-description').first();
      await sectionDescription.scrollIntoViewIfNeeded();
      await expect(sectionDescription).toBeVisible();

      const descMaxWidth = await sectionDescription.evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      expect(descMaxWidth).toBe('600px');
    });

    test('Configuration table has max-width constraint', async ({ page }) => {
      // Navigate to configuration section
      const configSection = page.locator('#configuration');
      await configSection.scrollIntoViewIfNeeded();
      await expect(configSection).toBeVisible();

      // Check table max-width
      const configTable = page.locator('.config-table table');
      await expect(configTable).toBeVisible();

      const tableMaxWidth = await configTable.evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      expect(tableMaxWidth).toBe('600px');

      // Verify table is centered
      const tableBox = await configTable.boundingBox();
      expect(tableBox).toBeTruthy();

      // Get parent container width
      const containerBox = await page.locator('#configuration .container').boundingBox();
      expect(containerBox).toBeTruthy();

      // Table should be centered within container
      const tableCenter = tableBox!.x + tableBox!.width / 2;
      const containerCenter = containerBox!.x + containerBox!.width / 2;
      expect(Math.abs(tableCenter - containerCenter)).toBeLessThan(50);
    });

    test('Line length for text content is readable (not too wide)', async ({ page }) => {
      // Check hero description width at actual rendering
      const description = page.locator('.description');
      await expect(description).toBeVisible();
      const descBox = await description.boundingBox();
      expect(descBox).toBeTruthy();

      // Description should be within readable line length
      // Optimal reading width is 45-75 characters, which at typical font size is ~400-700px
      expect(descBox!.width).toBeLessThanOrEqual(800);

      // Feature card text should also be reasonably constrained
      const featureCard = page.locator('.feature-card p').first();
      await featureCard.scrollIntoViewIfNeeded();
      await expect(featureCard).toBeVisible();
      const featureTextBox = await featureCard.boundingBox();
      expect(featureTextBox).toBeTruthy();

      // Feature card text should be within readable width
      expect(featureTextBox!.width).toBeLessThanOrEqual(400);
    });
  });

  test.describe('Additional Desktop Layout Tests', () => {
    test('Architecture diagram displays horizontally at desktop width', async ({ page }) => {
      await page.setViewportSize(DESKTOP_1280);
      await page.goto('/');

      // Navigate to architecture section
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();

      // Get architecture flow
      const archFlow = page.locator('.arch-flow');
      await expect(archFlow).toBeVisible();

      // Verify all components are visible
      const walComponent = page.locator('[data-component="wal"]');
      const memtableComponent = page.locator('[data-component="memtable"]');
      const sstableComponent = page.locator('[data-component="sstable"]');

      await expect(walComponent).toBeVisible();
      await expect(memtableComponent).toBeVisible();
      await expect(sstableComponent).toBeVisible();

      // Verify horizontal layout (components should be on the same row)
      const walBox = await walComponent.boundingBox();
      const memtableBox = await memtableComponent.boundingBox();
      const sstableBox = await sstableComponent.boundingBox();

      expect(walBox).toBeTruthy();
      expect(memtableBox).toBeTruthy();
      expect(sstableBox).toBeTruthy();

      // At desktop width, components should be arranged horizontally
      const yTolerance = 30;
      expect(Math.abs(walBox!.y - memtableBox!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(memtableBox!.y - sstableBox!.y)).toBeLessThan(yTolerance);

      // Components should be arranged left to right
      expect(memtableBox!.x).toBeGreaterThan(walBox!.x);
      expect(sstableBox!.x).toBeGreaterThan(memtableBox!.x);
    });

    test('Code blocks utilize desktop width appropriately', async ({ page }) => {
      await page.setViewportSize(DESKTOP_1280);
      await page.goto('/');

      // Navigate to code example section
      const codeSection = page.locator('#code-example');
      await codeSection.scrollIntoViewIfNeeded();
      await expect(codeSection).toBeVisible();

      // Check code block
      const codeBlock = page.locator('#code-example .code-block');
      await expect(codeBlock).toBeVisible();
      const codeBlockBox = await codeBlock.boundingBox();
      expect(codeBlockBox).toBeTruthy();

      // Code block should have reasonable width (not stretched too much but wide enough)
      expect(codeBlockBox!.width).toBeGreaterThan(400);
      expect(codeBlockBox!.width).toBeLessThanOrEqual(1200); // Within container max-width
    });

    test('Footer links display horizontally at desktop', async ({ page }) => {
      await page.setViewportSize(DESKTOP_1280);
      await page.goto('/');

      // Navigate to footer
      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Check footer links
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBe(3); // GitHub, Issues, Documentation

      // Get positions of footer links
      const githubLink = page.locator('[data-link="github-footer"]');
      const issuesLink = page.locator('[data-link="issues"]');
      const docsLink = page.locator('[data-link="documentation"]');

      const githubBox = await githubLink.boundingBox();
      const issuesBox = await issuesLink.boundingBox();
      const docsBox = await docsLink.boundingBox();

      expect(githubBox).toBeTruthy();
      expect(issuesBox).toBeTruthy();
      expect(docsBox).toBeTruthy();

      // Links should be arranged horizontally (similar Y position)
      const yTolerance = 10;
      expect(Math.abs(githubBox!.y - issuesBox!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(issuesBox!.y - docsBox!.y)).toBeLessThan(yTolerance);

      // Links should be left to right
      expect(issuesBox!.x).toBeGreaterThan(githubBox!.x);
      expect(docsBox!.x).toBeGreaterThan(issuesBox!.x);
    });
  });
});

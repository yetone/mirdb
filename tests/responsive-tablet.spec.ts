import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Tablet Viewport Responsive Design
 *
 * This test suite verifies that the homepage displays correctly on tablet viewports (768px width).
 * It tests layout adaptation, feature grid layout, and navigation usability at tablet widths.
 *
 * Requirements: NFR-2 (Fully responsive design supporting mobile, tablet, and desktop viewports)
 */

// Tablet viewport dimensions (iPad portrait)
const TABLET_VIEWPORT = {
  width: 768,
  height: 1024,
};

test.describe('Responsive Design - Tablet Viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet size before each test
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('Test Case 1: Homepage layout adapts appropriately at 768px tablet width', async ({ page }) => {
    // Verify viewport is set to tablet width
    const viewportSize = page.viewportSize();
    expect(viewportSize?.width).toBe(768);

    // Verify hero section is visible and properly displayed
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero content is centered
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Verify main heading is visible
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Verify description is visible
    const description = page.locator('.description');
    await expect(description).toBeVisible();

    // Verify CTA buttons are visible and accessible
    const getStartedBtn = page.locator('[data-link="get-started"]');
    const githubBtn = page.locator('[data-link="github-hero"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Verify buttons are not vertically stacked (should be side by side at tablet width)
    const getStartedBox = await getStartedBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();
    expect(getStartedBox).toBeTruthy();
    expect(githubBox).toBeTruthy();

    // Verify all main sections are visible
    const sections = ['#features', '#commands', '#code-example', '#architecture', '#getting-started'];
    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeVisible();
    }

    // Verify footer is visible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();
  });

  test('Test Case 2: Features grid displays in 2-column layout at tablet width', async ({ page }) => {
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
    expect(cardCount).toBeGreaterThanOrEqual(6); // Should have at least 6 features

    // Get bounding boxes of the first few cards to verify layout
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    await expect(firstCard).toBeVisible();
    await expect(secondCard).toBeVisible();
    await expect(thirdCard).toBeVisible();

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();
    const thirdCardBox = await thirdCard.boundingBox();

    expect(firstCardBox).toBeTruthy();
    expect(secondCardBox).toBeTruthy();
    expect(thirdCardBox).toBeTruthy();

    // At 768px width with minmax(300px, 1fr), we should have 2 columns
    // First and second cards should be on the same row (similar Y position)
    // Third card should be on a different row (higher Y position than first)
    const yTolerance = 20; // Allow some tolerance for layout variations

    // Verify first and second cards are on the same row
    expect(Math.abs(firstCardBox!.y - secondCardBox!.y)).toBeLessThan(yTolerance);

    // Verify third card is below the first row
    expect(thirdCardBox!.y).toBeGreaterThan(firstCardBox!.y + firstCardBox!.height - yTolerance);

    // Verify cards don't overlap horizontally
    expect(secondCardBox!.x).toBeGreaterThan(firstCardBox!.x);

    // Verify cards are reasonably sized (not too narrow or too wide)
    expect(firstCardBox!.width).toBeGreaterThan(200);
    expect(firstCardBox!.width).toBeLessThan(500);
  });

  test('Test Case 3: Navigation is usable and not cramped at tablet width', async ({ page }) => {
    // Verify footer navigation links are visible and accessible
    const footerLinks = page.locator('.footer-links');
    await footerLinks.scrollIntoViewIfNeeded();
    await expect(footerLinks).toBeVisible();

    // Check individual footer links
    const githubLink = page.locator('[data-link="github-footer"]');
    const issuesLink = page.locator('[data-link="issues"]');
    const documentationLink = page.locator('[data-link="documentation"]');

    await expect(githubLink).toBeVisible();
    await expect(issuesLink).toBeVisible();
    await expect(documentationLink).toBeVisible();

    // Get link positions to verify they have adequate spacing
    const githubBox = await githubLink.boundingBox();
    const issuesBox = await issuesLink.boundingBox();
    const docsBox = await documentationLink.boundingBox();

    expect(githubBox).toBeTruthy();
    expect(issuesBox).toBeTruthy();
    expect(docsBox).toBeTruthy();

    // Verify links are not cramped (have minimum spacing between them)
    const minSpacing = 10; // Minimum expected spacing in pixels
    expect(issuesBox!.x).toBeGreaterThan(githubBox!.x + githubBox!.width + minSpacing);

    // Verify CTA buttons in hero are accessible
    await page.locator('.hero').scrollIntoViewIfNeeded();
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await expect(getStartedBtn).toBeVisible();

    // Verify button is large enough for touch targets (minimum 44x44 for accessibility)
    const getStartedBox = await getStartedBtn.boundingBox();
    expect(getStartedBox).toBeTruthy();
    expect(getStartedBox!.width).toBeGreaterThanOrEqual(44);
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(44);

    // Verify "Get Started" button click navigates to getting started section
    await getStartedBtn.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify getting started section is now visible in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify commands grid is usable at tablet width
    await page.locator('#commands').scrollIntoViewIfNeeded();
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    const commandCards = page.locator('.command-card');
    const commandCardCount = await commandCards.count();
    expect(commandCardCount).toBe(7); // SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND

    // Verify command cards are visible and readable
    for (let i = 0; i < commandCardCount; i++) {
      const card = commandCards.nth(i);
      await expect(card).toBeVisible();

      const commandName = card.locator('.command-name');
      await expect(commandName).toBeVisible();
    }
  });

  test('Additional: Architecture diagram adapts properly at tablet width', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Verify architecture flow is visible
    const archFlow = page.locator('.arch-flow');
    await expect(archFlow).toBeVisible();

    // Verify all architecture components are visible
    const walComponent = page.locator('[data-component="wal"]');
    const memtableComponent = page.locator('[data-component="memtable"]');
    const sstableComponent = page.locator('[data-component="sstable"]');

    await expect(walComponent).toBeVisible();
    await expect(memtableComponent).toBeVisible();
    await expect(sstableComponent).toBeVisible();

    // Verify arrows are visible
    const arrows = page.locator('.arch-arrow');
    const arrowCount = await arrows.count();
    expect(arrowCount).toBe(2); // Two arrows: WAL -> Memtable -> SSTable
  });

  test('Additional: Code example section is properly displayed at tablet width', async ({ page }) => {
    // Navigate to code example section
    const codeExampleSection = page.locator('#code-example');
    await codeExampleSection.scrollIntoViewIfNeeded();
    await expect(codeExampleSection).toBeVisible();

    // Verify code block is visible (within the code-example section)
    const codeBlock = codeExampleSection.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    // Verify code is visible
    const code = page.locator('#example-code');
    await expect(code).toBeVisible();

    // Verify copy button is visible and clickable
    const copyBtn = codeExampleSection.locator('.copy-btn');
    await expect(copyBtn).toBeVisible();

    // Get code block width
    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).toBeTruthy();

    // Verify code block fits within viewport (doesn't extend beyond screen)
    expect(codeBlockBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('Additional: Getting Started steps are readable at tablet width', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Verify all steps are visible
    const steps = page.locator('.step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Verify each step has a heading and code block
    for (let i = 0; i < stepCount; i++) {
      const step = steps.nth(i);
      await expect(step).toBeVisible();

      const heading = step.locator('h3');
      await expect(heading).toBeVisible();

      const codeBlock = step.locator('.code-block');
      await expect(codeBlock).toBeVisible();
    }
  });
});

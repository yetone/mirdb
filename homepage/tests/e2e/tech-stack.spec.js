/**
 * Technology Stack Section E2E Tests
 * Owner: Scenario 3 - Technology Stack Section
 *
 * Tests the technology stack section displays the technologies used in MirDB
 * including Rust, Tokio, and other core technologies per REQ-7.
 */

const { test, expect } = require('@playwright/test');

test.describe('Technology Stack Section (Scenario 3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Technology section exists with heading
  test('TC1: Technology section exists with heading "Technology" or "Built With" or similar', async ({ page }) => {
    const techSection = page.locator('#tech-stack');
    await expect(techSection).toBeVisible();

    // Check for a technology heading (could be visible or sr-only)
    const techHeading = page.locator('#tech-title, .tech-stack__title, #tech-stack h2');
    await expect(techHeading).toHaveCount(1);

    const headingText = await techHeading.textContent();
    const lowerText = headingText.toLowerCase();
    const hasValidHeading = lowerText.includes('technology') ||
                            lowerText.includes('built with') ||
                            lowerText.includes('tech stack') ||
                            lowerText.includes('powered by');
    expect(hasValidHeading).toBeTruthy();
  });

  // Test Case 2: Section contains "Rust" text with optional logo/badge
  test('TC2: Section contains "Rust" text with optional logo/badge', async ({ page }) => {
    const techSection = page.locator('#tech-stack');
    const sectionText = await techSection.textContent();

    // Check for Rust mention
    expect(sectionText.toLowerCase()).toContain('rust');

    // Verify there's a tech item with Rust
    const rustItem = page.locator('#tech-stack .tech-stack__item, #tech-stack .tech-item, #tech-stack article').filter({
      hasText: /rust/i
    });
    await expect(rustItem.first()).toBeVisible();
  });

  // Test Case 3: Section contains "Tokio" text with optional logo/badge
  test('TC3: Section contains "Tokio" text with optional logo/badge', async ({ page }) => {
    const techSection = page.locator('#tech-stack');
    const sectionText = await techSection.textContent();

    // Check for Tokio mention
    expect(sectionText.toLowerCase()).toContain('tokio');

    // Verify there's a tech item with Tokio
    const tokioItem = page.locator('#tech-stack .tech-stack__item, #tech-stack .tech-item, #tech-stack article').filter({
      hasText: /tokio/i
    });
    await expect(tokioItem.first()).toBeVisible();
  });

  // Test Case 4: At least 2 technology items are visually represented
  test('TC4: At least 2 technology items are visually represented', async ({ page }) => {
    const techItems = page.locator('#tech-stack .tech-stack__item, #tech-stack .tech-item, #tech-stack article');
    const count = await techItems.count();

    expect(count).toBeGreaterThanOrEqual(2);

    // Check that each tech item has some visual representation (badge, icon, or logo)
    for (let i = 0; i < Math.min(count, 2); i++) {
      const item = techItems.nth(i);
      // Each item should have visual elements like an icon, badge, or be styled as a card
      await expect(item).toBeVisible();
    }
  });

  // Additional test: Tech items have titles or names
  test('Each technology item has a name/title', async ({ page }) => {
    const techItems = page.locator('#tech-stack .tech-stack__item, #tech-stack .tech-item, #tech-stack article');
    const count = await techItems.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const item = techItems.nth(i);
      const title = item.locator('h3, h4, .tech-stack__name, .tech-name, strong');
      await expect(title.first()).toBeVisible();
    }
  });

  // Additional test: Tech items have descriptions
  test('Each technology item has a description', async ({ page }) => {
    const techItems = page.locator('#tech-stack .tech-stack__item, #tech-stack .tech-item, #tech-stack article');
    const count = await techItems.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const item = techItems.nth(i);
      const description = item.locator('p, .tech-stack__description, .tech-description');
      await expect(description.first()).toBeVisible();
    }
  });

  // Additional test: Tech section has visual badges/logos
  test('Technology section has visual badges or icons', async ({ page }) => {
    const techSection = page.locator('#tech-stack');

    // Look for visual elements like SVGs, images, or badge-styled elements
    const visualElements = techSection.locator('svg, img, .tech-stack__icon, .tech-stack__badge, .tech-icon');
    const count = await visualElements.count();

    expect(count).toBeGreaterThanOrEqual(2);
  });
});

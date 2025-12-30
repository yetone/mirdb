const { test, expect } = require('@playwright/test');

/**
 * US-4: Visual Demonstration
 * Validate acceptance criteria for US-4: Demo shows MirDB in action
 *
 * Acceptance Criteria:
 * - Given I am on the homepage
 * - When I view the demo section
 * - Then I see an animated demonstration of MirDB usage
 * - And the demo clearly shows command input and output
 */

test.describe('US-4: Visual Demo - User Story Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Usage demonstration GIF is present and visible', async ({ page }) => {
    // Step 1: Navigate to demo section
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Step 2: Verify demo presence - Check that usage.gif animation is visible
    const usageGif = demoSection.locator('img[src*="usage.gif"]');
    await expect(usageGif).toBeVisible();

    // Verify the image source is correct
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');

    // Verify the image has proper dimensions (not broken)
    const boundingBox = await usageGif.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(100);
    expect(boundingBox.height).toBeGreaterThan(100);

    // Verify the image has alt text for accessibility
    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toMatch(/demo|usage|mirdb|action|command/);
  });

  test('Test Case 2: Demo has caption or description explaining what is being demonstrated', async ({ page }) => {
    // Step 1: Navigate to demo section
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Step 2: Verify demo section has a header explaining what it shows
    const demoHeader = demoSection.locator('.demo-header h2, h2');
    await expect(demoHeader.first()).toBeVisible();
    const headerText = await demoHeader.first().textContent();
    expect(headerText.toLowerCase()).toMatch(/mirdb|action|demo|see/);

    // Step 3: Verify demo clarity - Check that demo has caption/description
    const caption = demoSection.locator('figcaption, .demo-caption');
    await expect(caption).toBeVisible();

    // Verify caption explains what is being demonstrated
    const captionText = await caption.textContent();
    expect(captionText.length).toBeGreaterThan(20);

    // Caption should mention commands, operations, or what the demo shows
    const captionLower = captionText.toLowerCase();
    const hasExplanatoryContent =
      captionLower.includes('command') ||
      captionLower.includes('set') ||
      captionLower.includes('get') ||
      captionLower.includes('delete') ||
      captionLower.includes('memcached') ||
      captionLower.includes('demonstration') ||
      captionLower.includes('shows');

    expect(hasExplanatoryContent).toBe(true);
  });

  test('Demo section is navigable from the page', async ({ page }) => {
    // Verify the demo section can be scrolled to or navigated to
    const demoSection = page.locator('#demo');

    // Check that demo section exists on page
    await expect(demoSection).toBeAttached();

    // Scroll to demo section
    await demoSection.scrollIntoViewIfNeeded();

    // Verify it's visible after scrolling
    await expect(demoSection).toBeVisible();
  });

  test('Demo clearly shows command input and output context', async ({ page }) => {
    // Navigate to demo section
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // The caption should explain that commands are shown
    const caption = demoSection.locator('figcaption, .demo-caption');
    await expect(caption).toBeVisible();

    const captionText = await caption.textContent();
    const captionLower = captionText.toLowerCase();

    // Verify caption mentions input/output or specific commands
    const mentionsCommands =
      captionLower.includes('set') ||
      captionLower.includes('get') ||
      captionLower.includes('delete') ||
      captionLower.includes('command');

    expect(mentionsCommands).toBe(true);
  });
});

const { test, expect } = require('@playwright/test');

/**
 * US-2: Feature Discovery
 * Scenario: Validate acceptance criteria for US-2: Memcached user can see supported commands
 *
 * As a Memcached User, I want to see which memcached commands are supported,
 * so that I know if MirDB is compatible with my existing code.
 *
 * Acceptance Criteria:
 * - Navigate to features section and find command list
 * - Commands list includes SET, GET, DELETE, ADD, REPLACE
 * - Visual indicator distinguishes implemented features from planned (Raft)
 */

test.describe('US-2: Feature Discovery - Memcached User Views Supported Commands', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Commands list includes SET, GET, DELETE, ADD, REPLACE', async ({ page }) => {
    // Step 1: Navigate to features section
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection.first()).toBeVisible();

    // Step 2: Find command list - check commands section exists
    const commandsSection = page.locator('#commands, .commands');
    await expect(commandsSection.first()).toBeVisible();

    // Verify commands section has supported commands documentation
    const commandsSectionContent = await commandsSection.first().textContent();

    // Expected: Commands list includes SET, GET, DELETE, ADD, REPLACE
    expect(commandsSectionContent).toContain('SET');
    expect(commandsSectionContent).toContain('GET');
    expect(commandsSectionContent).toContain('DELETE');
    expect(commandsSectionContent).toContain('ADD');
    expect(commandsSectionContent).toContain('REPLACE');

    // Verify command categories exist for organization
    const storageCategory = commandsSection.locator('.command-category-storage, .command-category').filter({ hasText: /storage/i });
    await expect(storageCategory.first()).toBeVisible();

    const retrievalCategory = commandsSection.locator('.command-category-retrieval, .command-category').filter({ hasText: /retrieval/i });
    await expect(retrievalCategory.first()).toBeVisible();

    const deletionCategory = commandsSection.locator('.command-category-deletion, .command-category').filter({ hasText: /deletion/i });
    await expect(deletionCategory.first()).toBeVisible();
  });

  test('Test Case 2: Visual indicator distinguishes implemented features from planned (Raft)', async ({ page }) => {
    // Step 1: Navigate to features section
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection.first()).toBeVisible();

    // Step 2: Verify implemented features are visible without planned indicator
    // Check that implemented features exist (Async Networking, Memtable, Compaction)
    const implementedFeatures = featuresSection.locator('.feature-card:not(.planned), .feature-item:not(.planned)');
    const implementedCount = await implementedFeatures.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Step 3: Identify feature status - Raft should be marked as planned
    // Check Raft is displayed as a planned/upcoming feature with visual distinction
    const plannedFeature = featuresSection.locator('.feature-card.planned, .feature-item.planned');
    await expect(plannedFeature.first()).toBeVisible();

    // Verify the planned feature contains Raft
    const plannedContent = await plannedFeature.first().textContent();
    expect(plannedContent.toLowerCase()).toContain('raft');

    // Verify visual distinction - planned badge exists
    const plannedBadge = plannedFeature.locator('.planned-badge');
    await expect(plannedBadge.first()).toBeVisible();
    const badgeText = await plannedBadge.first().textContent();
    expect(badgeText.toLowerCase()).toMatch(/planned|upcoming|coming soon/);

    // Verify the planned feature has distinct styling (dashed border)
    const plannedFeatureElement = featuresSection.locator('.feature-card.planned').first();
    const borderStyle = await plannedFeatureElement.evaluate(el => {
      return window.getComputedStyle(el).borderStyle;
    });
    expect(borderStyle).toContain('dashed');
  });

  test('Supplementary: Navigation to features section works', async ({ page }) => {
    // Click on Features navigation link
    const featuresNavLink = page.locator('nav a[href="#features"]');
    await expect(featuresNavLink).toBeVisible();
    await featuresNavLink.click();

    // Verify features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Supplementary: Commands section includes additional storage commands (APPEND, PREPEND)', async ({ page }) => {
    const commandsSection = page.locator('#commands, .commands');
    await expect(commandsSection.first()).toBeVisible();

    const sectionContent = await commandsSection.first().textContent();

    // Verify additional storage commands per PRD
    expect(sectionContent).toContain('APPEND');
    expect(sectionContent).toContain('PREPEND');
    expect(sectionContent).toContain('GETS');
  });
});

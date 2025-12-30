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
    const implementedFeatures = featuresSection.locator('.feature-card:not(.planned), .feature-item:not(.planned)');
    const implementedCount = await implementedFeatures.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Step 3: Identify feature status - Raft should be marked as planned
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

  test('Step 1: Navigate to features section', async ({ page }) => {
    // As a memcached user, navigate to features section
    const featuresLink = page.locator('nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    await featuresLink.click();

    // Verify we can see the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  test('Step 2: Find command list', async ({ page }) => {
    // Locate the list of supported memcached commands
    const commandsSection = page.locator('#commands, .commands');
    await expect(commandsSection.first()).toBeVisible();

    // Verify commands header is present
    const commandsHeader = commandsSection.locator('.commands-header h2');
    await expect(commandsHeader).toHaveText(/supported commands/i);

    // Verify command list contains expected commands
    const commandList = commandsSection.locator('.command-list');
    const commandListCount = await commandList.count();
    expect(commandListCount).toBeGreaterThan(0);

    // Verify we have storage, retrieval, deletion, and MirDB-specific categories
    const categoryCards = commandsSection.locator('.command-category');
    const categoryCount = await categoryCards.count();
    expect(categoryCount).toBeGreaterThanOrEqual(4); // At least 4 categories
  });

  test('Step 3: Identify feature status - visual distinction', async ({ page }) => {
    // Distinguish between implemented and planned features
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const allFeatures = featuresSection.locator('.feature-card');
    const totalCount = await allFeatures.count();
    expect(totalCount).toBeGreaterThan(0);

    // Get implemented features (no planned class)
    const implementedFeatures = featuresSection.locator('.feature-card:not(.planned)');
    const implementedCount = await implementedFeatures.count();

    // Get planned features (with planned class)
    const plannedFeatures = featuresSection.locator('.feature-card.planned');
    const plannedCount = await plannedFeatures.count();

    // Verify we have both implemented and planned features
    expect(implementedCount).toBeGreaterThan(0);
    expect(plannedCount).toBeGreaterThan(0);

    // Verify total matches
    expect(implementedCount + plannedCount).toBe(totalCount);

    // Verify planned features have visual distinction (badge visible)
    for (let i = 0; i < plannedCount; i++) {
      const plannedCard = plannedFeatures.nth(i);
      const badge = plannedCard.locator('.planned-badge');
      await expect(badge).toBeVisible();
    }

    // Verify implemented features do NOT have the planned badge visible
    for (let i = 0; i < implementedCount; i++) {
      const implementedCard = implementedFeatures.nth(i);
      const badge = implementedCard.locator('.planned-badge');
      // Badge should either not exist or be hidden for implemented features
      const badgeCount = await badge.count();
      if (badgeCount > 0) {
        await expect(badge).not.toBeVisible();
      }
    }
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

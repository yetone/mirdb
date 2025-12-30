// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Project status section exists', async ({ page }) => {
    // Test Case 1: Query for project status or roadmap section
    // Expected: Section exists indicating project status
    const projectStatusSection = page.locator('#project-status, [data-testid="project-status"], .project-status');
    await expect(projectStatusSection).toBeVisible();

    // Verify the section has a heading indicating project status
    const heading = projectStatusSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/status|roadmap/i);
  });

  test('TC2: Visual distinction between implemented and planned features', async ({ page }) => {
    // Test Case 2: Query for visual indicators differentiating implemented vs planned features
    // Expected: Clear visual distinction between implemented and planned features
    const projectStatusSection = page.locator('#project-status, [data-testid="project-status"], .project-status');
    await expect(projectStatusSection).toBeVisible();

    // Check for implemented features section/header
    const implementedHeader = projectStatusSection.locator('[data-testid="implemented-header"], .implemented-header, h3').filter({
      hasText: /implemented/i
    }).first();
    await expect(implementedHeader).toBeVisible();

    // Check for planned features section/header
    const plannedHeader = projectStatusSection.locator('[data-testid="planned-header"], .planned-header, h3').filter({
      hasText: /planned/i
    }).first();
    await expect(plannedHeader).toBeVisible();

    // Verify visual distinction exists (different badges, icons, or colors)
    const implementedBadges = projectStatusSection.locator('.implemented-badge, [data-testid="feature-implemented"]');
    const plannedBadges = projectStatusSection.locator('.planned-badge, [data-testid="feature-planned"]');

    // At least one implemented and one planned feature should exist
    await expect(implementedBadges.first()).toBeVisible();
    await expect(plannedBadges.first()).toBeVisible();

    // Get the styles to verify visual distinction
    const implementedIcon = projectStatusSection.locator('.implemented-icon').first();
    const plannedIcon = projectStatusSection.locator('.planned-icon').first();

    if (await implementedIcon.count() > 0 && await plannedIcon.count() > 0) {
      const implementedBgColor = await implementedIcon.evaluate(el => window.getComputedStyle(el).backgroundColor);
      const plannedBgColor = await plannedIcon.evaluate(el => window.getComputedStyle(el).backgroundColor);

      // The colors should be different to indicate visual distinction
      expect(implementedBgColor).not.toEqual(plannedBgColor);
    }
  });

  test('TC3: Core commands SET, GET, DELETE are listed as implemented', async ({ page }) => {
    // Test Case 3: Verify supported commands list includes SET, GET, DELETE
    // Expected: Core commands are listed as implemented
    const projectStatusSection = page.locator('#project-status, [data-testid="project-status"], .project-status');
    await expect(projectStatusSection).toBeVisible();

    // Check for supported commands section
    const commandsSection = projectStatusSection.locator('[data-testid="supported-commands"], .supported-commands');
    await expect(commandsSection).toBeVisible();

    // Verify SET command is listed (use test-id for precision)
    const setCommand = commandsSection.locator('[data-testid="command-set"]');
    await expect(setCommand).toBeVisible();
    const setCommandText = await setCommand.textContent();
    expect(setCommandText).toContain('SET');

    // Verify GET command is listed (use test-id for precision to avoid matching GETS)
    const getCommand = commandsSection.locator('[data-testid="command-get"]');
    await expect(getCommand).toBeVisible();
    const getCommandText = await getCommand.textContent();
    expect(getCommandText).toContain('GET');

    // Verify DELETE command is listed (use test-id for precision)
    const deleteCommand = commandsSection.locator('[data-testid="command-delete"]');
    await expect(deleteCommand).toBeVisible();
    const deleteCommandText = await deleteCommand.textContent();
    expect(deleteCommandText).toContain('DELETE');
  });

  test('Implemented features list contains expected items', async ({ page }) => {
    // Additional test: Verify implemented features are actually listed
    const implementedList = page.locator('[data-testid="implemented-features"], .implemented-list');
    await expect(implementedList).toBeVisible();

    // Check for specific implemented features from knowledge base
    const implementedItems = implementedList.locator('.status-item, li');
    const count = await implementedItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify at least one implemented feature has the correct badge
    const firstImplementedItem = implementedItems.first();
    const badgeText = await firstImplementedItem.locator('.status-badge, .implemented-badge').textContent();
    expect(badgeText?.toLowerCase()).toContain('implemented');
  });

  test('Planned features list contains expected items', async ({ page }) => {
    // Additional test: Verify planned features are shown
    const plannedList = page.locator('[data-testid="planned-features"], .planned-list');
    await expect(plannedList).toBeVisible();

    // Check for at least one planned feature
    const plannedItems = plannedList.locator('.status-item, li');
    const count = await plannedItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify the planned feature has the correct badge
    const firstPlannedItem = plannedItems.first();
    const badgeText = await firstPlannedItem.locator('.status-badge, .planned-badge').textContent();
    expect(badgeText?.toLowerCase()).toContain('planned');

    // Verify Raft consensus is listed as planned
    const raftFeature = plannedList.locator('.status-item, li').filter({
      hasText: /raft/i
    });
    await expect(raftFeature).toBeVisible();
  });
});

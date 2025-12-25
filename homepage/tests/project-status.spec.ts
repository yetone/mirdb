import { test, expect } from '@playwright/test';

test.describe('Project Status and Roadmap', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: displays implemented features list with clear indicators (checkmarks)', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="status-section"]');
    await expect(statusSection).toBeVisible();

    // Check for implemented features card
    const implementedFeatures = page.locator('[data-testid="implemented-features"]');
    await expect(implementedFeatures).toBeVisible();

    // Verify the implemented list exists and has items
    const implementedList = page.locator('[data-testid="implemented-list"]');
    await expect(implementedList).toBeVisible();

    // Verify list contains expected implemented features
    const listItems = implementedList.locator('li');
    const count = await listItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify checkmark indicators are present on implemented features
    const checkmarks = implementedList.locator('.checkmark');
    const checkmarkCount = await checkmarks.count();
    expect(checkmarkCount).toBeGreaterThanOrEqual(1);

    // Verify each implemented feature has a checkmark indicator
    for (let i = 0; i < count; i++) {
      const item = listItems.nth(i);
      const checkmark = item.locator('.checkmark');
      await expect(checkmark).toBeVisible();

      // Verify checkmark has aria-label for accessibility
      const ariaLabel = await checkmark.getAttribute('aria-label');
      expect(ariaLabel).toBe('implemented');
    }

    // Verify specific implemented features are listed
    const listText = await implementedList.textContent();
    expect(listText).toContain('Memcached protocol');
    expect(listText).toContain('async networking');
    expect(listText).toContain('compaction');
  });

  test('TC2: displays planned features list with different visual treatment', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="status-section"]');
    await expect(statusSection).toBeVisible();

    // Check for planned features card
    const plannedFeatures = page.locator('[data-testid="planned-features"]');
    await expect(plannedFeatures).toBeVisible();

    // Verify the planned list exists and has items
    const plannedList = page.locator('[data-testid="planned-list"]');
    await expect(plannedList).toBeVisible();

    // Verify list contains at least one planned feature
    const listItems = plannedList.locator('li');
    const count = await listItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify planned indicators are present (different from checkmarks)
    const plannedIndicators = plannedList.locator('.planned-indicator');
    const indicatorCount = await plannedIndicators.count();
    expect(indicatorCount).toBeGreaterThanOrEqual(1);

    // Verify each planned feature has a planned indicator
    for (let i = 0; i < count; i++) {
      const item = listItems.nth(i);
      const indicator = item.locator('.planned-indicator');
      await expect(indicator).toBeVisible();

      // Verify indicator has aria-label for accessibility
      const ariaLabel = await indicator.getAttribute('aria-label');
      expect(ariaLabel).toBe('planned');
    }

    // Verify Raft consensus is listed as a planned feature
    const listText = await plannedList.textContent();
    expect(listText).toContain('Raft consensus');

    // Verify visual distinction between implemented and planned sections
    const implementedCard = page.locator('[data-testid="implemented-features"]');
    const plannedCard = page.locator('[data-testid="planned-features"]');

    // Check they have different styling - implemented uses emerald (green) colors, planned uses orange
    const implementedClasses = await implementedCard.getAttribute('class');
    const plannedClasses = await plannedCard.getAttribute('class');

    // Verify implemented card has green/emerald styling (different from planned)
    expect(implementedClasses).toMatch(/emerald/);
    // Verify planned card has orange styling (different from implemented)
    expect(plannedClasses).toMatch(/orange/);
  });

  test('TC3: displays contribution guidelines link', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="status-section"]');
    await expect(statusSection).toBeVisible();

    // Check for contribute section
    const contributeSection = page.locator('[data-testid="contribute-section"]');
    await expect(contributeSection).toBeVisible();

    // Check for contribution guidelines link
    const contributingLink = page.locator('[data-testid="contributing-link"]');
    await expect(contributingLink).toBeVisible();

    // Verify link text contains "contribution" or "contributing"
    const linkText = await contributingLink.textContent();
    expect(linkText?.toLowerCase()).toMatch(/contribut/i);

    // Verify link href points to contribution guidelines
    const href = await contributingLink.getAttribute('href');
    expect(href).toContain('CONTRIBUTING');

    // Verify it opens in a new tab (target="_blank")
    const target = await contributingLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has security attribute for external links
    const rel = await contributingLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('project status section has proper structure and styling', async ({ page }) => {
    // Navigate to status section via navigation or scroll
    const statusSection = page.locator('[data-testid="status-section"]');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Check section has a heading
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/status/i);

    // Check both implemented and planned cards are present
    const implementedCard = page.locator('[data-testid="implemented-features"]');
    const plannedCard = page.locator('[data-testid="planned-features"]');

    await expect(implementedCard).toBeVisible();
    await expect(plannedCard).toBeVisible();

    // Verify implemented card heading contains checkmark or "Implemented"
    const implementedHeading = implementedCard.locator('h3');
    await expect(implementedHeading).toContainText(/Implemented/);

    // Verify planned card heading contains "Planned"
    const plannedHeading = plannedCard.locator('h3');
    await expect(plannedHeading).toContainText(/Planned/);
  });

  test('status section is navigable from page', async ({ page }) => {
    // Status section should be visible when scrolling
    const statusSection = page.locator('[data-testid="status-section"]');

    // Scroll to status section
    await statusSection.scrollIntoViewIfNeeded();

    // Verify it becomes visible
    await expect(statusSection).toBeInViewport();
  });
});

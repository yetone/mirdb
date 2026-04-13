/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - All required features displayed
 * - Feature descriptions present
 * - Grid/list layout
 */
import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display Memcached protocol feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Memcached protocol feature
    const memcachedTitle = page.locator('.feature-title', { hasText: /Memcached Protocol/i });
    await expect(memcachedTitle).toBeVisible();

    // Check description mentions protocol compatibility
    const memcachedCard = page.locator('[data-testid="feature-card"]', { hasText: /Memcached Protocol/i });
    await expect(memcachedCard.locator('.feature-description')).toContainText(/protocol/i);
  });

  test('should display persistence feature with SSTable mention', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for disk persistence feature
    const persistenceTitle = page.locator('.feature-title', { hasText: /Disk Persistence/i });
    await expect(persistenceTitle).toBeVisible();

    // Check description mentions SSTable
    const persistenceCard = page.locator('[data-testid="feature-card"]', { hasText: /Disk Persistence/i });
    await expect(persistenceCard.locator('.feature-description')).toContainText(/SSTable/i);
  });

  test('should display LSM-tree feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for LSM-tree feature
    const lsmTitle = page.locator('.feature-title', { hasText: /LSM-Tree/i });
    await expect(lsmTitle).toBeVisible();

    // Check description mentions LSM-tree architecture
    const lsmCard = page.locator('[data-testid="feature-card"]', { hasText: /LSM-Tree/i });
    await expect(lsmCard.locator('.feature-description')).toContainText(/Log-Structured Merge-Tree/i);
  });

  test('should display skip list feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for skip list feature
    const skipListTitle = page.locator('.feature-title', { hasText: /Skip List/i });
    await expect(skipListTitle).toBeVisible();

    // Check description mentions skip list memtables
    const skipListCard = page.locator('[data-testid="feature-card"]', { hasText: /Skip List/i });
    await expect(skipListCard.locator('.feature-description')).toContainText(/skip list/i);
  });

  test('should display compaction feature with minor and major mention', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for compaction feature
    const compactionTitle = page.locator('.feature-title', { hasText: /Compaction/i });
    await expect(compactionTitle).toBeVisible();

    // Check description mentions minor and major compaction
    const compactionCard = page.locator('[data-testid="feature-card"]', { hasText: /Compaction/i });
    await expect(compactionCard.locator('.feature-description')).toContainText(/minor and major compaction/i);
  });

  test('should display at least 5 features', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(5);
  });

  test('should display features in organized grid layout', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check grid container exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid display style
    const gridDisplay = await featuresGrid.evaluate((el) => window.getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Each feature card should have title and description
    const featureCards = page.locator('[data-testid="feature-card"]');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('.feature-title')).toBeVisible();
      await expect(card.locator('.feature-description')).toBeVisible();
    }
  });

  test('should have accessible section heading', async ({ page }) => {
    const featuresHeading = page.locator('#features-heading');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Features');

    // Check section is labelled by heading
    const section = page.locator('#features');
    await expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
  });
});

/**
 * Rust Language Highlight E2E Tests
 * Owner: Scenario 18 - Rust Language Highlight
 *
 * Tests:
 * - Rust keyword appears on the page
 * - Features section includes 'Written in Rust' or similar
 * - Rust is mentioned as a feature/benefit (not incidentally)
 */
test.describe('Rust Language Highlight', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display Rust keyword on the page', async ({ page }) => {
    // Search for 'Rust' text anywhere on the page
    const rustMention = page.locator('text=Rust');
    await expect(rustMention.first()).toBeVisible();
  });

  test('should display "Written in Rust" in features section', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Rust feature title
    const rustTitle = page.locator('.feature-title', { hasText: /Written in Rust|Rust/i });
    await expect(rustTitle).toBeVisible();

    // Check feature card contains Rust-related content
    const rustCard = page.locator('[data-testid="feature-card"]', { hasText: /Rust/i });
    await expect(rustCard).toBeVisible();
    await expect(rustCard.locator('.feature-description')).toBeVisible();
  });

  test('should mention Rust as a feature/benefit with context', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Rust feature card
    const rustCard = page.locator('[data-testid="feature-card"]', { hasText: /Rust/i });
    await expect(rustCard).toBeVisible();

    // Verify Rust is mentioned in context of benefits (performance, safety, reliability)
    const description = rustCard.locator('.feature-description');
    await expect(description).toBeVisible();

    // Check that the description mentions Rust benefits (memory safety, performance, reliability)
    const descriptionText = await description.textContent();
    expect(descriptionText).toBeTruthy();

    // Verify it contains at least one benefit keyword
    const hasBenefitContext =
      /memory safety|performance|reliability|safe|fast|efficient/i.test(descriptionText || '');
    expect(hasBenefitContext).toBe(true);
  });

  test('should have Rust feature with icon and proper structure', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Rust feature card
    const rustCard = page.locator('[data-testid="feature-card"]', { hasText: /Rust/i });
    await expect(rustCard).toBeVisible();

    // Verify it has the expected structure (title + description)
    const title = rustCard.locator('.feature-title');
    const description = rustCard.locator('.feature-description');

    await expect(title).toBeVisible();
    await expect(description).toBeVisible();

    // Check for Rust crab emoji icon (🦀)
    const icon = rustCard.locator('.feature-icon');
    if (await icon.count() > 0) {
      await expect(icon).toContainText('🦀');
    }
  });
});

/**
 * Roadmap Section E2E Tests
 * Owner: Scenario 7 - Planned Features Section
 *
 * Tests:
 * - Project status/roadmap section exists
 * - Completed features have visual indicators
 * - Raft is listed as planned/upcoming
 * - Visual distinction between completed and planned
 */
test.describe('Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display project status/roadmap section', async ({ page }) => {
    // Section with heading containing 'Status', 'Roadmap', or 'Features' exists
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for heading
    const heading = page.locator('#roadmap-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/Status|Roadmap|Features/i);
  });

  test('should display completed features with visual indicators', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for completed features container
    const completedFeatures = page.locator('[data-testid="completed-features"]');
    await expect(completedFeatures).toBeVisible();

    // Check that completed items have status indicator
    const completedItems = completedFeatures.locator('[data-testid="roadmap-item"]');
    const count = await completedItems.count();
    expect(count).toBeGreaterThan(0);

    // Each completed item should have visual indicator (checkmark or 'completed' label)
    for (let i = 0; i < count; i++) {
      const item = completedItems.nth(i);
      await expect(item).toHaveAttribute('data-status', 'completed');

      // Check for completed badge or checkmark indicator
      const badge = item.locator('.roadmap-item-badge--completed');
      await expect(badge).toBeVisible();
      await expect(badge).toContainText('Completed');
    }
  });

  test('should display Raft consensus as planned/upcoming feature', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for planned features container
    const plannedFeatures = page.locator('[data-testid="planned-features"]');
    await expect(plannedFeatures).toBeVisible();

    // Find Raft consensus item
    const raftItem = plannedFeatures.locator('[data-testid="roadmap-item"]', { hasText: /Raft|consensus/i });
    await expect(raftItem).toBeVisible();
    await expect(raftItem).toHaveAttribute('data-status', 'planned');

    // Check for planned badge
    const plannedBadge = raftItem.locator('.roadmap-item-badge--planned');
    await expect(plannedBadge).toBeVisible();
    await expect(plannedBadge).toContainText('Planned');
  });

  test('should have visual distinction between completed and planned features', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for separate columns with different styling
    const completedColumn = page.locator('.roadmap-column--completed');
    const plannedColumn = page.locator('.roadmap-column--planned');

    await expect(completedColumn).toBeVisible();
    await expect(plannedColumn).toBeVisible();

    // Verify different border colors (visual distinction)
    const completedBorderColor = await completedColumn.evaluate((el) =>
      window.getComputedStyle(el).borderTopColor
    );
    const plannedBorderColor = await plannedColumn.evaluate((el) =>
      window.getComputedStyle(el).borderTopColor
    );

    // Colors should be different
    expect(completedBorderColor).not.toBe(plannedBorderColor);

    // Check that completed and planned items have different badge styling
    const completedBadge = completedColumn.locator('.roadmap-item-badge--completed').first();
    const plannedBadge = plannedColumn.locator('.roadmap-item-badge--planned').first();

    await expect(completedBadge).toBeVisible();
    await expect(plannedBadge).toBeVisible();

    // Verify different background colors for badges
    const completedBadgeBg = await completedBadge.evaluate((el) =>
      window.getComputedStyle(el).backgroundColor
    );
    const plannedBadgeBg = await plannedBadge.evaluate((el) =>
      window.getComputedStyle(el).backgroundColor
    );

    expect(completedBadgeBg).not.toBe(plannedBadgeBg);
  });

  test('should have accessible roadmap section', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check aria-labelledby
    await expect(roadmapSection).toHaveAttribute('aria-labelledby', 'roadmap-heading');

    // Check for list structure
    const completedList = page.locator('[data-testid="completed-features"]');
    const plannedList = page.locator('[data-testid="planned-features"]');

    await expect(completedList).toHaveRole('list');
    await expect(plannedList).toHaveRole('list');
  });
});

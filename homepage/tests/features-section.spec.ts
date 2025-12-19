import { test, expect } from '@playwright/test';

test.describe('Key Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Features section is visible with grid or card-based layout', async ({ page }) => {
    // Navigate to the features section by scrolling
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features section is visible
    await expect(featuresSection).toBeVisible();

    // Verify the features grid exists with card-based layout
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify the section has a proper heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toContainText('Key Features');
  });

  test('TC2: Memcached compatibility feature is displayed with brief explanation', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Memcached feature card
    const memcachedCard = page.getByTestId('feature-memcached');
    await expect(memcachedCard).toBeVisible();

    // Verify heading mentions Memcached protocol
    const heading = memcachedCard.locator('h3');
    await expect(heading).toContainText('Memcached Protocol');

    // Verify explanation is present and mentions protocol compatibility
    const description = memcachedCard.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText?.toLowerCase()).toMatch(/memcached.*protocol|protocol.*memcached|compatible|existing clients/);
  });

  test('TC3: Persistence feature is displayed with explanation', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the persistence feature card
    const persistenceCard = page.getByTestId('feature-persistence');
    await expect(persistenceCard).toBeVisible();

    // Verify heading mentions persistence/storage
    const heading = persistenceCard.locator('h3');
    await expect(heading).toContainText('Persistent Storage');

    // Verify explanation mentions data persistence capability
    const description = persistenceCard.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText?.toLowerCase()).toMatch(/persist|disk|survives|sstable/);
  });

  test('TC4: LSM tree architecture feature is displayed with brief explanation', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the LSM feature card
    const lsmCard = page.getByTestId('feature-lsm');
    await expect(lsmCard).toBeVisible();

    // Verify heading mentions LSM tree
    const heading = lsmCard.locator('h3');
    await expect(heading).toContainText('LSM Tree Architecture');

    // Verify explanation mentions LSM tree concepts
    const description = lsmCard.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText?.toLowerCase()).toMatch(/memtable|wal|sstable|compaction|storage engine/);
  });

  test('TC5: Async performance feature mentions Tokio for high-concurrency workloads', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the async performance feature card
    const asyncCard = page.getByTestId('feature-async');
    await expect(asyncCard).toBeVisible();

    // Verify heading mentions async performance
    const heading = asyncCard.locator('h3');
    await expect(heading).toContainText('Async Performance');

    // Verify explanation mentions Tokio and high-concurrency
    const description = asyncCard.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toMatch(/Tokio/i);
    expect(descText?.toLowerCase()).toMatch(/concurrency|high-concurrency|non-blocking/);
  });

  test('TC6: Features are presented in easily scannable format (cards, grid, or list with clear headings)', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify grid layout is used
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that CSS grid or flexbox is applied for scannable layout
    const gridDisplay = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display;
    });
    expect(['grid', 'flex']).toContain(gridDisplay);

    // Verify all feature cards have clear headings (h3)
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const heading = card.locator('h3');
      await expect(heading).toBeVisible();

      // Verify heading text is not empty
      const headingText = await heading.textContent();
      expect(headingText?.trim().length).toBeGreaterThan(0);

      // Verify each card has a description
      const description = card.locator('p');
      await expect(description).toBeVisible();
    }

    // Verify cards are visually styled as cards (have padding and background)
    const firstCard = featureCards.first();
    const cardStyles = await firstCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        padding: style.padding,
        backgroundColor: style.backgroundColor,
        borderRadius: style.borderRadius
      };
    });

    // Cards should have some padding (not 0px 0px 0px 0px)
    expect(cardStyles.padding).not.toBe('0px');
  });

  test('Features section is accessible via navigation link', async ({ page }) => {
    // Find the features navigation link
    const featuresLink = page.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click the features link
    await featuresLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify features section is now in view
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeInViewport();
  });

  test('Features section is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('file://' + process.cwd() + '/public/index.html');

    // Navigate to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify all feature cards are still visible
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Verify features are readable (no horizontal overflow)
    const sectionBox = await featuresSection.boundingBox();
    expect(sectionBox).toBeTruthy();
    expect(sectionBox!.width).toBeLessThanOrEqual(375);
  });
});

/**
 * Content Sections E2E Tests
 * Owner: Scenarios 2-9, 19 - All content sections
 *
 * Tests:
 * - Hero section content and CTA
 * - Features section cards and usage.gif
 * - Quick Start code blocks
 * - Architecture documentation
 * - API Reference commands
 * - Configuration parameters
 * - Performance information
 * - Contributing guidelines
 * - Asset loading (logo.gif, usage.gif)
 */

const { test, expect } = require('@playwright/test');

// ============================================================================
// Hero Section Tests (Scenario 2)
// ============================================================================

test.describe('Hero Section (Scenario 2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section contains clear value proposition', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check for value proposition elements
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check tagline mentions key value prop
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached');

    // Check description provides additional context
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();
  });

  test('TC2: Primary CTA button is visible and clickable', async ({ page }) => {
    // Find the CTA button
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Verify it's labeled appropriately (Quick Start or similar)
    await expect(ctaButton).toContainText(/Quick Start/i);

    // Verify it's clickable (has href attribute)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('quick-start');
  });

  test('TC3: Quick Start CTA navigates to Quick Start section', async ({ page }) => {
    // Click the CTA button
    const ctaButton = page.locator('.hero__cta');
    await ctaButton.click();

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify we're at the Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC4: Logo.gif is loaded and visible in hero section', async ({ page }) => {
    // Find the hero logo
    const heroLogo = page.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();

    // Verify it's an image with the correct source
    const src = await heroLogo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Verify the image has loaded (naturalWidth > 0)
    const isLoaded = await heroLogo.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('TC5: Scroll indicator exists in hero section', async ({ page }) => {
    // Find the scroll indicator
    const scrollIndicator = page.locator('.scroll-indicator');
    await expect(scrollIndicator).toBeVisible();

    // Verify it has visual elements (text and/or arrow)
    const scrollText = page.locator('.scroll-indicator__text');
    const scrollArrow = page.locator('.scroll-indicator__arrow');

    // At least one should be visible
    const hasText = await scrollText.isVisible();
    const hasArrow = await scrollArrow.isVisible();
    expect(hasText || hasArrow).toBe(true);
  });
});

// ============================================================================
// Features Section Tests (Scenario 3)
// ============================================================================

test.describe('Features Section (Scenario 3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have features section visible', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  test('should have at least 4 feature cards covering key capabilities', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    // Test Case 1: At least 4 feature cards present
    expect(count).toBeGreaterThanOrEqual(4);

    // Verify the 4 key features are present
    const memcachedCard = page.locator('.feature-card[data-feature="memcached-protocol"]');
    const persistenceCard = page.locator('.feature-card[data-feature="persistence"]');
    const lsmTreeCard = page.locator('.feature-card[data-feature="lsm-tree"]');
    const compactionCard = page.locator('.feature-card[data-feature="compaction"]');

    await expect(memcachedCard).toBeVisible();
    await expect(persistenceCard).toBeVisible();
    await expect(lsmTreeCard).toBeVisible();
    await expect(compactionCard).toBeVisible();
  });

  test('should have Memcached Protocol feature with correct content', async ({ page }) => {
    // Test Case 2: Verify Memcached Protocol feature content
    const memcachedCard = page.locator('.feature-card[data-feature="memcached-protocol"]');
    await expect(memcachedCard).toBeVisible();

    // Check title
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toContainText('Memcached Protocol');

    // Check description mentions memcached text protocol compatibility
    const description = memcachedCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toContain('memcached');
    expect(descriptionText.toLowerCase()).toMatch(/protocol|compatibility|compatible/);
  });

  test('should have Persistence feature with correct content', async ({ page }) => {
    // Test Case 3: Verify Persistence feature content
    const persistenceCard = page.locator('.feature-card[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Check title
    const title = persistenceCard.locator('.feature-title');
    await expect(title).toContainText('Persistence');

    // Check description mentions disk persistence and SSTables
    const description = persistenceCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toMatch(/disk|persist/);
    expect(descriptionText.toLowerCase()).toContain('sstable');
  });

  test('should have LSM Tree feature with correct content', async ({ page }) => {
    // Test Case 4: Verify LSM Tree feature content
    const lsmTreeCard = page.locator('.feature-card[data-feature="lsm-tree"]');
    await expect(lsmTreeCard).toBeVisible();

    // Check title
    const title = lsmTreeCard.locator('.feature-title');
    await expect(title).toContainText('LSM Tree');

    // Check description mentions Log-Structured Merge-tree, memtables, and SSTable levels
    const description = lsmTreeCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toMatch(/log-structured|merge-tree|lsm/);
    expect(descriptionText.toLowerCase()).toContain('memtable');
    expect(descriptionText.toLowerCase()).toMatch(/sstable|level/);
  });

  test('should have usage.gif loaded and visible in Features section', async ({ page }) => {
    // Test Case 5: Check usage.gif presence
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    // Verify the image source contains usage.gif
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');

    // Wait for the image to load (it's a large GIF ~6MB)
    await page.waitForFunction(() => {
      const img = document.getElementById('usage-gif');
      return img && img.complete && img.naturalWidth > 0;
    }, { timeout: 30000 });

    // Verify the image is properly loaded (naturalWidth > 0)
    const isLoaded = await usageGif.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('should have icons/visual elements on each feature card', async ({ page }) => {
    // Test Case 6: Verify feature cards have icons
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon');

      // Each card should have an icon container
      await expect(icon).toBeVisible();

      // The icon should contain an SVG element
      const svg = icon.locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('should navigate to features section from anchor link', async ({ page }) => {
    // Click the Features link in navigation
    await page.locator('a[href="#features"]').first().click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('should have proper section header', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const sectionTitle = featuresSection.locator('.section-title');
    const sectionSubtitle = featuresSection.locator('.section-subtitle');

    await expect(sectionTitle).toContainText('Features');
    await expect(sectionSubtitle).toBeVisible();
  });

  test('should have usage demo section', async ({ page }) => {
    const demoSection = page.locator('.features-demo');
    await expect(demoSection).toBeVisible();

    const demoTitle = demoSection.locator('.features-demo-title');
    await expect(demoTitle).toContainText('See it in Action');

    const gifContainer = demoSection.locator('.usage-gif-container');
    await expect(gifContainer).toBeVisible();
  });
});

// ============================================================================
// Placeholder tests for other sections
// ============================================================================

test.describe('Quick Start Section (Scenario 4)', () => {
  test.skip('Quick Start section shows installation commands', async ({ page }) => {
    // To be implemented by Scenario 4
  });
});

test.describe('Architecture Section (Scenario 5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section exists with clear heading', async ({ page }) => {
    // Navigate to Architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for clear heading
    const heading = architectureSection.locator('h2.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Architecture');
  });

  test('TC2: Section explains memtable as in-memory write buffer with skip list', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for memtable explanation
    const memtableContent = architectureSection.locator('.architecture-component[data-component="memtable"], .arch-component--memtable');
    await expect(memtableContent).toBeVisible();

    // Verify content mentions key concepts
    const memtableText = await architectureSection.textContent();
    expect(memtableText.toLowerCase()).toContain('memtable');
    expect(memtableText.toLowerCase()).toMatch(/in-memory|memory/);
    expect(memtableText.toLowerCase()).toMatch(/write|buffer/);
    expect(memtableText.toLowerCase()).toMatch(/skip.?list/);
  });

  test('TC3: Section explains SSTable as persistent on-disk storage format', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for SSTable explanation
    const sstableContent = architectureSection.locator('.architecture-component[data-component="sstable"], .arch-component--sstable');
    await expect(sstableContent).toBeVisible();

    // Verify content mentions key concepts
    const sstableText = await architectureSection.textContent();
    expect(sstableText.toLowerCase()).toContain('sstable');
    expect(sstableText.toLowerCase()).toMatch(/sorted.?string.?table|sorted string table/);
    expect(sstableText.toLowerCase()).toMatch(/disk|persistent|storage/);
  });

  test('TC4: Section explains Write-Ahead Log for durability and crash recovery', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for WAL explanation
    const walContent = architectureSection.locator('.architecture-component[data-component="wal"], .arch-component--wal');
    await expect(walContent).toBeVisible();

    // Verify content mentions key concepts
    const walText = await architectureSection.textContent();
    expect(walText.toLowerCase()).toMatch(/write.?ahead.?log|wal/);
    expect(walText.toLowerCase()).toMatch(/durability|durable/);
    expect(walText.toLowerCase()).toMatch(/crash|recovery/);
  });

  test('TC5: Section explains minor and major compaction', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for compaction explanation
    const compactionContent = architectureSection.locator('.architecture-component[data-component="compaction"], .arch-component--compaction');
    await expect(compactionContent).toBeVisible();

    // Verify content mentions both compaction types
    const compactionText = await architectureSection.textContent();
    expect(compactionText.toLowerCase()).toMatch(/minor.?compaction/);
    expect(compactionText.toLowerCase()).toMatch(/major.?compaction|level.?compaction/);
    expect(compactionText.toLowerCase()).toMatch(/memtable.*(to|->).*sstable|flush/i);
  });

  test('TC6: Visual diagram of LSM Tree architecture is present', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for architecture diagram container
    const diagramContainer = architectureSection.locator('.architecture-diagram').first();
    await expect(diagramContainer).toBeVisible();

    // Verify diagram has visual elements representing data flow
    const diagramContent = await diagramContainer.innerHTML();
    // Should contain either SVG elements or ASCII/text-based diagram elements
    const hasSvgElements = diagramContent.includes('<svg') || diagramContent.includes('<path') || diagramContent.includes('<rect');
    const hasTextDiagram = diagramContent.includes('data-flow') || diagramContent.includes('flow-arrow') || diagramContent.includes('arch-box');
    expect(hasSvgElements || hasTextDiagram || diagramContent.length > 100).toBe(true);
  });

  test('Architecture section is accessible via navigation', async ({ page }) => {
    // Click navigation link to Architecture
    await page.locator('a[href="#architecture"]').first().click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify section is in viewport
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });
});

test.describe('API Reference Section (Scenario 6)', () => {
  test.skip('API section documents Memcached commands', async ({ page }) => {
    // To be implemented by Scenario 6
  });
});

test.describe('Configuration Section (Scenario 7)', () => {
  test.skip('Configuration section shows parameters', async ({ page }) => {
    // To be implemented by Scenario 7
  });
});

test.describe('Performance Section (Scenario 8)', () => {
  test.skip('Performance section displays benchmarks', async ({ page }) => {
    // To be implemented by Scenario 8
  });
});

test.describe('Contributing Section (Scenario 9)', () => {
  test.skip('Contributing section shows guidelines', async ({ page }) => {
    // To be implemented by Scenario 9
  });
});

test.describe('Asset Integration (Scenario 19)', () => {
  test.skip('Usage.gif is loaded in features section', async ({ page }) => {
    // To be implemented by Scenario 19
  });
});

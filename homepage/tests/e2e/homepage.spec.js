/**
 * E2E tests for MirDB Homepage
 * Shared by multiple scenarios
 *
 * Test suites:
 * - Hero section tests (Scenario 1)
 * - Features display tests (Scenario 2)
 * - Code example tests (Scenario 3)
 * - Quick start tests (Scenario 5)
 * - Performance tests (Scenario 8)
 */

const { test, expect } = require('@playwright/test');

/**
 * Scenario 1: Hero Section and Branding Tests
 * Tests REQ-1, REQ-2, and Story 1
 */
test.describe('Hero Section and Branding', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page loads successfully with HTTP 200 status', async ({ page }) => {
    // Navigate to homepage and verify successful load
    const response = await page.goto('/');
    expect(response.status()).toBe(200);
  });

  test('TC2: MirDB logo image is present with correct src and alt text', async ({ page }) => {
    // Check for logo image in hero section
    const heroLogo = page.locator('#hero .hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify src contains 'logo'
    const src = await heroLogo.getAttribute('src');
    expect(src).toContain('logo');

    // Verify alt text is present
    const altText = await heroLogo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
  });

  test('TC3: H1 heading exists with text MirDB', async ({ page }) => {
    // Check for h1 element with MirDB text
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');
  });

  test('TC4: Tagline contains Persistent Key-Value Store and Memcached', async ({ page }) => {
    // Check for tagline element with required text
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached');
  });

  test('TC5: Get Started CTA button is present and clickable', async ({ page }) => {
    // Check for CTA button with Get Started text
    const ctaButton = page.locator('.hero-cta');
    await expect(ctaButton).toBeVisible();

    const ctaText = await ctaButton.textContent();
    expect(ctaText).toContain('Get Started');

    // Verify it's clickable (has href attribute)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify it links to quickstart section or GitHub
    expect(href === '#quickstart' || href.includes('github')).toBeTruthy();
  });

  test('TC6: Hero section is visible without scrolling on 1920x1080 viewport', async ({ page }) => {
    // Set viewport to 1920x1080
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Check that hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero is in viewport without scrolling
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.y).toBeGreaterThanOrEqual(0);
    expect(heroBox.y).toBeLessThan(1080); // Should be visible in viewport

    // Verify key elements are visible
    const heroLogo = page.locator('#hero .hero-logo');
    const heroTitle = page.locator('#hero .hero-title');
    const heroTagline = page.locator('#hero .hero-tagline');
    const heroCta = page.locator('#hero .hero-cta');

    await expect(heroLogo).toBeInViewport();
    await expect(heroTitle).toBeInViewport();
    await expect(heroTagline).toBeInViewport();
    await expect(heroCta).toBeInViewport();
  });
});

// ============================================
// SCENARIO 2: Features and Status Display Tests
// Tests REQ-3, REQ-4, and Story 2
// ============================================

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check for features section element
  test('features section exists with proper identifier', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  // Test Case 2: Check for Memcached protocol feature
  test('displays Memcached protocol feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const memcachedFeature = featuresSection.getByText(/Memcached protocol|Memcached compatible/i).first();
    await expect(memcachedFeature).toBeVisible();
  });

  // Test Case 3: Check for persistence feature
  test('displays persistence/SSTables feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const persistenceFeature = featuresSection.getByText(/persistence|SSTables/i).first();
    await expect(persistenceFeature).toBeVisible();
  });

  // Test Case 4: Check for LSM tree feature
  test('displays LSM tree feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const lsmFeature = featuresSection.getByText(/LSM tree|Log-Structured Merge/i).first();
    await expect(lsmFeature).toBeVisible();
  });

  // Test Case 5: Check for skip list feature
  test('displays skip list/memtable feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const skipListFeature = featuresSection.getByText(/skip list|memtable/i).first();
    await expect(skipListFeature).toBeVisible();
  });

  // Test Case 6: Check for compaction feature
  test('displays compaction feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const compactionFeature = featuresSection.getByText(/compaction/i).first();
    await expect(compactionFeature).toBeVisible();
  });

  // Test Case 10: Verify feature grid layout
  test('features are displayed in grid layout', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that it's using CSS grid or flexbox
    const display = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).display;
    });

    expect(['grid', 'flex']).toContain(display);
  });
});

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 7: Check for project status section
  test('status section exists with proper identifier', async ({ page }) => {
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Also check for Status heading
    const statusHeading = statusSection.getByRole('heading', { name: /Status/i });
    await expect(statusHeading).toBeVisible();
  });

  // Test Case 8: Verify implemented features have checkmarks
  test('implemented features have check indicators', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Check that Tokio is marked as implemented
    const tokioItem = statusSection.getByText(/Tokio/i);
    await expect(tokioItem).toBeVisible();

    // Check that skip list is marked as implemented
    const skipListItem = statusSection.getByText(/skip list/i);
    await expect(skipListItem).toBeVisible();

    // Check that minor compaction is marked as implemented
    const minorCompactionItem = statusSection.getByText(/minor compaction/i);
    await expect(minorCompactionItem).toBeVisible();

    // Check that major compaction is marked as implemented
    const majorCompactionItem = statusSection.getByText(/major compaction/i);
    await expect(majorCompactionItem).toBeVisible();

    // Verify checkmark indicators exist for implemented items
    const implementedChecks = statusSection.locator('.status-check.implemented');
    const checkCount = await implementedChecks.count();
    expect(checkCount).toBeGreaterThanOrEqual(4);
  });

  // Test Case 9: Verify raft is marked as planned
  test('raft is marked as planned or coming soon', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Check that Raft exists
    const raftItem = statusSection.getByText(/Raft/i).first();
    await expect(raftItem).toBeVisible();

    // Check for planned/coming soon indicator - use .first() since multiple elements may match
    const plannedIndicator = statusSection.getByText(/planned|coming soon/i).first();
    await expect(plannedIndicator).toBeVisible();
  });
});

/**
 * Scenario 3: Usage Examples Section Tests
 * Tests REQ-5, NFR-5, and Story 3
 */
test.describe('Usage Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section with id="usage" exists', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();
  });

  test('TC1 alt: Heading "Usage" exists', async ({ page }) => {
    const usageHeading = page.locator('#usage .section-title');
    await expect(usageHeading).toHaveText('Usage');
  });

  test('TC2: Pre or code element with example commands exists', async ({ page }) => {
    const codeBlock = page.locator('#usage .code-block code');
    await expect(codeBlock.first()).toBeVisible();
  });

  test('TC3: Code example contains "set" Memcached command', async ({ page }) => {
    const codeContent = page.locator('#usage .code-block code');
    await expect(codeContent.first()).toContainText('set');
  });

  test('TC4: Code example contains "get" Memcached command', async ({ page }) => {
    const codeContent = page.locator('#usage .code-block code');
    const allCodeBlocks = await codeContent.allTextContents();
    const hasGetCommand = allCodeBlocks.some(text => text.includes('get'));
    expect(hasGetCommand).toBe(true);
  });

  test('TC5: Code block has syntax highlighting classes', async ({ page }) => {
    // Check for syntax highlighting classes
    const syntaxKeyword = page.locator('#usage .syntax-keyword');
    const syntaxString = page.locator('#usage .syntax-string');
    const syntaxVariable = page.locator('#usage .syntax-variable');

    // At least one of these highlighting classes should exist
    const keywordCount = await syntaxKeyword.count();
    const stringCount = await syntaxString.count();
    const variableCount = await syntaxVariable.count();

    expect(keywordCount + stringCount + variableCount).toBeGreaterThan(0);
  });

  test('TC6: Copy button exists near code block', async ({ page }) => {
    const copyButton = page.locator('#usage .copy-btn');
    await expect(copyButton.first()).toBeVisible();
  });

  test('TC6 alt: Copy button has "Copy" text or copy icon', async ({ page }) => {
    const copyButton = page.locator('#usage .copy-btn').first();
    const copyText = copyButton.locator('.copy-text');
    const copyIcon = copyButton.locator('.copy-icon');

    // Either copy text or copy icon should be present
    const hasText = await copyText.count() > 0;
    const hasIcon = await copyIcon.count() > 0;

    expect(hasText || hasIcon).toBe(true);
  });

  test('TC7: Click copy button copies code to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('#usage .copy-btn').first();
    await copyButton.click();

    // Check clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Clipboard should contain some code (set command)
    expect(clipboardContent).toContain('set');
  });

  test('TC8: Visual feedback appears after copying', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('#usage .copy-btn').first();
    await copyButton.click();

    // Check for visual feedback - either "copied" class or "Copied!" text
    const hasCopiedClass = await copyButton.evaluate(btn => btn.classList.contains('copied'));
    const copyText = copyButton.locator('.copy-text');
    const textContent = await copyText.textContent();

    // Either the button has the "copied" class or displays "Copied!" text
    expect(hasCopiedClass || textContent === 'Copied!').toBe(true);
  });

  test('TC8 alt: Check icon appears after copying', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('#usage .copy-btn').first();
    await copyButton.click();

    // When button has "copied" class, check-icon should be visible
    const hasCopiedClass = await copyButton.evaluate(btn => btn.classList.contains('copied'));

    if (hasCopiedClass) {
      // Verify CSS makes check-icon visible (via display property computed style)
      const checkIconVisible = await copyButton.locator('.check-icon').evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.display !== 'none';
      });
      expect(checkIconVisible).toBe(true);
    } else {
      // If no class, just verify feedback text changed
      const copyText = await copyButton.locator('.copy-text').textContent();
      expect(copyText).toBe('Copied!');
    }
  });
});

// Placeholder for Scenario 5: Quick Start Tests
test.describe.skip('Quick Start and Documentation', () => {
  // To be implemented by Scenario 5
});

// Placeholder for Scenario 8: Performance Tests
test.describe.skip('Performance and Load Time', () => {
  // To be implemented by Scenario 8
});

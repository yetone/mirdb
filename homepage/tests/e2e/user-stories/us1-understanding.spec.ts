/**
 * E2E Tests for US-1: Understanding the Product
 * Owner: Scenario 17 - User Story - Understanding the Product
 *
 * Verifies that Alex (Software Architect evaluating options) can quickly
 * understand what MirDB is and its key differentiators within 30 seconds.
 *
 * Test Cases:
 * 1. Hero section shows clear explanation of MirDB purpose
 * 2. Features section prominently displays persistence as key differentiator
 * 3. Page content mentions Rust and LSM tree architecture
 *
 * Requirements: REQ-2, REQ-3, REQ-4
 */

import { test, expect } from '@playwright/test';

test.describe('US-1: Understanding the Product', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC-1: Hero section displays clear explanation of MirDB purpose', async ({ page }) => {
    // Verify hero section is visible within viewport
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify MirDB name/branding is prominently displayed
    const heading = heroSection.locator('h1');
    await expect(heading).toContainText('MirDB');
    await expect(heading).toBeVisible();

    // Verify tagline explains purpose clearly
    const tagline = heroSection.locator('p').first();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Compatibility');
    await expect(tagline).toBeVisible();

    // Verify key information is within initial viewport (above fold)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      // Hero section should start near the top of the page
      expect(heroBox.y).toBeLessThan(200);
    }

    // Verify CTAs are present for immediate action
    const getStartedButton = heroSection.locator('text=Get Started');
    await expect(getStartedButton).toBeVisible();

    const githubButton = heroSection.locator('text=View on GitHub');
    await expect(githubButton).toBeVisible();
  });

  test('TC-2: Features section prominently displays persistence as key differentiator from memcached', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify section heading
    const featuresHeading = featuresSection.locator('h2');
    await expect(featuresHeading).toContainText('Why Choose MirDB');

    // Verify persistence feature is displayed
    const persistentStorageCard = featuresSection.locator('[data-testid="feature-card-persistent-storage"]');
    await expect(persistentStorageCard).toBeVisible();

    // Verify persistence description mentions key differentiator
    const persistenceText = await persistentStorageCard.textContent();
    expect(persistenceText).toContain('Persistent Storage');
    expect(persistenceText).toContain('LSM tree');
    expect(persistenceText).toContain('persists');

    // Verify memcached compatibility feature is displayed
    const memcachedCard = featuresSection.locator('[data-testid="feature-card-memcached-compatible"]');
    await expect(memcachedCard).toBeVisible();

    const memcachedText = await memcachedCard.textContent();
    expect(memcachedText).toContain('Memcached Compatible');
    expect(memcachedText).toContain('Drop-in replacement');

    // Verify feature grid layout (3 cards visible)
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);
  });

  test('TC-3: Page content mentions Rust and LSM tree architecture', async ({ page }) => {
    // Check for Rust mention in features section
    const featuresSection = page.locator('#features');
    const rustCard = featuresSection.locator('[data-testid="feature-card-rust-performance"]');
    await expect(rustCard).toBeVisible();

    const rustText = await rustCard.textContent();
    expect(rustText).toContain('Rust Performance');
    expect(rustText).toContain('Built in Rust');

    // Check for LSM tree mention in features section (persistent storage card)
    const persistentCard = featuresSection.locator('[data-testid="feature-card-persistent-storage"]');
    const persistentText = await persistentCard.textContent();
    expect(persistentText).toContain('LSM tree');

    // Check How It Works section for detailed LSM tree architecture info
    const howItWorksSection = page.locator('#how-it-works');
    await howItWorksSection.scrollIntoViewIfNeeded();
    await expect(howItWorksSection).toBeVisible();

    // Verify LSM tree is explained
    const howItWorksText = await howItWorksSection.textContent();
    expect(howItWorksText).toContain('LSM');
    expect(howItWorksText).toContain('Log-Structured Merge');

    // Verify comparison table mentions Rust
    const comparisonTable = howItWorksSection.locator('table');
    await expect(comparisonTable).toBeVisible();
    const tableText = await comparisonTable.textContent();
    expect(tableText).toContain('Rust');

    // Verify architecture diagram is present
    const architectureDiagram = howItWorksSection.locator('img[alt*="Architecture"]');
    await expect(architectureDiagram).toBeVisible();
  });

  test('User can understand MirDB within 30 seconds (combined flow)', async ({ page }) => {
    // This test simulates a first-time visitor experience
    // and verifies all key information is accessible quickly

    const startTime = Date.now();

    // Step 1: Land on homepage and see hero
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();
    await expect(heroSection.locator('h1')).toContainText('MirDB');
    await expect(heroSection.locator('p').first()).toContainText('Persistent Key-Value Store');

    // Step 2: Scroll to features (should be immediately below hero)
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify key differentiators are visible
    await expect(featuresSection.locator('[data-testid="feature-card-persistent-storage"]')).toBeVisible();
    await expect(featuresSection.locator('[data-testid="feature-card-memcached-compatible"]')).toBeVisible();
    await expect(featuresSection.locator('[data-testid="feature-card-rust-performance"]')).toBeVisible();

    // Step 3: Verify core concepts are clear
    const pageContent = await page.content();

    // Memcached compatible
    expect(pageContent).toContain('Memcached');

    // Persistent
    expect(pageContent).toContain('Persistent');

    // Rust
    expect(pageContent).toContain('Rust');

    // LSM tree
    expect(pageContent).toContain('LSM');

    const elapsed = Date.now() - startTime;

    // Verify the entire verification took less than 30 seconds
    // Note: This is a sanity check; actual user comprehension time is subjective
    expect(elapsed).toBeLessThan(30000);
  });
});

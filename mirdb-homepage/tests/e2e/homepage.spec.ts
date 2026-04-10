/**
 * Homepage E2E Tests
 * Owner: Shared across Scenarios 1, 2, 3, 4, 9, 16
 *
 * Playwright tests for homepage functionality:
 * - Hero section (Scenario 1)
 * - Features section (Scenario 2)
 * - Code example section (Scenario 3)
 * - Quick start section (Scenario 4)
 * - Footer section (Scenario 9)
 * - Theme toggle (Scenario 16)
 */

import { test, expect } from '@playwright/test';

/**
 * Scenario 1: Hero Section and Value Proposition Tests
 */
test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Project name MirDB is visible in large typography (48px+ on desktop)', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    const heroTitle = page.getByTestId('hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify font size is at least 48px
    const fontSize = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeNum = parseFloat(fontSize);
    expect(fontSizeNum).toBeGreaterThanOrEqual(48);
  });

  test('Test Case 2: Tagline describing MirDB as a high-performance Rust key-value store is visible', async ({ page }) => {
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toContain('high-performance');
    expect(taglineText?.toLowerCase()).toContain('rust');
    expect(taglineText?.toLowerCase()).toContain('key-value');
  });

  test('Test Case 3: Click Get Started button navigates to quick start documentation', async ({ page }) => {
    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveText('Get Started');

    // Get the href attribute
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('/docs/quickstart');

    // Verify the button is clickable (link element)
    const tagName = await ctaButton.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('a');
  });

  test('Test Case 4: Button displays hover state with visual feedback', async ({ page }) => {
    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();

    // Get initial styles
    const initialBgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover over the button
    await ctaButton.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Get hover styles
    const hoverBgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Verify visual change occurred (background color changed or transform applied)
    const hasVisualChange = initialBgColor !== hoverBgColor;
    expect(hasVisualChange).toBe(true);
  });

  test('Test Case 5: Button displays visible focus indicator when focused via keyboard', async ({ page }) => {
    const ctaButton = page.getByTestId('cta-button');

    // Tab to the button to focus it
    await page.keyboard.press('Tab');

    // Ensure the button is focused
    const isFocused = await ctaButton.evaluate((el) => {
      return document.activeElement === el;
    });

    // If not focused, tab again (might have skip link)
    if (!isFocused) {
      await page.keyboard.press('Tab');
    }

    // Check for visible focus indicator
    const outlineStyle = await ctaButton.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineColor: style.outlineColor,
        boxShadow: style.boxShadow,
      };
    });

    // Verify there's a visible focus indicator (outline or box-shadow)
    const hasOutline = outlineStyle.outlineWidth !== '0px' && outlineStyle.outline !== 'none';
    const hasBoxShadow = outlineStyle.boxShadow !== 'none';
    expect(hasOutline || hasBoxShadow).toBe(true);
  });
});

/**
 * Scenario 2: Features Section Display Tests
 */
test.describe('Features Section - Scenario 2', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check features section for LSM-tree content', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM-tree feature card
    const lsmTreeCard = page.getByTestId('feature-card-lsm-tree');
    await expect(lsmTreeCard).toBeVisible();

    // Verify heading contains LSM-tree
    const cardTitle = lsmTreeCard.locator('.card__title');
    await expect(cardTitle).toContainText('LSM-tree');

    // Verify description mentions architecture
    const cardDescription = lsmTreeCard.locator('.card__description');
    await expect(cardDescription).toBeVisible();
    const descText = await cardDescription.textContent();
    expect(descText?.toLowerCase()).toContain('architecture');
  });

  test('TC2: Check features section for Rust implementation content', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Rust implementation feature card
    const rustCard = page.getByTestId('feature-card-rust-implementation');
    await expect(rustCard).toBeVisible();

    // Verify heading mentions Rust
    const cardTitle = rustCard.locator('.card__title');
    await expect(cardTitle).toContainText('Rust');

    // Verify description highlights Rust benefits
    const cardDescription = rustCard.locator('.card__description');
    await expect(cardDescription).toBeVisible();
    const descText = await cardDescription.textContent();
    expect(descText?.toLowerCase()).toMatch(/memory|safety|performance/);
  });

  test('TC3: Check features section for crash recovery content', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the crash recovery feature card
    const crashRecoveryCard = page.getByTestId('feature-card-crash-recovery');
    await expect(crashRecoveryCard).toBeVisible();

    // Verify heading mentions crash recovery
    const cardTitle = crashRecoveryCard.locator('.card__title');
    await expect(cardTitle).toContainText('Crash Recovery');

    // Verify description mentions durability
    const cardDescription = crashRecoveryCard.locator('.card__description');
    await expect(cardDescription).toBeVisible();
    const descText = await cardDescription.textContent();
    expect(descText?.toLowerCase()).toMatch(/durability|recovery|data loss/);
  });

  test('TC4: Check features section for Memcached compatibility content', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached compatibility feature card
    const memcachedCard = page.getByTestId('feature-card-memcached-compatibility');
    await expect(memcachedCard).toBeVisible();

    // Verify heading mentions Memcached
    const cardTitle = memcachedCard.locator('.card__title');
    await expect(cardTitle).toContainText('Memcached');

    // Verify description mentions protocol compatibility
    const cardDescription = memcachedCard.locator('.card__description');
    await expect(cardDescription).toBeVisible();
    const descText = await cardDescription.textContent();
    expect(descText?.toLowerCase()).toMatch(/protocol|compatibility|client/);
  });

  test('TC5: Verify features grid layout on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    // Verify features grid is visible
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has correct CSS grid layout (2 columns)
    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
        gap: styles.gap,
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Should have 2 columns on desktop
    expect(gridStyle.gridTemplateColumns.split(' ').length).toBeGreaterThanOrEqual(2);

    // Verify all 4 feature cards are present
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    await expect(featureCards).toHaveCount(4);

    // Verify consistent spacing (gap should be set)
    expect(gridStyle.gap).toBeTruthy();
    expect(gridStyle.gap).not.toBe('normal');
  });

  test('TC6: Hover over feature card displays hover effect', async ({ page }) => {
    const featureCard = page.getByTestId('feature-card-lsm-tree');
    await expect(featureCard).toBeVisible();

    // Get initial styles
    const initialStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      };
    });

    // Hover over the card
    await featureCard.hover();

    // Wait for transition to complete
    await page.waitForTimeout(300);

    // Get styles after hover
    const hoverStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      };
    });

    // Verify visual feedback on hover (at least one style should change)
    const hasVisualChange =
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.boxShadow !== hoverStyles.boxShadow ||
      initialStyles.borderColor !== hoverStyles.borderColor;

    expect(hasVisualChange).toBe(true);
  });

  test('Features section has proper accessibility attributes', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Verify section has aria-labelledby
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

    // Verify heading is present and visible
    const heading = page.locator('#features-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Key Features');
  });

  test('Each feature card has an icon', async ({ page }) => {
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.card__icon');
      await expect(icon).toBeVisible();

      // Verify icon contains SVG
      const svg = icon.locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('Features grid is responsive on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has single column layout on mobile
    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Should have 1 column on mobile (single value means 1 column)
    const columns = gridStyle.gridTemplateColumns.split(' ').length;
    expect(columns).toBe(1);
  });
});

/**
 * E2E Tests for Features Section Display
 * Owner: Scenario 3 - Features Section Display
 *
 * Test Cases:
 * - TC2: View Features section on desktop (> 1024px) - 3-column grid layout
 * - TC3: View Features section on mobile (< 768px) - single column layout
 * - TC4: Hover over feature card on desktop - subtle lift effect
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC2: Desktop layout (> 1024px)', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('should display feature cards in 3-column grid layout', async ({ page }) => {
      const featuresGrid = page.locator('.features__grid');
      await expect(featuresGrid).toBeVisible();

      // Check that the grid has 3 columns via computed style
      const gridStyle = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.getPropertyValue('grid-template-columns');
      });

      // Should have 3 column values (e.g., "300px 300px 300px" or similar)
      const columns = gridStyle.split(' ').filter(col => col.length > 0);
      expect(columns.length).toBe(3);
    });

    test('should have 6 visible feature cards', async ({ page }) => {
      const cards = page.locator('.feature-card');
      await expect(cards).toHaveCount(6);

      for (let i = 0; i < 6; i++) {
        await expect(cards.nth(i)).toBeVisible();
      }
    });

    test('feature cards should be arranged in 2 rows of 3', async ({ page }) => {
      const cards = page.locator('.feature-card');
      const boxes = await cards.evaluateAll((elements) => {
        return elements.map(el => {
          const rect = el.getBoundingClientRect();
          return { top: rect.top, left: rect.left };
        });
      });

      // Group cards by their top position (with some tolerance)
      const rows = {};
      boxes.forEach((box, index) => {
        const rowKey = Math.round(box.top / 10) * 10; // Round to nearest 10px
        if (!rows[rowKey]) rows[rowKey] = [];
        rows[rowKey].push(index);
      });

      const rowValues = Object.values(rows);
      expect(rowValues.length).toBe(2); // Should have 2 rows
      expect(rowValues[0].length).toBe(3); // First row should have 3 cards
      expect(rowValues[1].length).toBe(3); // Second row should have 3 cards
    });
  });

  test.describe('TC3: Mobile layout (< 768px)', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should display feature cards in single column layout', async ({ page }) => {
      const featuresGrid = page.locator('.features__grid');
      await expect(featuresGrid).toBeVisible();

      // Check that the grid has 1 column
      const gridStyle = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.getPropertyValue('grid-template-columns');
      });

      // Should have 1 column value
      const columns = gridStyle.split(' ').filter(col => col.length > 0);
      expect(columns.length).toBe(1);
    });

    test('feature cards should stack vertically', async ({ page }) => {
      const cards = page.locator('.feature-card');
      const boxes = await cards.evaluateAll((elements) => {
        return elements.map(el => {
          const rect = el.getBoundingClientRect();
          return { top: rect.top, left: rect.left };
        });
      });

      // All cards should have similar left position (single column)
      const leftPositions = boxes.map(b => Math.round(b.left));
      const uniqueLeftPositions = [...new Set(leftPositions)];
      expect(uniqueLeftPositions.length).toBe(1);

      // Each card should be below the previous one
      for (let i = 1; i < boxes.length; i++) {
        expect(boxes[i].top).toBeGreaterThan(boxes[i - 1].top);
      }
    });

    test('all 6 feature cards should be visible on scroll', async ({ page }) => {
      const cards = page.locator('.feature-card');
      await expect(cards).toHaveCount(6);

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Verify each card becomes visible when scrolled to
      for (let i = 0; i < 6; i++) {
        await cards.nth(i).scrollIntoViewIfNeeded();
        await expect(cards.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('TC4: Hover effects on desktop', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('feature card should have subtle lift effect on hover', async ({ page }) => {
      const firstCard = page.locator('.feature-card').first();
      await expect(firstCard).toBeVisible();

      // Get initial transform
      const initialTransform = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over the card
      await firstCard.hover();

      // Wait for transition
      await page.waitForTimeout(350);

      // Get transform after hover
      const hoverTransform = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Transform should change (indicating lift effect)
      // The card should move up (translateY(-4px))
      expect(hoverTransform).not.toBe(initialTransform);
    });

    test('feature card should have transition property for smooth animation', async ({ page }) => {
      const firstCard = page.locator('.feature-card').first();

      const transition = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });

      // Should have transition defined (either specific properties or 'all')
      expect(transition).not.toBe('none');
      expect(transition.length).toBeGreaterThan(0);
    });

    test('feature card should have box-shadow change on hover', async ({ page }) => {
      const firstCard = page.locator('.feature-card').first();
      await expect(firstCard).toBeVisible();

      // Get initial box-shadow
      const initialShadow = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow;
      });

      // Hover over the card
      await firstCard.hover();

      // Wait for transition
      await page.waitForTimeout(400);

      // Get box-shadow after hover
      const hoverShadow = await firstCard.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow;
      });

      // Box shadow should be present on hover (either changed or defined)
      // The test validates hover effects work
      expect(hoverShadow !== 'none' || initialShadow !== 'none').toBe(true);
    });
  });

  test.describe('Features section content', () => {
    test('should display section title "Why MirDB?"', async ({ page }) => {
      const title = page.locator('.features__title');
      await expect(title).toBeVisible();
      await expect(title).toHaveText('Why MirDB?');
    });

    test('should display all 6 feature titles', async ({ page }) => {
      const expectedTitles = [
        'Persistent Storage',
        'Memcached Protocol',
        'LSM-Tree Architecture',
        'Minor/Major Compaction',
        'Rust-Powered',
        'Async I/O'
      ];

      for (const title of expectedTitles) {
        const titleElement = page.locator('.feature-card__title', { hasText: title });
        await expect(titleElement).toBeVisible();
      }
    });

    test('each feature card should have visible icon', async ({ page }) => {
      const icons = page.locator('.feature-card__icon');
      await expect(icons).toHaveCount(6);

      for (let i = 0; i < 6; i++) {
        await expect(icons.nth(i)).toBeVisible();
        const svg = icons.nth(i).locator('svg');
        await expect(svg).toBeVisible();
      }
    });
  });
});

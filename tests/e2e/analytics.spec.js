/**
 * Analytics Counter Display Tests
 * Owner: Scenario 12 - Analytics Counter Display
 *
 * Tests:
 * - Counter DOM presence and content
 * - Number formatting with K/M/B suffixes
 * - API data fetching with loading/error states
 * - Visibility and contrast in both light and dark themes
 * - Responsive layout across breakpoints
 */

const { test, expect } = require('@playwright/test');

/**
 * Extract a normalized brightness (0-255) from a computed color string.
 */
function getBrightness(colorStr) {
  if (!colorStr) return null;

  const rgbMatch = colorStr.match(/rgba?\((\d+(?:\.\d+)?),\s*(\d+(?:\.\d+)?),\s*(\d+(?:\.\d+)?)/);
  if (rgbMatch) {
    const r = parseFloat(rgbMatch[1]);
    const g = parseFloat(rgbMatch[2]);
    const b = parseFloat(rgbMatch[3]);
    return (r + g + b) / 3;
  }

  const oklchMatch = colorStr.match(/oklch\(([\d.]+)/);
  if (oklchMatch) {
    const l = parseFloat(oklchMatch[1]);
    return l * 255;
  }

  const oklabMatch = colorStr.match(/oklab\(([\d.]+)/);
  if (oklabMatch) {
    const l = parseFloat(oklabMatch[1]);
    return l * 255;
  }

  const hslMatch = colorStr.match(/hsl\([\d.]+,\s*[\d.]+%?,\s*([\d.]+)%?\)/);
  if (hslMatch) {
    const l = parseFloat(hslMatch[1]);
    return (l / 100) * 255;
  }

  return null;
}

test.describe('Analytics Counter Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    // Wait for counter JS to initialize
    await page.waitForTimeout(300);
  });

  // ============================================================
  // Test Case 1: Analytics counter DOM presence (E2E)
  // ============================================================
  test.describe('Counter DOM Presence', () => {
    test('should render analytics section on the homepage', async ({ page }) => {
      const analyticsSection = page.locator('[data-testid="analytics-section"]');
      await expect(analyticsSection).toBeVisible();
    });

    test('should display section heading and subheading', async ({ page }) => {
      const heading = page.locator('[data-testid="analytics-heading"]');
      await expect(heading).toBeVisible();

      const headingText = await heading.textContent();
      expect(headingText).toBeTruthy();
      expect(headingText.toLowerCase()).toContain('trust');

      const subheading = page.locator('[data-testid="analytics-subheading"]');
      await expect(subheading).toBeVisible();

      const subheadingText = await subheading.textContent();
      expect(subheadingText).toBeTruthy();
      expect(subheadingText.length).toBeGreaterThan(10);
    });

    test('should display three stat cards with labels', async ({ page }) => {
      const grid = page.locator('[data-testid="analytics-grid"]');
      await expect(grid).toBeVisible();

      // Check URLs stat card
      const urlsCard = page.locator('[data-testid="stat-card-urls"]');
      await expect(urlsCard).toBeVisible();
      const urlsLabel = urlsCard.locator('[data-testid="stat-label-urls"]');
      await expect(urlsLabel).toBeVisible();
      const urlsLabelText = await urlsLabel.textContent();
      expect(urlsLabelText.toLowerCase()).toContain('url');

      // Check Users stat card
      const usersCard = page.locator('[data-testid="stat-card-users"]');
      await expect(usersCard).toBeVisible();
      const usersLabel = usersCard.locator('[data-testid="stat-label-users"]');
      await expect(usersLabel).toBeVisible();
      const usersLabelText = await usersLabel.textContent();
      expect(usersLabelText.toLowerCase()).toContain('user');

      // Check Clicks stat card
      const clicksCard = page.locator('[data-testid="stat-card-clicks"]');
      await expect(clicksCard).toBeVisible();
      const clicksLabel = clicksCard.locator('[data-testid="stat-label-clicks"]');
      await expect(clicksLabel).toBeVisible();
      const clicksLabelText = await clicksLabel.textContent();
      expect(clicksLabelText.toLowerCase()).toContain('click');
    });

    test('should display meaningful stat values after loading', async ({ page }) => {
      // Wait for the stat values to be populated (they start as "--")
      await page.waitForFunction(() => {
        const el = document.querySelector('[data-stat-key="urls_created"]');
        return el && el.textContent !== '--';
      }, { timeout: 5000 });

      const urlsValue = page.locator('[data-testid="stat-value-urls"]');
      await expect(urlsValue).toBeVisible();
      const urlsText = await urlsValue.textContent();
      expect(urlsText).toBeTruthy();
      // Should contain a number with optional suffix
      expect(urlsText).toMatch(/[\d.]+[KMB+]?/);

      const usersValue = page.locator('[data-testid="stat-value-users"]');
      await expect(usersValue).toBeVisible();
      const usersText = await usersValue.textContent();
      expect(usersText).toBeTruthy();
      expect(usersText).toMatch(/[\d.]+[KMB+]?/);

      const clicksValue = page.locator('[data-testid="stat-value-clicks"]');
      await expect(clicksValue).toBeVisible();
      const clicksText = await clicksValue.textContent();
      expect(clicksText).toBeTruthy();
      expect(clicksText).toMatch(/[\d.]+[KMB+]?/);
    });

    test('should have aria-live regions for accessibility', async ({ page }) => {
      const statValues = page.locator('[data-testid^="stat-value"]');
      const count = await statValues.count();
      expect(count).toBe(3);

      for (let i = 0; i < count; i++) {
        const ariaLive = await statValues.nth(i).getAttribute('aria-live');
        expect(ariaLive).toBe('polite');
      }
    });

    test('should have semantic section structure with aria-labelledby', async ({ page }) => {
      const section = page.locator('section#analytics');
      await expect(section).toBeVisible();

      const ariaLabelledBy = await section.getAttribute('aria-labelledby');
      expect(ariaLabelledBy).toBe('analytics-heading');

      const heading = page.locator('#analytics-heading');
      await expect(heading).toBeVisible();
    });
  });

  // ============================================================
  // Test Case 2: Counter number formatting (Unit)
  // ============================================================
  test.describe('Counter Number Formatting', () => {
    test('should format thousands with K+ suffix', async ({ page }) => {
      const results = await page.evaluate(() => {
        const format = window.App.Stats.formatNumber;
        return {
          oneThousand: format(1000),
          fifteenHundred: format(1500),
          oneHundredTwentyEightK: format(128456),
          nineHundredNinetyNine: format(999),
        };
      });

      expect(results.oneThousand).toBe('1K+');
      expect(results.fifteenHundred).toBe('1.5K+');
      expect(results.oneHundredTwentyEightK).toBe('128.5K+');
      expect(results.nineHundredNinetyNine).toBe('999');
    });

    test('should format millions with M+ suffix', async ({ page }) => {
      const results = await page.evaluate(() => {
        const format = window.App.Stats.formatNumber;
        return {
          oneMillion: format(1000000),
          onePointFiveMillion: format(1500000),
          eightPointNineMillion: format(8923456),
        };
      });

      expect(results.oneMillion).toBe('1M+');
      expect(results.onePointFiveMillion).toBe('1.5M+');
      expect(results.eightPointNineMillion).toBe('8.9M+');
    });

    test('should format billions with B+ suffix', async ({ page }) => {
      const results = await page.evaluate(() => {
        const format = window.App.Stats.formatNumber;
        return {
          oneBillion: format(1000000000),
          onePointFiveBillion: format(1500000000),
          twoBillion: format(2000000000),
        };
      });

      expect(results.oneBillion).toBe('1B+');
      expect(results.onePointFiveBillion).toBe('1.5B+');
      expect(results.twoBillion).toBe('2B+');
    });

    test('should handle small numbers without suffix', async ({ page }) => {
      const results = await page.evaluate(() => {
        const format = window.App.Stats.formatNumber;
        return {
          zero: format(0),
          one: format(1),
          fiveHundred: format(500),
        };
      });

      expect(results.zero).toBe('0');
      expect(results.one).toBe('1');
      expect(results.fiveHundred).toBe('500');
    });

    test('should handle invalid inputs gracefully', async ({ page }) => {
      const results = await page.evaluate(() => {
        const format = window.App.Stats.formatNumber;
        return {
          nullValue: format(null),
          undefinedValue: format(undefined),
          stringValue: format('not a number'),
        };
      });

      expect(results.nullValue).toBe('--');
      expect(results.undefinedValue).toBe('--');
      expect(results.stringValue).toBe('--');
    });
  });

  // ============================================================
  // Test Case 3: Counter API data fetch (Integration)
  // ============================================================
  test.describe('Counter API Data Fetch', () => {
    test('should fetch stats from API and display formatted values', async ({ page }) => {
      // Mock the API response
      await page.route('/api/stats', async (route) => {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            urls_created: 256789,
            active_users: 5432,
            total_clicks: 15678901,
          }),
        });
      });

      // Reload to trigger fresh fetch
      await page.reload();
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Wait for values to update from the mocked API
      await page.waitForFunction(() => {
        const el = document.querySelector('[data-stat-key="urls_created"]');
        return el && el.textContent !== '--';
      }, { timeout: 5000 });

      // Verify the formatted values from the mock API
      const urlsValue = await page.locator('[data-stat-key="urls_created"]').textContent();
      expect(urlsValue).toBe('256.8K+');

      const usersValue = await page.locator('[data-stat-key="active_users"]').textContent();
      expect(usersValue).toBe('5.4K+');

      const clicksValue = await page.locator('[data-stat-key="total_clicks"]').textContent();
      expect(clicksValue).toBe('15.7M+');
    });

    test('should handle API errors gracefully with fallback values', async ({ page }) => {
      // Mock the API to return an error
      await page.route('/api/stats', async (route) => {
        await route.fulfill({
          status: 500,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'Internal Server Error' }),
        });
      });

      // Reload to trigger fresh fetch
      await page.reload();
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Wait for fallback values to be applied
      await page.waitForFunction(() => {
        const el = document.querySelector('[data-stat-key="urls_created"]');
        return el && el.textContent !== '--';
      }, { timeout: 5000 });

      // Verify fallback values are displayed
      const urlsValue = await page.locator('[data-stat-key="urls_created"]').textContent();
      expect(urlsValue).toBe('128.5K+');

      const usersValue = await page.locator('[data-stat-key="active_users"]').textContent();
      expect(usersValue).toBe('3.4K+');

      const clicksValue = await page.locator('[data-stat-key="total_clicks"]').textContent();
      expect(clicksValue).toBe('8.9M+');
    });

    test('should handle network errors with fallback values', async ({ page }) => {
      // Mock the API to abort the request
      await page.route('/api/stats', async (route) => {
        await route.abort('failed');
      });

      // Reload to trigger fresh fetch
      await page.reload();
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(300);

      // Wait for fallback values to be applied
      await page.waitForFunction(() => {
        const el = document.querySelector('[data-stat-key="urls_created"]');
        return el && el.textContent !== '--';
      }, { timeout: 5000 });

      // Verify fallback values are displayed
      const urlsValue = await page.locator('[data-stat-key="urls_created"]').textContent();
      expect(urlsValue).toBe('128.5K+');
    });

    test('should show loading state during fetch', async ({ page }) => {
      // Create a delayed response to observe loading state
      await page.route('/api/stats', async (route) => {
        await new Promise(resolve => setTimeout(resolve, 500));
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            urls_created: 1000,
            active_users: 500,
            total_clicks: 10000,
          }),
        });
      });

      // Set initial state to '--' before reload
      await page.evaluate(() => {
        document.querySelectorAll('.stat-number').forEach(el => {
          el.textContent = '--';
        });
      });

      // Reload and immediately check loading state
      await page.reload();
      await page.waitForLoadState('domcontentloaded');

      // The stat values should be '--' while loading
      const urlsValue = await page.locator('[data-stat-key="urls_created"]').textContent();
      expect(urlsValue).toBe('--');
    });
  });

  // ============================================================
  // Test Case 4: Counter visibility in both themes (E2E)
  // ============================================================
  test.describe('Counter Theme Visibility', () => {
    test('should have sufficient contrast in light mode', async ({ page }) => {
      // Ensure light mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
        localStorage.setItem('theme', 'light');
      });
      await page.waitForTimeout(300);

      const urlsCard = page.locator('[data-testid="stat-card-urls"]');
      await expect(urlsCard).toBeVisible();

      // Check card background brightness (should be light)
      const cardBg = await urlsCard.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });
      const cardBrightness = getBrightness(cardBg);
      expect(cardBrightness).not.toBeNull();
      expect(cardBrightness).toBeGreaterThan(200);

      // Check stat value text brightness (should be dark for readability)
      const statValue = urlsCard.locator('[data-testid="stat-value-urls"]');
      const textColor = await statValue.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.color;
      });
      const textBrightness = getBrightness(textColor);
      expect(textBrightness).not.toBeNull();
      // Primary color text should have reasonable contrast
      expect(textBrightness).toBeGreaterThan(50);

      // Check label text brightness (should be darker/muted)
      const statLabel = urlsCard.locator('[data-testid="stat-label-urls"]');
      const labelColor = await statLabel.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.color;
      });
      const labelBrightness = getBrightness(labelColor);
      expect(labelBrightness).not.toBeNull();
      expect(labelBrightness).toBeLessThan(150);
    });

    test('should have sufficient contrast in dark mode', async ({ page }) => {
      // Switch to dark mode
      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible').first();
      if (await toggleBtn.isVisible().catch(() => false)) {
        await toggleBtn.click();
      } else {
        await page.evaluate(() => {
          document.documentElement.setAttribute('data-theme', 'dark');
          localStorage.setItem('theme', 'dark');
        });
      }
      await page.waitForTimeout(350);

      const urlsCard = page.locator('[data-testid="stat-card-urls"]');
      await expect(urlsCard).toBeVisible();

      // Check card background brightness (should be dark)
      const cardBg = await urlsCard.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });
      const cardBrightness = getBrightness(cardBg);
      expect(cardBrightness).not.toBeNull();
      expect(cardBrightness).toBeLessThan(100);

      // Check stat value text brightness (should be light for readability)
      const statValue = urlsCard.locator('[data-testid="stat-value-urls"]');
      const textColor = await statValue.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.color;
      });
      const textBrightness = getBrightness(textColor);
      expect(textBrightness).not.toBeNull();
      expect(textBrightness).toBeGreaterThan(80);

      // Check label text brightness (should be lighter in dark mode)
      const statLabel = urlsCard.locator('[data-testid="stat-label-urls"]');
      const labelColor = await statLabel.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.color;
      });
      const labelBrightness = getBrightness(labelColor);
      expect(labelBrightness).not.toBeNull();
      expect(labelBrightness).toBeGreaterThan(80);
    });

    test('should remain visible after theme toggle', async ({ page }) => {
      const analyticsSection = page.locator('[data-testid="analytics-section"]');
      await expect(analyticsSection).toBeVisible();

      // Toggle to dark mode
      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible').first();
      if (await toggleBtn.isVisible().catch(() => false)) {
        await toggleBtn.click();
      } else {
        await page.evaluate(() => {
          const current = document.documentElement.getAttribute('data-theme') || 'light';
          const next = current === 'dark' ? 'light' : 'dark';
          document.documentElement.setAttribute('data-theme', next);
        });
      }
      await page.waitForTimeout(350);

      // Section should still be visible
      await expect(analyticsSection).toBeVisible();

      // All stat cards should be visible
      await expect(page.locator('[data-testid="stat-card-urls"]')).toBeVisible();
      await expect(page.locator('[data-testid="stat-card-users"]')).toBeVisible();
      await expect(page.locator('[data-testid="stat-card-clicks"]')).toBeVisible();
    });
  });

  // ============================================================
  // Responsive Layout Tests
  // ============================================================
  test.describe('Counter Responsive Layout', () => {
    test('should stack stat cards vertically on mobile (375px)', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.waitForTimeout(300);

      const grid = page.locator('[data-testid="analytics-grid"]');
      await expect(grid).toBeVisible();

      // Cards should be in a single column (stacked)
      const cards = page.locator('[data-testid^="stat-card-"]');
      const count = await cards.count();
      expect(count).toBe(3);

      const firstCard = await cards.nth(0).boundingBox();
      const secondCard = await cards.nth(1).boundingBox();
      const thirdCard = await cards.nth(2).boundingBox();

      // All cards should be stacked vertically (different y positions)
      expect(secondCard.y).toBeGreaterThan(firstCard.y);
      expect(thirdCard.y).toBeGreaterThan(secondCard.y);

      // No horizontal overflow
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);
    });

    test('should show stat cards side by side on desktop (1280px)', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.waitForTimeout(300);

      const cards = page.locator('[data-testid^="stat-card-"]');
      const count = await cards.count();
      expect(count).toBe(3);

      const firstCard = await cards.nth(0).boundingBox();
      const secondCard = await cards.nth(1).boundingBox();
      const thirdCard = await cards.nth(2).boundingBox();

      // On desktop, cards should be in the same row (similar y positions)
      expect(Math.abs(secondCard.y - firstCard.y)).toBeLessThanOrEqual(20);
      expect(Math.abs(thirdCard.y - firstCard.y)).toBeLessThanOrEqual(20);

      // Cards should be side by side (different x positions)
      expect(secondCard.x).not.toBe(firstCard.x);
      expect(thirdCard.x).not.toBe(secondCard.x);
    });

    test('should have readable stat values at all breakpoints', async ({ page }) => {
      const breakpoints = [
        { name: 'mobile', width: 375, height: 812 },
        { name: 'tablet', width: 768, height: 1024 },
        { name: 'desktop', width: 1280, height: 800 },
      ];

      for (const bp of breakpoints) {
        await page.setViewportSize({ width: bp.width, height: bp.height });
        await page.waitForTimeout(200);

        // Stat values should be readable
        const statValues = page.locator('[data-testid^="stat-value-"]');
        const firstValue = statValues.first();
        await expect(firstValue).toBeVisible();

        const fontSize = await firstValue.evaluate(el => {
          const style = window.getComputedStyle(el);
          return parseFloat(style.fontSize);
        });
        expect(fontSize, `Font too small at ${bp.name}`).toBeGreaterThanOrEqual(16);

        // No horizontal overflow
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        const viewportWidth = await page.evaluate(() => window.innerWidth);
        expect(bodyWidth, `Horizontal overflow at ${bp.name}`).toBeLessThanOrEqual(viewportWidth + 1);
      }
    });
  });
});

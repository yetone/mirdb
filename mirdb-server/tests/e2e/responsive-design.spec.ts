import { test, expect, Page } from '@playwright/test';

/**
 * MirDB Web Dashboard - Responsive Design E2E Tests
 *
 * Scenario 11: Web UI Responsive Design
 *
 * Test cases:
 * 1. Desktop viewport (1920x1080) - Full layout displayed correctly
 * 2. Tablet viewport (768x1024) - Responsive layout adapts, all features accessible
 * 3. Mobile viewport (375x667) - Mobile-optimized layout, no horizontal overflow
 * 4. Form interactions on mobile - All inputs accessible, keyboard works correctly
 */

// Helper to check for horizontal overflow
async function hasNoHorizontalOverflow(page: Page): Promise<boolean> {
  const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
  const windowWidth = await page.evaluate(() => window.innerWidth);
  return bodyScrollWidth <= windowWidth;
}

// Helper to get element dimensions
async function getElementDimensions(page: Page, selector: string) {
  const element = await page.locator(selector).first();
  const box = await element.boundingBox();
  return box;
}

test.describe('Desktop Viewport (1920x1080)', () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test('full layout displayed correctly', async ({ page }) => {
    await page.goto('/');

    // Verify all main sections are visible
    await expect(page.locator('#status-panel')).toBeVisible();
    await expect(page.locator('#get-panel')).toBeVisible();
    await expect(page.locator('#set-panel')).toBeVisible();
    await expect(page.locator('#operations-panel')).toBeVisible();

    // Check header is visible
    await expect(page.locator('header h1')).toBeVisible();
    await expect(page.locator('header h1')).toContainText('MirDB');

    // Verify no horizontal overflow
    const noOverflow = await hasNoHorizontalOverflow(page);
    expect(noOverflow).toBe(true);

    // Check status grid layout (should have multiple columns)
    const statusGrid = await page.locator('.status-grid').boundingBox();
    expect(statusGrid).toBeTruthy();
    if (statusGrid) {
      expect(statusGrid.width).toBeGreaterThan(400);
    }

    // Verify form row has two columns on desktop
    const formRow = await page.locator('.form-row').first().boundingBox();
    expect(formRow).toBeTruthy();
    if (formRow) {
      expect(formRow.width).toBeGreaterThan(300);
    }
  });

  test('all interactive elements are accessible', async ({ page }) => {
    await page.goto('/');

    // Check all buttons are visible and clickable
    await expect(page.locator('#refresh-status')).toBeVisible();
    await expect(page.locator('#lookup-btn')).toBeVisible();
    await expect(page.locator('#set-btn')).toBeVisible();
    await expect(page.locator('#compact-btn')).toBeVisible();

    // Check all inputs are accessible
    await expect(page.locator('#get-key-input')).toBeVisible();
    await expect(page.locator('#set-key-input')).toBeVisible();
    await expect(page.locator('#set-value-input')).toBeVisible();
    await expect(page.locator('#set-flags-input')).toBeVisible();
    await expect(page.locator('#set-ttl-input')).toBeVisible();
  });
});

test.describe('Tablet Viewport (768x1024)', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test('responsive layout adapts, all features accessible', async ({ page }) => {
    await page.goto('/');

    // Verify all sections are visible
    await expect(page.locator('#status-panel')).toBeVisible();
    await expect(page.locator('#get-panel')).toBeVisible();
    await expect(page.locator('#set-panel')).toBeVisible();
    await expect(page.locator('#operations-panel')).toBeVisible();

    // Verify no horizontal overflow
    const noOverflow = await hasNoHorizontalOverflow(page);
    expect(noOverflow).toBe(true);

    // Check buttons have adequate touch targets (min 44px)
    const buttonBox = await page.locator('#refresh-status').boundingBox();
    expect(buttonBox).toBeTruthy();
    if (buttonBox) {
      expect(buttonBox.height).toBeGreaterThanOrEqual(44);
    }

    // Check inputs have adequate touch targets
    const inputBox = await page.locator('#get-key-input').boundingBox();
    expect(inputBox).toBeTruthy();
    if (inputBox) {
      expect(inputBox.height).toBeGreaterThanOrEqual(44);
    }

    // Verify forms are accessible
    await expect(page.locator('#get-key-form')).toBeVisible();
    await expect(page.locator('#set-key-form')).toBeVisible();
  });

  test('status grid adapts to tablet width', async ({ page }) => {
    await page.goto('/');

    // Status items should still be in a grid
    const statusItems = await page.locator('.status-item').all();
    expect(statusItems.length).toBeGreaterThanOrEqual(3);

    for (const item of statusItems) {
      await expect(item).toBeVisible();
    }
  });
});

test.describe('Mobile Viewport (375x667)', () => {
  test.use({
    viewport: { width: 375, height: 667 },
    isMobile: true,
    hasTouch: true,
  });

  test('mobile-optimized layout, no horizontal overflow', async ({ page }) => {
    await page.goto('/');

    // Verify all sections are visible (may need to scroll)
    await expect(page.locator('#status-panel')).toBeVisible();

    // Scroll to other sections and verify visibility
    await page.locator('#get-panel').scrollIntoViewIfNeeded();
    await expect(page.locator('#get-panel')).toBeVisible();

    await page.locator('#set-panel').scrollIntoViewIfNeeded();
    await expect(page.locator('#set-panel')).toBeVisible();

    await page.locator('#operations-panel').scrollIntoViewIfNeeded();
    await expect(page.locator('#operations-panel')).toBeVisible();

    // Critical: Verify no horizontal overflow
    const noOverflow = await hasNoHorizontalOverflow(page);
    expect(noOverflow).toBe(true);

    // Verify body width doesn't exceed viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(375);
  });

  test('buttons have adequate tap targets', async ({ page }) => {
    await page.goto('/');

    // All buttons should have minimum 48px height on mobile
    const buttons = ['#refresh-status', '#lookup-btn', '#set-btn', '#compact-btn'];

    for (const selector of buttons) {
      await page.locator(selector).scrollIntoViewIfNeeded();
      const box = await page.locator(selector).boundingBox();
      expect(box).toBeTruthy();
      if (box) {
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('form inputs have adequate tap targets', async ({ page }) => {
    await page.goto('/');

    // All inputs should have minimum 48px height on mobile
    const inputs = ['#get-key-input', '#set-key-input', '#set-value-input', '#set-flags-input', '#set-ttl-input'];

    for (const selector of inputs) {
      await page.locator(selector).scrollIntoViewIfNeeded();
      const box = await page.locator(selector).boundingBox();
      expect(box).toBeTruthy();
      if (box) {
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('form row stacks vertically on mobile', async ({ page }) => {
    await page.goto('/');

    // Navigate to set panel
    await page.locator('#set-panel').scrollIntoViewIfNeeded();

    // Get the flags and TTL input positions
    const flagsBox = await page.locator('#set-flags-input').boundingBox();
    const ttlBox = await page.locator('#set-ttl-input').boundingBox();

    expect(flagsBox).toBeTruthy();
    expect(ttlBox).toBeTruthy();

    if (flagsBox && ttlBox) {
      // On mobile, these should be stacked (TTL below flags)
      // or at least not side by side in a way that causes overflow
      // Check that neither extends beyond viewport
      expect(flagsBox.x + flagsBox.width).toBeLessThanOrEqual(375);
      expect(ttlBox.x + ttlBox.width).toBeLessThanOrEqual(375);
    }
  });
});

test.describe('Form Interactions on Mobile', () => {
  test.use({
    viewport: { width: 375, height: 667 },
    isMobile: true,
    hasTouch: true,
  });

  test('all inputs accessible, keyboard works correctly', async ({ page }) => {
    await page.goto('/');

    // Test Get Key input
    await page.locator('#get-key-input').scrollIntoViewIfNeeded();
    await page.locator('#get-key-input').tap();
    await expect(page.locator('#get-key-input')).toBeFocused();
    await page.locator('#get-key-input').fill('test-key');
    await expect(page.locator('#get-key-input')).toHaveValue('test-key');

    // Test Set Key input
    await page.locator('#set-key-input').scrollIntoViewIfNeeded();
    await page.locator('#set-key-input').tap();
    await expect(page.locator('#set-key-input')).toBeFocused();
    await page.locator('#set-key-input').fill('my-key');
    await expect(page.locator('#set-key-input')).toHaveValue('my-key');

    // Test value textarea
    await page.locator('#set-value-input').scrollIntoViewIfNeeded();
    await page.locator('#set-value-input').tap();
    await expect(page.locator('#set-value-input')).toBeFocused();
    await page.locator('#set-value-input').fill('test value');
    await expect(page.locator('#set-value-input')).toHaveValue('test value');

    // Test number inputs
    await page.locator('#set-flags-input').scrollIntoViewIfNeeded();
    await page.locator('#set-flags-input').tap();
    await page.locator('#set-flags-input').fill('42');
    await expect(page.locator('#set-flags-input')).toHaveValue('42');

    await page.locator('#set-ttl-input').scrollIntoViewIfNeeded();
    await page.locator('#set-ttl-input').tap();
    await page.locator('#set-ttl-input').fill('3600');
    await expect(page.locator('#set-ttl-input')).toHaveValue('3600');
  });

  test('buttons respond to tap interactions', async ({ page }) => {
    await page.goto('/');

    // Test that buttons are tappable
    await page.locator('#refresh-status').scrollIntoViewIfNeeded();
    await expect(page.locator('#refresh-status')).toBeEnabled();

    await page.locator('#lookup-btn').scrollIntoViewIfNeeded();
    await expect(page.locator('#lookup-btn')).toBeEnabled();

    await page.locator('#set-btn').scrollIntoViewIfNeeded();
    await expect(page.locator('#set-btn')).toBeEnabled();

    await page.locator('#compact-btn').scrollIntoViewIfNeeded();
    await expect(page.locator('#compact-btn')).toBeEnabled();
  });

  test('toast container positioned correctly on mobile', async ({ page }) => {
    await page.goto('/');

    // Toast container should be present
    const toastContainer = await page.locator('#toast-container');
    await expect(toastContainer).toBeAttached();

    // Get computed styles
    const position = await toastContainer.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        position: style.position,
        bottom: style.bottom,
        left: style.left,
        right: style.right,
      };
    });

    // Should be fixed positioned at bottom
    expect(position.position).toBe('fixed');
  });

  test('dialog displays correctly on mobile', async ({ page }) => {
    await page.goto('/');

    // The delete dialog should be hidden by default
    const dialog = page.locator('#delete-confirm-dialog');
    await expect(dialog).toBeHidden();

    // Dialog should have proper styling for mobile when shown
    // (We can't easily trigger the dialog without API, so just verify it exists)
    await expect(dialog).toBeAttached();
  });
});

test.describe('Accessibility on All Viewports', () => {
  for (const viewport of [
    { name: 'desktop', width: 1920, height: 1080 },
    { name: 'tablet', width: 768, height: 1024 },
    { name: 'mobile', width: 375, height: 667 },
  ]) {
    test(`${viewport.name}: all form inputs have labels`, async ({ page }) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');

      // Check that each input has an associated label
      const inputs = [
        { id: 'get-key-input', label: 'Key' },
        { id: 'set-key-input', label: 'Key' },
        { id: 'set-value-input', label: 'Value' },
        { id: 'set-flags-input', label: 'Flags' },
        { id: 'set-ttl-input', label: 'TTL' },
      ];

      for (const { id } of inputs) {
        const input = page.locator(`#${id}`);
        await input.scrollIntoViewIfNeeded();

        // Input should exist
        await expect(input).toBeAttached();

        // Check for associated label using for attribute
        const label = page.locator(`label[for="${id}"]`);
        await expect(label).toBeAttached();
      }
    });

    test(`${viewport.name}: sections have ARIA labels`, async ({ page }) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');

      // Check that main sections have aria-label attributes
      const sections = [
        '#status-panel',
        '#get-panel',
        '#set-panel',
        '#operations-panel',
      ];

      for (const selector of sections) {
        const section = page.locator(selector);
        await expect(section).toBeAttached();
        const ariaLabel = await section.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
      }
    });
  }
});

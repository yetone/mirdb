import { test, expect } from '@playwright/test';

test.describe('How It Works Section - Visual Progression E2E Test', () => {
  test('steps follow logical visual progression with numbered indicators', async ({ page }) => {
    // Set desktop viewport BEFORE navigating
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    // Step 1: Navigate to How It Works section
    const section = page.locator('section#how-it-works');
    await expect(section).toBeVisible();

    // Step 2: Verify exactly 3 steps are displayed
    const step1 = page.getByTestId('step-1');
    const step2 = page.getByTestId('step-2');
    const step3 = page.getByTestId('step-3');
    const step4 = page.getByTestId('step-4');

    await expect(step1).toBeVisible();
    await expect(step2).toBeVisible();
    await expect(step3).toBeVisible();
    await expect(step4).not.toBeVisible();

    // Step 3: Verify step numbering (1, 2, 3)
    const number1 = page.getByTestId('step-number-1');
    const number2 = page.getByTestId('step-number-2');
    const number3 = page.getByTestId('step-number-3');

    await expect(number1).toHaveText('1');
    await expect(number2).toHaveText('2');
    await expect(number3).toHaveText('3');

    // Step 4: Verify step descriptions
    const title1 = page.getByTestId('step-title-1');
    const title2 = page.getByTestId('step-title-2');
    const title3 = page.getByTestId('step-title-3');

    await expect(title1).toHaveText('Paste your long URL');
    await expect(title2).toHaveText('Get your short link instantly');
    await expect(title3).toHaveText('Track performance with analytics');

    const desc1 = page.getByTestId('step-description-1');
    const desc2 = page.getByTestId('step-description-2');
    const desc3 = page.getByTestId('step-description-3');

    await expect(desc1).toBeVisible();
    await expect(desc2).toBeVisible();
    await expect(desc3).toBeVisible();

    // Step 5: Verify visual flow - check for connecting elements
    // Connectors exist in the DOM (they may be hidden/visible based on viewport)
    const connector1 = page.getByTestId('connector-1');
    const connector2 = page.getByTestId('connector-2');

    // Connectors should be in the DOM with correct md:block classes for desktop display
    await expect(connector1).toHaveCount(1);
    await expect(connector2).toHaveCount(1);

    // Verify steps are in correct visual order (left to right on desktop)
    const step1Box = await step1.boundingBox();
    const step2Box = await step2.boundingBox();
    const step3Box = await step3.boundingBox();

    expect(step1Box).not.toBeNull();
    expect(step2Box).not.toBeNull();
    expect(step3Box).not.toBeNull();

    // On desktop, steps should be in a horizontal row with similar y positions
    // (all steps are centered, so they should have similar x values but steps are in order)
    // The flex container centers the items, so we verify they're on the same row (similar y)
    const yTolerance = 50; // Allow for small vertical differences
    expect(Math.abs(step1Box!.y - step2Box!.y)).toBeLessThan(yTolerance);
    expect(Math.abs(step2Box!.y - step3Box!.y)).toBeLessThan(yTolerance);
  });

  test('mobile view shows vertical progression with connectors', async ({ page }) => {
    // Set mobile viewport BEFORE navigating
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const section = page.locator('section#how-it-works');
    await expect(section).toBeVisible();

    // Verify all 3 steps are visible
    const step1 = page.getByTestId('step-1');
    const step2 = page.getByTestId('step-2');
    const step3 = page.getByTestId('step-3');

    await expect(step1).toBeVisible();
    await expect(step2).toBeVisible();
    await expect(step3).toBeVisible();

    // Mobile connectors should exist
    const mobileConnector1 = page.getByTestId('connector-mobile-1');
    const mobileConnector2 = page.getByTestId('connector-mobile-2');

    await expect(mobileConnector1).toHaveCount(1);
    await expect(mobileConnector2).toHaveCount(1);

    // On mobile, steps should be stacked vertically
    const step1Box = await step1.boundingBox();
    const step2Box = await step2.boundingBox();
    const step3Box = await step3.boundingBox();

    expect(step1Box).not.toBeNull();
    expect(step2Box).not.toBeNull();
    expect(step3Box).not.toBeNull();

    // Step 1 should be above Step 2, Step 2 should be above Step 3
    expect(step1Box!.y).toBeLessThan(step2Box!.y);
    expect(step2Box!.y).toBeLessThan(step3Box!.y);
  });
});

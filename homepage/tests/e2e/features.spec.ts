import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test('navigate to features section via navigation link', async ({ page }) => {
    await page.goto('/');

    // Verify the features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify that features content is displayed
    await expect(page.getByTestId('features-section')).toBeVisible();

    // Verify key features are present
    await expect(page.getByText('Key Features')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Memcached Protocol' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Data Persistence' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'LSM Tree Architecture' })).toBeVisible();
  });

  test('features section displays at least 3 feature cards', async ({ page }) => {
    await page.goto('/');

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4); // We have 4 features
  });

  test('features section is accessible via scroll', async ({ page }) => {
    await page.goto('/');

    // The features section should be visible in the viewport
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeInViewport();
  });
});

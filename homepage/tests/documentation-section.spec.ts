import { test, expect } from '@playwright/test';

test.describe('Documentation Section Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Documentation section is visible with multiple resource links', async ({ page }) => {
    // Navigate to documentation section
    const documentationSection = page.locator('[data-testid="documentation-section"]');
    await expect(documentationSection).toBeVisible();

    // Verify the section has a heading
    const heading = documentationSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Documentation');

    // Verify the grid layout exists
    const documentationGrid = page.locator('[data-testid="documentation-grid"]');
    await expect(documentationGrid).toBeVisible();
    await expect(documentationGrid).toHaveClass(/grid/);

    // Verify there are multiple documentation cards (at least 3)
    const docCards = documentationGrid.locator('a');
    const cardCount = await docCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify each card has an icon, title, and description
    for (const docId of ['configuration', 'protocol', 'deployment']) {
      const card = page.locator(`[data-testid="doc-card-${docId}"]`);
      await expect(card).toBeVisible();

      const icon = page.locator(`[data-testid="doc-icon-${docId}"]`);
      await expect(icon).toBeVisible();

      const title = page.locator(`[data-testid="doc-title-${docId}"]`);
      await expect(title).toBeVisible();
      await expect(title).not.toBeEmpty();

      const description = page.locator(`[data-testid="doc-description-${docId}"]`);
      await expect(description).toBeVisible();
      await expect(description).not.toBeEmpty();
    }
  });

  test('TC2: Configuration reference link navigates to configuration documentation', async ({ page }) => {
    // Find and verify the configuration reference card
    const configCard = page.locator('[data-testid="doc-card-configuration"]');
    await expect(configCard).toBeVisible();

    // Verify the title
    const configTitle = page.locator('[data-testid="doc-title-configuration"]');
    await expect(configTitle).toContainText('Configuration Reference');

    // Verify the link has href attribute pointing to quick-start section
    await expect(configCard).toHaveAttribute('href', '#quick-start');

    // Click the link and verify navigation
    await configCard.click();

    // Verify the URL hash changed to quick-start
    await expect(page).toHaveURL(/#quick-start$/);

    // Verify the quick-start section is now in view
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeVisible();
  });

  test('TC3: API/protocol documentation link navigates to API documentation', async ({ page }) => {
    // Find and verify the protocol documentation card
    const protocolCard = page.locator('[data-testid="doc-card-protocol"]');
    await expect(protocolCard).toBeVisible();

    // Verify the title
    const protocolTitle = page.locator('[data-testid="doc-title-protocol"]');
    await expect(protocolTitle).toContainText('Memcached Protocol');

    // Verify the link has href attribute pointing to external GitHub documentation
    await expect(protocolCard).toHaveAttribute('href', /github\.com.*mirdb/);

    // Verify the link opens in a new tab (external link)
    await expect(protocolCard).toHaveAttribute('target', '_blank');
    await expect(protocolCard).toHaveAttribute('rel', /noopener/);

    // Verify the description mentions protocol or commands
    const protocolDescription = page.locator('[data-testid="doc-description-protocol"]');
    await expect(protocolDescription).toContainText(/protocol|commands|clients/i);
  });

  test('TC4: Deployment guide link navigates to deployment documentation', async ({ page }) => {
    // Find and verify the deployment guide card
    const deploymentCard = page.locator('[data-testid="doc-card-deployment"]');
    await expect(deploymentCard).toBeVisible();

    // Verify the title
    const deploymentTitle = page.locator('[data-testid="doc-title-deployment"]');
    await expect(deploymentTitle).toContainText('Deployment Guide');

    // Verify the link has href attribute pointing to external GitHub documentation
    await expect(deploymentCard).toHaveAttribute('href', /github\.com.*mirdb/);

    // Verify the link opens in a new tab (external link)
    await expect(deploymentCard).toHaveAttribute('target', '_blank');
    await expect(deploymentCard).toHaveAttribute('rel', /noopener/);

    // Verify the description mentions deployment or production
    const deploymentDescription = page.locator('[data-testid="doc-description-deployment"]');
    await expect(deploymentDescription).toContainText(/deploy|production|performance/i);
  });
});

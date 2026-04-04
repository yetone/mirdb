/**
 * Features Section Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - 4 feature cards present
 * - Memcached Protocol card
 * - Persistence/SSTables card
 * - LSM Tree Architecture card
 * - Async/Tokio Networking card
 * - Icons on all cards
 */

const { test, expect } = require('@playwright/test');
const { SELECTORS, FEATURE_TITLES, navigateToSection } = require('./test-utils');

test.describe('Features Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('Test Case 1: Exactly 4 feature cards are displayed', async ({ page }) => {
        // Navigate to features section
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeVisible();

        // Query for all feature cards
        const featureCards = page.locator(SELECTORS.featureCard);

        // Verify exactly 4 cards are present
        await expect(featureCards).toHaveCount(4);
    });

    test('Test Case 2: Memcached Protocol Compatible feature card exists', async ({ page }) => {
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeVisible();

        // Find the Memcached Protocol card
        const memcachedCard = page.locator(SELECTORS.featureCard).filter({
            has: page.locator(SELECTORS.featureTitle, { hasText: 'Memcached Protocol Compatible' })
        });

        await expect(memcachedCard).toBeVisible();

        // Check title is exactly as expected
        const title = memcachedCard.locator(SELECTORS.featureTitle);
        await expect(title).toHaveText('Memcached Protocol Compatible');

        // Check description exists and is not empty
        const description = memcachedCard.locator(SELECTORS.featureDescription);
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText.length).toBeGreaterThan(0);
    });

    test('Test Case 3: Persistent Storage feature card exists', async ({ page }) => {
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeVisible();

        // Find the Persistent Storage card (might be titled "Persistent Storage" or mention "SSTables")
        const persistenceCard = page.locator(SELECTORS.featureCard).filter({
            has: page.locator(SELECTORS.featureTitle, { hasText: /Persistent Storage|SSTables/i })
        });

        await expect(persistenceCard).toBeVisible();

        // Check title contains expected text
        const title = persistenceCard.locator(SELECTORS.featureTitle);
        await expect(title).toContainText(/Persistent Storage|SSTables/);

        // Check description exists and is not empty
        const description = persistenceCard.locator(SELECTORS.featureDescription);
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText.length).toBeGreaterThan(0);
    });

    test('Test Case 4: LSM Tree Architecture feature card exists', async ({ page }) => {
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeVisible();

        // Find the LSM Tree card
        const lsmTreeCard = page.locator(SELECTORS.featureCard).filter({
            has: page.locator(SELECTORS.featureTitle, { hasText: 'LSM Tree Architecture' })
        });

        await expect(lsmTreeCard).toBeVisible();

        // Check title is exactly as expected
        const title = lsmTreeCard.locator(SELECTORS.featureTitle);
        await expect(title).toHaveText('LSM Tree Architecture');

        // Check description exists and is not empty
        const description = lsmTreeCard.locator(SELECTORS.featureDescription);
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText.length).toBeGreaterThan(0);
    });

    test('Test Case 5: Async/Tokio Networking feature card exists', async ({ page }) => {
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeVisible();

        // Find the Async/Tokio card
        const asyncCard = page.locator(SELECTORS.featureCard).filter({
            has: page.locator(SELECTORS.featureTitle, { hasText: 'Async/Tokio Networking' })
        });

        await expect(asyncCard).toBeVisible();

        // Check title is exactly as expected
        const title = asyncCard.locator(SELECTORS.featureTitle);
        await expect(title).toHaveText('Async/Tokio Networking');

        // Check description exists and is not empty
        const description = asyncCard.locator(SELECTORS.featureDescription);
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText.length).toBeGreaterThan(0);
    });

    test('Test Case 6: All 4 feature cards contain an icon element (img or svg)', async ({ page }) => {
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeVisible();

        // Get all feature cards
        const featureCards = page.locator(SELECTORS.featureCard);
        const count = await featureCards.count();

        // Verify we have exactly 4 cards
        expect(count).toBe(4);

        // Check each card has an icon (either img or svg)
        for (let i = 0; i < count; i++) {
            const card = featureCards.nth(i);
            const iconContainer = card.locator(SELECTORS.featureIcon);

            // Check icon container exists
            await expect(iconContainer).toBeVisible();

            // Check for img or svg inside the icon container
            const hasImg = await iconContainer.locator('img').count() > 0;
            const hasSvg = await iconContainer.locator('svg').count() > 0;

            // At least one of img or svg should be present
            expect(hasImg || hasSvg, `Feature card ${i + 1} should have an img or svg icon`).toBeTruthy();
        }
    });

    test('Features section is accessible via navigation', async ({ page }) => {
        // Click on Features navigation link
        await page.click('a[href="#features"]');

        // Wait for section to be visible
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeVisible();

        // Verify the section heading is visible
        const heading = page.locator(SELECTORS.featuresHeading);
        await expect(heading).toBeVisible();
        await expect(heading).toHaveText('Features');
    });

    test('Feature cards have consistent styling and layout', async ({ page }) => {
        const featureCards = page.locator(SELECTORS.featureCard);
        const count = await featureCards.count();

        expect(count).toBe(4);

        // Verify all cards are visible and have expected structure
        for (let i = 0; i < count; i++) {
            const card = featureCards.nth(i);
            await expect(card).toBeVisible();

            // Each card should have icon, title, and description
            await expect(card.locator(SELECTORS.featureIcon)).toBeVisible();
            await expect(card.locator(SELECTORS.featureTitle)).toBeVisible();
            await expect(card.locator(SELECTORS.featureDescription)).toBeVisible();
        }
    });
});

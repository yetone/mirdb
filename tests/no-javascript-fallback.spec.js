// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('No JavaScript Fallback', () => {
    test.use({ javaScriptEnabled: false });

    test('page renders with basic content visible when JavaScript is disabled', async ({ page }) => {
        await page.goto('/');

        // Verify the page loaded successfully
        await expect(page).toHaveTitle(/MirDB/);

        // Check that the main content sections exist and are visible
        const body = page.locator('body');
        await expect(body).toBeVisible();

        // Verify hero section is visible
        const hero = page.locator('.hero, #hero, header');
        await expect(hero.first()).toBeVisible();

        // Verify features section is visible
        const features = page.locator('.features, #features');
        await expect(features.first()).toBeVisible();

        // Verify footer is visible
        const footer = page.locator('footer, .footer');
        await expect(footer.first()).toBeVisible();
    });

    test('hero text and CTAs are visible without JavaScript', async ({ page }) => {
        await page.goto('/');

        // Check hero section elements
        const heroSection = page.locator('.hero, #hero, header').first();
        await expect(heroSection).toBeVisible();

        // Verify main heading is visible
        const heading = page.locator('h1');
        await expect(heading).toBeVisible();
        await expect(heading).toContainText('MirDB');

        // Verify tagline is visible
        const tagline = page.locator('.tagline');
        await expect(tagline).toBeVisible();
        await expect(tagline).toContainText('Persistent Key-Value Store');

        // Verify description is visible
        const description = page.locator('.hero .description, .hero p').first();
        await expect(description).toBeVisible();

        // Verify CTA buttons are visible
        const ctaButtons = page.locator('.cta-buttons');
        await expect(ctaButtons).toBeVisible();

        // Check "Get Started" button
        const getStartedBtn = page.locator('.btn-primary, a:has-text("Get Started")').first();
        await expect(getStartedBtn).toBeVisible();

        // Check "View Documentation" button
        const docsBtn = page.locator('.btn-secondary, a:has-text("Documentation")').first();
        await expect(docsBtn).toBeVisible();
    });

    test('feature cards content is readable without JavaScript', async ({ page }) => {
        await page.goto('/');

        // Find features section
        const featuresSection = page.locator('.features, #features').first();
        await expect(featuresSection).toBeVisible();

        // Verify features section title
        const featuresTitle = featuresSection.locator('.section-title, h2').first();
        await expect(featuresTitle).toBeVisible();
        await expect(featuresTitle).toContainText('Features');

        // Verify feature cards are visible
        const featureCards = page.locator('.feature-card');
        const cardCount = await featureCards.count();
        expect(cardCount).toBeGreaterThanOrEqual(3);

        // Check each feature card has a title and description
        for (let i = 0; i < Math.min(cardCount, 3); i++) {
            const card = featureCards.nth(i);
            await expect(card).toBeVisible();

            // Check feature title
            const cardTitle = card.locator('.feature-title, h3').first();
            await expect(cardTitle).toBeVisible();

            // Check feature description
            const cardDesc = card.locator('.feature-description, p').first();
            await expect(cardDesc).toBeVisible();
        }

        // Verify specific feature content is present
        const memcachedFeature = page.locator('[data-feature="memcached-compatible"], .feature-card:has-text("Memcached")').first();
        await expect(memcachedFeature).toBeVisible();

        const persistentFeature = page.locator('[data-feature="persistent-storage"], .feature-card:has-text("Persistent")').first();
        await expect(persistentFeature).toBeVisible();

        const performanceFeature = page.locator('[data-feature="high-performance"], .feature-card:has-text("Performance")').first();
        await expect(performanceFeature).toBeVisible();
    });

    test('code examples are visible without JavaScript (syntax highlighting may be disabled)', async ({ page }) => {
        await page.goto('/');

        // Navigate to quick start / getting started section
        const quickStartSection = page.locator('.getting-started, #getting-started, [data-testid="quick-start-section"]').first();
        await expect(quickStartSection).toBeVisible();

        // Verify code blocks are visible
        const codeBlocks = page.locator('.code-block, pre, code');
        const codeBlockCount = await codeBlocks.count();
        expect(codeBlockCount).toBeGreaterThanOrEqual(1);

        // Check installation code block
        const installationCode = page.locator('[data-testid="installation-code-block"], .code-block:has-text("cargo install")').first();
        await expect(installationCode).toBeVisible();
        await expect(installationCode).toContainText('cargo');

        // Check configuration code block
        const configCode = page.locator('[data-testid="configuration-code-block"], .code-block:has-text("addr")').first();
        await expect(configCode).toBeVisible();

        // Check that pre/code elements are visible and contain text
        const preElements = page.locator('pre');
        const preCount = await preElements.count();
        expect(preCount).toBeGreaterThanOrEqual(1);

        for (let i = 0; i < Math.min(preCount, 4); i++) {
            const pre = preElements.nth(i);
            await expect(pre).toBeVisible();

            // Verify the code block has content
            const textContent = await pre.textContent();
            expect(textContent.trim().length).toBeGreaterThan(0);
        }

        // Verify usage example code block is visible
        const usageCode = page.locator('[data-testid="usage-code-block"], .code-block:has-text("telnet")').first();
        await expect(usageCode).toBeVisible();
        await expect(usageCode).toContainText('telnet');
    });
});

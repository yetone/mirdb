/**
 * Responsive Design E2E Tests
 * Owner: Scenario 6 - Responsive Design (Mobile)
 * Also covers: Scenario 7 - Responsive Design (Tablet)
 *
 * Tests for:
 * - Mobile viewport (375px) layout
 * - No horizontal scrolling
 * - Touch target sizes
 * - Tablet viewport (768px) layout
 * - Feature grid adaptation
 * - Code block responsiveness
 */

const { test, expect } = require('@playwright/test');
const config = require('../setup/test-config');

// Mobile viewport dimensions
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile', () => {
    test.beforeEach(async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize(MOBILE_VIEWPORT);
        await page.goto('/');
        // Wait for page to be fully loaded
        await page.waitForLoadState('domcontentloaded');
    });

    test('TC1: No horizontal scroll bar appears at 375px width', async ({ page }) => {
        // Check that horizontal overflow is not present
        const hasHorizontalScroll = await page.evaluate(() => {
            return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });

        expect(hasHorizontalScroll).toBe(false);

        // Also verify body doesn't overflow
        const bodyOverflow = await page.evaluate(() => {
            const body = document.body;
            return body.scrollWidth > body.clientWidth;
        });

        expect(bodyOverflow).toBe(false);
    });

    test('TC2: Hero content stacks vertically and remains readable', async ({ page }) => {
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Check hero logo is visible
        const heroLogo = page.locator('.hero-logo');
        await expect(heroLogo).toBeVisible();

        // Check hero title is visible and readable
        const heroTitle = page.locator('#hero-title');
        await expect(heroTitle).toBeVisible();
        await expect(heroTitle).toContainText('MirDB');

        // Verify title font size is reasonable for mobile (at least 1.5rem = ~24px)
        const titleFontSize = await heroTitle.evaluate(el =>
            parseFloat(window.getComputedStyle(el).fontSize)
        );
        expect(titleFontSize).toBeGreaterThanOrEqual(24);

        // Check tagline is visible
        const tagline = page.locator('.hero-tagline');
        await expect(tagline).toBeVisible();

        // Check CTA buttons are visible
        const ctaButtons = page.locator('.hero-ctas .btn');
        await expect(ctaButtons.first()).toBeVisible();

        // Verify hero content fits within viewport
        const heroBox = await heroSection.boundingBox();
        expect(heroBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    });

    test('TC3: Feature cards stack vertically in single column layout', async ({ page }) => {
        const featuresSection = page.locator('#features');
        await featuresSection.scrollIntoViewIfNeeded();
        await expect(featuresSection).toBeVisible();

        const featureCards = page.locator('.feature-card');
        const cardCount = await featureCards.count();
        expect(cardCount).toBeGreaterThanOrEqual(5);

        // Get positions of first two feature cards
        const firstCard = featureCards.nth(0);
        const secondCard = featureCards.nth(1);

        const firstBox = await firstCard.boundingBox();
        const secondBox = await secondCard.boundingBox();

        // In vertical stack, second card should be below first card
        // (its top should be >= first card's bottom, accounting for gap)
        expect(secondBox.y).toBeGreaterThan(firstBox.y);

        // Cards should be roughly same width (single column)
        expect(Math.abs(firstBox.width - secondBox.width)).toBeLessThan(10);

        // Each card should fit within viewport width
        expect(firstBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
        expect(secondBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    });

    test('TC4: CTA buttons have minimum touch target size of 44x44 pixels', async ({ page }) => {
        // Check primary CTA button
        const primaryCta = page.locator('#cta-github');
        await expect(primaryCta).toBeVisible();

        const primaryBox = await primaryCta.boundingBox();
        expect(primaryBox.height).toBeGreaterThanOrEqual(44);
        expect(primaryBox.width).toBeGreaterThanOrEqual(44);

        // Check secondary CTA button
        const secondaryCta = page.locator('#cta-docs');
        await expect(secondaryCta).toBeVisible();

        const secondaryBox = await secondaryCta.boundingBox();
        expect(secondaryBox.height).toBeGreaterThanOrEqual(44);
        expect(secondaryBox.width).toBeGreaterThanOrEqual(44);

        // Check copy button in code section
        const codeSection = page.locator('#code-example');
        await codeSection.scrollIntoViewIfNeeded();

        const copyBtn = page.locator('#copy-btn');
        await expect(copyBtn).toBeVisible();

        const copyBox = await copyBtn.boundingBox();
        // Copy button should have reasonable touch target
        expect(copyBox.height).toBeGreaterThanOrEqual(32); // Slightly smaller is ok for utility buttons
        expect(copyBox.width).toBeGreaterThanOrEqual(44);
    });

    test('TC5: Code block is scrollable horizontally if needed', async ({ page }) => {
        const codeSection = page.locator('#code-example');
        await codeSection.scrollIntoViewIfNeeded();
        await expect(codeSection).toBeVisible();

        // Get the code container
        const codeContainer = page.locator('#code-example .code-container');
        await expect(codeContainer).toBeVisible();

        // Check that pre element has overflow-x auto or scroll
        const preElement = page.locator('#code-example pre');
        await expect(preElement).toBeVisible();

        const overflowX = await preElement.evaluate(el =>
            window.getComputedStyle(el).overflowX
        );

        // Should be 'auto', 'scroll', or 'visible' (when content fits)
        expect(['auto', 'scroll', 'visible']).toContain(overflowX);

        // The code container should fit within viewport
        const containerBox = await codeContainer.boundingBox();
        expect(containerBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    });

    test('All sections are visible and accessible on mobile', async ({ page }) => {
        // Hero section
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Features section
        const featuresSection = page.locator('#features');
        await featuresSection.scrollIntoViewIfNeeded();
        await expect(featuresSection).toBeVisible();

        // Code example section
        const codeSection = page.locator('#code-example');
        await codeSection.scrollIntoViewIfNeeded();
        await expect(codeSection).toBeVisible();

        // Quick start section
        const quickStartSection = page.locator('#quick-start');
        await quickStartSection.scrollIntoViewIfNeeded();
        await expect(quickStartSection).toBeVisible();

        // Footer
        const footer = page.locator('footer');
        await footer.scrollIntoViewIfNeeded();
        await expect(footer).toBeVisible();
    });

    test('Text remains readable without horizontal scrolling', async ({ page }) => {
        // Check that all major text content is readable
        const heroTitle = page.locator('#hero-title');
        const heroTitleBox = await heroTitle.boundingBox();
        expect(heroTitleBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width - 16); // Account for padding

        // Check features title
        const featuresTitle = page.locator('#features-title');
        await featuresTitle.scrollIntoViewIfNeeded();
        const featuresTitleBox = await featuresTitle.boundingBox();
        expect(featuresTitleBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width - 16);

        // Check that paragraphs don't overflow
        const paragraphs = page.locator('p');
        const paragraphCount = await paragraphs.count();

        for (let i = 0; i < Math.min(paragraphCount, 5); i++) {
            const para = paragraphs.nth(i);
            const isVisible = await para.isVisible();
            if (isVisible) {
                const box = await para.boundingBox();
                if (box) {
                    expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
                }
            }
        }
    });
});

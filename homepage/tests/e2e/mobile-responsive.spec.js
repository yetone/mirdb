// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Mobile Responsive Design Tests
 * Verifies REQ-8: Support responsive design for mobile and tablet viewing
 * Verifies US-5: View on Mobile Device
 *
 * Tests run at mobile viewport (375x667 - iPhone SE)
 */

test.describe('Mobile Responsive Design - 375px Viewport', () => {
    // Use mobile viewport for all tests in this describe block
    test.use({ viewport: { width: 375, height: 667 } });

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        // Wait for page to fully load
        await page.waitForLoadState('networkidle');
    });

    test('TC1: No horizontal scrolling required at 375px viewport', async ({ page }) => {
        // Get the page body dimensions
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        const viewportWidth = await page.evaluate(() => window.innerWidth);

        // The body should not be wider than the viewport (allowing small margin for scrollbar)
        expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 5);

        // Verify document doesn't have horizontal overflow
        const hasHorizontalScroll = await page.evaluate(() => {
            return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });
        expect(hasHorizontalScroll).toBe(false);

        // Verify no element causes horizontal overflow
        const overflowingElements = await page.evaluate(() => {
            const elements = document.querySelectorAll('*');
            const overflowing = [];
            const viewportWidth = window.innerWidth;

            elements.forEach(el => {
                const rect = el.getBoundingClientRect();
                if (rect.right > viewportWidth + 5) {
                    overflowing.push({
                        tag: el.tagName,
                        class: el.className,
                        right: rect.right,
                        viewportWidth: viewportWidth
                    });
                }
            });
            return overflowing;
        });

        // Should have no overflowing elements
        if (overflowingElements.length > 0) {
            console.log('Overflowing elements:', JSON.stringify(overflowingElements, null, 2));
        }
        expect(overflowingElements.length).toBe(0);
    });

    test('TC2: Mobile-friendly navigation is present', async ({ page }) => {
        // Check for mobile navigation element (hamburger menu or collapsible navigation)
        const mobileNav = await page.locator('[data-testid="mobile-nav"], [data-testid="hamburger-menu"], .mobile-nav, .hamburger-menu, nav.mobile').first();

        // At mobile viewport, either:
        // 1. A dedicated mobile navigation should be visible, OR
        // 2. The footer navigation should be accessible as a fallback

        // Check if mobile navigation exists
        const hasMobileNav = await mobileNav.count() > 0;

        if (hasMobileNav) {
            // If mobile nav exists, verify it's visible or can be triggered
            await expect(mobileNav).toBeAttached();
        } else {
            // Fallback: Check footer navigation is accessible
            const footerNav = page.locator('[data-testid="footer-nav"]');
            await expect(footerNav).toBeVisible();

            // Verify footer nav links are accessible
            const footerLinks = footerNav.locator('a');
            const linkCount = await footerLinks.count();
            expect(linkCount).toBeGreaterThan(0);
        }
    });

    test('TC4: Hero section is visible and properly sized for mobile', async ({ page }) => {
        const heroSection = page.locator('[data-testid="hero-section"]');
        await expect(heroSection).toBeVisible();

        // Verify hero title is visible
        const heroTitle = page.locator('[data-testid="hero-title"]');
        await expect(heroTitle).toBeVisible();

        // Verify hero tagline is visible
        const heroTagline = page.locator('[data-testid="hero-tagline"]');
        await expect(heroTagline).toBeVisible();

        // Verify hero description is visible
        const heroDescription = page.locator('[data-testid="hero-description"]');
        await expect(heroDescription).toBeVisible();

        // Verify CTA buttons are visible and accessible
        const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
        await expect(ctaButtons).toBeVisible();

        // Check hero section dimensions
        const heroBox = await heroSection.boundingBox();
        expect(heroBox).not.toBeNull();

        // Hero should be at least viewport width (no gap on sides)
        expect(heroBox.width).toBeGreaterThanOrEqual(370);

        // Hero content should be readable - check title font size is reasonable for mobile
        const titleFontSize = await heroTitle.evaluate(el => {
            return parseFloat(window.getComputedStyle(el).fontSize);
        });
        // Title should be at least 24px for readability on mobile (but not too large)
        expect(titleFontSize).toBeGreaterThanOrEqual(24);
        expect(titleFontSize).toBeLessThanOrEqual(64);
    });

    test('TC5: Feature cards stack vertically on mobile', async ({ page }) => {
        const featuresSection = page.locator('[data-testid="features-section"]');
        await featuresSection.scrollIntoViewIfNeeded();
        await expect(featuresSection).toBeVisible();

        const featureGrid = page.locator('[data-testid="feature-grid"]');
        await expect(featureGrid).toBeVisible();

        // Get all feature cards
        const featureCards = page.locator('.feature-card');
        const cardCount = await featureCards.count();
        expect(cardCount).toBeGreaterThan(0);

        // Get positions of first few cards to verify vertical stacking
        if (cardCount >= 2) {
            const firstCard = featureCards.nth(0);
            const secondCard = featureCards.nth(1);

            const firstBox = await firstCard.boundingBox();
            const secondBox = await secondCard.boundingBox();

            expect(firstBox).not.toBeNull();
            expect(secondBox).not.toBeNull();

            // Second card should be below first card (vertical stacking)
            // Its top should be greater than or equal to first card's bottom
            expect(secondBox.y).toBeGreaterThanOrEqual(firstBox.y + firstBox.height - 10);

            // Cards should be approximately full width (minus padding)
            expect(firstBox.width).toBeGreaterThanOrEqual(300);
            expect(secondBox.width).toBeGreaterThanOrEqual(300);
        }

        // Verify all cards are fully visible within viewport width
        for (let i = 0; i < cardCount; i++) {
            const card = featureCards.nth(i);
            const cardBox = await card.boundingBox();
            expect(cardBox).not.toBeNull();
            // Card should not extend beyond viewport
            expect(cardBox.x + cardBox.width).toBeLessThanOrEqual(380);
            expect(cardBox.x).toBeGreaterThanOrEqual(-5);
        }
    });

    test('TC6: Code blocks are scrollable horizontally within container', async ({ page }) => {
        const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
        await codeExamplesSection.scrollIntoViewIfNeeded();
        await expect(codeExamplesSection).toBeVisible();

        // Get all code blocks
        const codeBlocks = page.locator('.code-block');
        const codeBlockCount = await codeBlocks.count();
        expect(codeBlockCount).toBeGreaterThan(0);

        // Check each code block
        for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
            const codeBlock = codeBlocks.nth(i);
            const codeBlockBox = await codeBlock.boundingBox();

            expect(codeBlockBox).not.toBeNull();

            // Code block container should not exceed viewport width
            expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(380);
            expect(codeBlockBox.x).toBeGreaterThanOrEqual(-5);

            // Check if code block has overflow-x set to auto or scroll
            const overflowX = await codeBlock.evaluate(el => {
                return window.getComputedStyle(el).overflowX;
            });
            expect(['auto', 'scroll']).toContain(overflowX);
        }

        // Also check the larger code examples in Getting Started section
        const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
        await gettingStartedSection.scrollIntoViewIfNeeded();

        const codeExamples = page.locator('.code-example');
        const codeExampleCount = await codeExamples.count();

        for (let i = 0; i < codeExampleCount; i++) {
            const codeExample = codeExamples.nth(i);
            const codeExampleBox = await codeExample.boundingBox();

            if (codeExampleBox) {
                // Container should not exceed viewport
                expect(codeExampleBox.x + codeExampleBox.width).toBeLessThanOrEqual(380);

                // Check overflow property
                const overflowX = await codeExample.evaluate(el => {
                    return window.getComputedStyle(el).overflowX;
                });
                expect(['auto', 'scroll']).toContain(overflowX);
            }
        }
    });

    test('All content is readable without horizontal scrolling', async ({ page }) => {
        // Verify container widths don't exceed viewport
        const containers = page.locator('.container');
        const containerCount = await containers.count();

        for (let i = 0; i < containerCount; i++) {
            const container = containers.nth(i);
            const containerBox = await container.boundingBox();

            if (containerBox) {
                // Container should fit within viewport
                expect(containerBox.width).toBeLessThanOrEqual(380);
            }
        }

        // Verify sections are visible and within viewport
        const sections = ['hero-section', 'features-section', 'how-it-works-section',
                         'code-examples-section', 'getting-started-section', 'footer-section'];

        for (const sectionId of sections) {
            const section = page.locator(`[data-testid="${sectionId}"]`);
            if (await section.count() > 0) {
                await section.scrollIntoViewIfNeeded();
                const isVisible = await section.isVisible();
                expect(isVisible).toBe(true);
            }
        }
    });

    test('Data path cards stack vertically on mobile', async ({ page }) => {
        const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
        await howItWorksSection.scrollIntoViewIfNeeded();

        const dataPathsGrid = page.locator('[data-testid="data-paths-grid"]');
        if (await dataPathsGrid.count() > 0) {
            const writePathCard = page.locator('[data-testid="write-path-card"]');
            const readPathCard = page.locator('[data-testid="read-path-card"]');

            const writeBox = await writePathCard.boundingBox();
            const readBox = await readPathCard.boundingBox();

            if (writeBox && readBox) {
                // Cards should stack vertically (read path below write path)
                expect(readBox.y).toBeGreaterThanOrEqual(writeBox.y + writeBox.height - 20);

                // Cards should be nearly full width
                expect(writeBox.width).toBeGreaterThanOrEqual(300);
                expect(readBox.width).toBeGreaterThanOrEqual(300);
            }
        }
    });

    test('CTA buttons stack vertically on very small screens', async ({ page }) => {
        // Use smaller viewport for this test
        await page.setViewportSize({ width: 375, height: 667 });

        const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
        await expect(ctaButtons).toBeVisible();

        // Check flex direction
        const flexDirection = await ctaButtons.evaluate(el => {
            return window.getComputedStyle(el).flexDirection;
        });

        // At 375px (which is < 480px), buttons should stack vertically
        // Based on CSS: @media (max-width: 480px) { flex-direction: column; }
        expect(flexDirection).toBe('column');
    });
});

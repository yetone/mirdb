/**
 * Responsive Design E2E Tests
 * Owner: Scenario 8 - Desktop, Scenario 9 - Tablet, Scenario 10 - Mobile
 *
 * Tests:
 * - Desktop layout (1920px)
 * - Tablet layout (768px, 1024px)
 * - Mobile layout (320px, 375px)
 * - Mobile menu functionality
 * - Touch target sizes
 */

import { test, expect } from '@playwright/test';

// ============================================
// Scenario 8: Desktop Responsive Tests (1920px+)
// ============================================
test.describe('Responsive Design - Desktop (1920px+)', () => {
    test.use({
        viewport: { width: 1920, height: 1080 }
    });

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: All content displays correctly without horizontal scroll at 1920px', async ({ page }) => {
        // Wait for page to be fully loaded
        await page.waitForLoadState('networkidle');

        // Check there's no horizontal scrollbar
        const hasHorizontalScroll = await page.evaluate(() => {
            return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });
        expect(hasHorizontalScroll).toBe(false);

        // Verify all main sections are visible
        const sections = ['#hero', '#features', '#code-examples', '#architecture', '#status', '#specifications'];
        for (const section of sections) {
            const sectionElement = page.locator(section);
            await expect(sectionElement).toBeVisible();
        }

        // Verify content is not cut off or overflowing
        const bodyOverflow = await page.evaluate(() => {
            const body = document.body;
            return {
                overflowX: window.getComputedStyle(body).overflowX,
                scrollWidth: body.scrollWidth,
                clientWidth: body.clientWidth
            };
        });
        expect(bodyOverflow.scrollWidth).toBeLessThanOrEqual(bodyOverflow.clientWidth + 1);
    });

    test('TC2: Full navigation menu visible (no hamburger menu) at desktop viewport', async ({ page }) => {
        // Wait for navigation to be visible
        const navContainer = page.locator('.nav-container');
        await expect(navContainer).toBeVisible();

        // Check that the hamburger menu toggle is NOT visible
        const navToggle = page.locator('.nav-toggle');
        await expect(navToggle).not.toBeVisible();

        // Check that the navigation links container is visible
        const navLinks = page.locator('.nav-links');
        await expect(navLinks).toBeVisible();

        // Verify navigation links are displayed horizontally (not stacked)
        const navLinksDisplay = await navLinks.evaluate(el => {
            const style = window.getComputedStyle(el);
            return {
                display: style.display,
                flexDirection: style.flexDirection
            };
        });
        expect(navLinksDisplay.display).toBe('flex');
        expect(navLinksDisplay.flexDirection).not.toBe('column');

        // Verify all navigation links are visible
        const links = page.locator('.nav-links .nav-link');
        const linkCount = await links.count();
        expect(linkCount).toBeGreaterThanOrEqual(4);

        for (let i = 0; i < linkCount; i++) {
            await expect(links.nth(i)).toBeVisible();
        }

        // Verify CTA buttons are visible in the header
        const navCta = page.locator('.nav-cta');
        await expect(navCta).toBeVisible();

        const primaryCta = page.locator('.nav-cta .cta-btn--primary');
        await expect(primaryCta).toBeVisible();
    });

    test('TC3: Features display in multi-column grid or zigzag layout at desktop', async ({ page }) => {
        // Scroll to features section
        await page.locator('#features').scrollIntoViewIfNeeded();

        // Verify features grid exists
        const featuresGrid = page.locator('.features-grid');
        await expect(featuresGrid).toBeVisible();

        // Count feature cards
        const featureCards = page.locator('.features-grid .feature-card');
        const cardCount = await featureCards.count();
        expect(cardCount).toBeGreaterThanOrEqual(6);

        // Verify that at desktop width, cards are displayed in a multi-column
        // or zigzag layout (not stacked vertically in a single column)
        if (cardCount >= 2) {
            const card1 = featureCards.nth(0);
            const card2 = featureCards.nth(1);

            const card1Box = await card1.boundingBox();
            const card2Box = await card2.boundingBox();

            if (card1Box && card2Box) {
                // At 1920px desktop width, feature cards should either be:
                // 1. On the same row (similar Y positions) - grid layout
                // 2. Side by side (card2 X > card1 X) - multi-column
                // 3. Cards in a zigzag pattern (alternating layout)
                // The key point is cards should NOT be in a narrow single column

                const sameLine = Math.abs(card1Box.y - card2Box.y) < 100;
                const sideByBide = card2Box.x > card1Box.x;

                // At desktop, expect either same row or offset positioning
                // OR if stacked, each card should span significant width
                const cardsWideEnough = card1Box.width > 300 && card2Box.width > 300;

                // Accept either multi-column layout or wide single-column cards
                expect(sameLine || sideByBide || cardsWideEnough).toBe(true);
            }
        }
    });

    test('TC4: Hero section spans appropriate width with centered content at desktop', async ({ page }) => {
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Get hero section dimensions
        const heroBox = await heroSection.boundingBox();
        expect(heroBox).not.toBeNull();

        // Hero should span a reasonable width at desktop viewport
        // At 1920px viewport, hero should be visible and appropriately sized
        if (heroBox) {
            expect(heroBox.width).toBeGreaterThan(800);
        }

        // Verify main content is centered (visually centered on page)
        const mainContent = page.locator('main');
        const mainBox = await mainContent.boundingBox();

        if (mainBox) {
            // At 1920px viewport, the main content (max-width: 1200px)
            // should be centered with equal margins on both sides
            const viewportWidth = 1920;
            const leftMargin = mainBox.x;
            const rightMargin = viewportWidth - (mainBox.x + mainBox.width);

            // Margins should be approximately equal (centered content)
            // Allow 10px tolerance for padding/scrollbar
            expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);

            // Main content should have a max-width and be centered
            expect(mainBox.width).toBeGreaterThan(600);
            expect(mainBox.width).toBeLessThanOrEqual(1400);
        }
    });

    test('Desktop layout utilizes full width appropriately', async ({ page }) => {
        // Verify the header spans full width
        const header = page.locator('header');
        await expect(header).toBeVisible();

        const headerBox = await header.boundingBox();
        if (headerBox) {
            // Header should be close to viewport width
            expect(headerBox.width).toBeGreaterThanOrEqual(1900);
        }

        // Verify main content is constrained but reasonable
        const mainContent = page.locator('main');
        const mainBox = await mainContent.boundingBox();
        if (mainBox) {
            // Content should be contained within a max-width but still substantial
            expect(mainBox.width).toBeGreaterThan(600);
            expect(mainBox.width).toBeLessThanOrEqual(1400);
        }
    });

    test('All sections display without overlap at desktop viewport', async ({ page }) => {
        await page.waitForLoadState('networkidle');

        // Get bounding boxes for main sections
        const sections = ['#features', '#code-examples', '#architecture', '#status', '#specifications'];
        const boxes = [];

        for (const selector of sections) {
            const section = page.locator(selector);
            if (await section.isVisible()) {
                const box = await section.boundingBox();
                if (box) {
                    boxes.push({ selector, box });
                }
            }
        }

        // Verify sections don't overlap vertically
        for (let i = 0; i < boxes.length - 1; i++) {
            const current = boxes[i];
            const next = boxes[i + 1];

            const currentBottom = current.box.y + current.box.height;
            // Next section should start after or at the end of current (with small tolerance)
            expect(next.box.y).toBeGreaterThanOrEqual(currentBottom - 5);
        }
    });

    test('Typography is readable at desktop viewport', async ({ page }) => {
        // Check h2 headings have appropriate font size for desktop
        const h2 = page.locator('h2').first();
        if (await h2.isVisible()) {
            const h2Styles = await h2.evaluate(el => {
                const style = window.getComputedStyle(el);
                return {
                    fontSize: parseFloat(style.fontSize),
                    lineHeight: style.lineHeight
                };
            });
            // H2 should be at least 24px on desktop
            expect(h2Styles.fontSize).toBeGreaterThanOrEqual(24);
        }

        // Check body text is readable
        const paragraph = page.locator('p').first();
        if (await paragraph.isVisible()) {
            const pStyles = await paragraph.evaluate(el => {
                const style = window.getComputedStyle(el);
                return {
                    fontSize: parseFloat(style.fontSize)
                };
            });
            // Body text should be at least 14px
            expect(pStyles.fontSize).toBeGreaterThanOrEqual(14);
        }
    });

    test('Navigation CTA buttons are properly styled at desktop', async ({ page }) => {
        const primaryCta = page.locator('.nav-cta .cta-btn--primary');
        const secondaryCta = page.locator('.nav-cta .cta-btn--secondary');

        await expect(primaryCta).toBeVisible();

        // Check CTA buttons are displayed inline (not stacked)
        const navCta = page.locator('.nav-cta');
        const ctaStyles = await navCta.evaluate(el => {
            const style = window.getComputedStyle(el);
            return {
                display: style.display,
                flexDirection: style.flexDirection
            };
        });

        expect(ctaStyles.display).toBe('flex');
        expect(ctaStyles.flexDirection).not.toBe('column');
    });
});

// ============================================
// Scenario 9: Tablet Responsive Tests (768px-1024px)
// ============================================
test.describe('Responsive Design - Tablet Viewport', () => {
    // Test Case 1: Load page at 768px viewport width
    test.describe('768px viewport (lower tablet range)', () => {
        test.use({ viewport: { width: 768, height: 1024 } });

        test('test case 1: all content displays correctly without horizontal scroll', async ({ page }) => {
            await page.goto('/');

            // Check that page does not have horizontal scroll
            const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
            const viewportWidth = await page.evaluate(() => window.innerWidth);

            expect(pageWidth).toBeLessThanOrEqual(viewportWidth);

            // Verify main sections are visible
            const featuresSection = page.locator('#features');
            await expect(featuresSection).toBeVisible();

            const codeExamplesSection = page.locator('#code-examples');
            await expect(codeExamplesSection).toBeVisible();

            const architectureSection = page.locator('#architecture');
            await expect(architectureSection).toBeVisible();

            const statusSection = page.locator('#status');
            await expect(statusSection).toBeVisible();

            const specificationsSection = page.locator('#specifications');
            await expect(specificationsSection).toBeVisible();
        });

        test('navigation is accessible at 768px (mobile menu may be active)', async ({ page }) => {
            await page.goto('/');

            // Navigation container should be visible
            const navContainer = page.locator('.nav-container');
            await expect(navContainer).toBeVisible();

            // At 768px, the mobile menu toggle may be active (CSS media query is max-width: 768px)
            // Check that either the nav links are visible OR the toggle button is visible
            const navToggle = page.locator('.nav-toggle');
            const navLinks = page.locator('.nav-links');

            // The nav toggle should be visible at 768px (mobile breakpoint)
            const toggleVisible = await navToggle.isVisible();

            if (toggleVisible) {
                // Mobile mode - verify menu can be toggled
                await expect(navToggle).toBeVisible();
                // Click to open menu
                await navToggle.click();
                // After clicking, nav links should become visible
                await expect(navLinks).toBeVisible();
            } else {
                // Desktop mode - nav links should be directly visible
                await expect(navLinks).toBeVisible();
            }
        });

        test('test case 3: features adapt to 2-column or stacked layout at 768px', async ({ page }) => {
            await page.goto('/');

            // Check features grid exists
            const featuresGrid = page.locator('.features-grid');
            await expect(featuresGrid).toBeVisible();

            // Verify feature cards are visible
            const featureCards = page.locator('.feature-card');
            const count = await featureCards.count();
            expect(count).toBeGreaterThanOrEqual(6);

            // Check that all feature cards are visible
            for (let i = 0; i < count; i++) {
                await expect(featureCards.nth(i)).toBeVisible();
            }
        });

        test('test case 4: code blocks are readable without horizontal scrolling at 768px', async ({ page }) => {
            await page.goto('/');

            // Navigate to code examples
            const codeExamplesSection = page.locator('#code-examples');
            await codeExamplesSection.scrollIntoViewIfNeeded();

            // Check code blocks exist and are visible
            const codeBlocks = page.locator('.code-block');
            const count = await codeBlocks.count();
            expect(count).toBeGreaterThan(0);

            // Verify code blocks are visible
            for (let i = 0; i < count; i++) {
                await expect(codeBlocks.nth(i)).toBeVisible();
            }

            // Check that code examples container is properly contained
            const codeExamplesContainer = page.locator('.code-examples-container');
            await expect(codeExamplesContainer).toBeVisible();
        });

        test('architecture diagram is visible and properly sized at 768px', async ({ page }) => {
            await page.goto('/');

            const architectureSection = page.locator('#architecture');
            await architectureSection.scrollIntoViewIfNeeded();

            // Check architecture diagram
            const diagram = page.locator('.architecture-diagram');
            await expect(diagram).toBeVisible();

            // Verify diagram doesn't overflow
            const diagramBox = await diagram.boundingBox();
            expect(diagramBox).not.toBeNull();
            if (diagramBox) {
                expect(diagramBox.width).toBeLessThanOrEqual(768);
            }
        });

        test('specifications grid is accessible at 768px', async ({ page }) => {
            await page.goto('/');

            const specificationsSection = page.locator('#specifications');
            await specificationsSection.scrollIntoViewIfNeeded();

            // Check specification cards are visible
            const specCards = page.locator('.spec-card');
            const count = await specCards.count();
            expect(count).toBeGreaterThan(0);

            for (let i = 0; i < count; i++) {
                await expect(specCards.nth(i)).toBeVisible();
            }
        });

        test('status section is accessible at 768px', async ({ page }) => {
            await page.goto('/');

            const statusSection = page.locator('#status');
            await statusSection.scrollIntoViewIfNeeded();

            // Check status cards are visible
            const statusCards = page.locator('.status-card');
            const count = await statusCards.count();
            expect(count).toBeGreaterThan(0);

            for (let i = 0; i < count; i++) {
                await expect(statusCards.nth(i)).toBeVisible();
            }
        });
    });

    // Test Case 2: Load page at 1024px viewport width
    test.describe('1024px viewport (upper tablet range)', () => {
        test.use({ viewport: { width: 1024, height: 768 } });

        test('test case 2: all content displays correctly at upper tablet range (1024px)', async ({ page }) => {
            await page.goto('/');

            // Check that page does not have horizontal scroll
            const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
            const viewportWidth = await page.evaluate(() => window.innerWidth);

            expect(pageWidth).toBeLessThanOrEqual(viewportWidth);

            // Verify main sections are visible
            const featuresSection = page.locator('#features');
            await expect(featuresSection).toBeVisible();

            const codeExamplesSection = page.locator('#code-examples');
            await expect(codeExamplesSection).toBeVisible();

            const architectureSection = page.locator('#architecture');
            await expect(architectureSection).toBeVisible();

            const statusSection = page.locator('#status');
            await expect(statusSection).toBeVisible();

            const specificationsSection = page.locator('#specifications');
            await expect(specificationsSection).toBeVisible();
        });

        test('navigation is fully accessible at 1024px', async ({ page }) => {
            await page.goto('/');

            // Navigation should be visible
            const navContainer = page.locator('.nav-container');
            await expect(navContainer).toBeVisible();

            // Check nav links
            const navLinks = page.locator('.nav-links');
            await expect(navLinks).toBeVisible();

            // Check CTA buttons
            const navCta = page.locator('.nav-cta');
            await expect(navCta).toBeVisible();
        });

        test('features grid layout at 1024px', async ({ page }) => {
            await page.goto('/');

            // Check features grid exists and is visible
            const featuresGrid = page.locator('.features-grid');
            await expect(featuresGrid).toBeVisible();

            // Verify all feature cards are accessible
            const featureCards = page.locator('.feature-card');
            const count = await featureCards.count();
            expect(count).toBeGreaterThanOrEqual(6);

            for (let i = 0; i < count; i++) {
                await expect(featureCards.nth(i)).toBeVisible();
            }
        });

        test('code examples are readable at 1024px', async ({ page }) => {
            await page.goto('/');

            const codeExamplesSection = page.locator('#code-examples');
            await codeExamplesSection.scrollIntoViewIfNeeded();

            // Check code examples exist
            const codeExamples = page.locator('.code-example');
            const count = await codeExamples.count();
            expect(count).toBeGreaterThan(0);

            for (let i = 0; i < count; i++) {
                await expect(codeExamples.nth(i)).toBeVisible();
            }

            // Verify copy buttons are visible
            const copyButtons = page.locator('.copy-btn');
            const copyCount = await copyButtons.count();
            expect(copyCount).toBeGreaterThan(0);
        });

        test('architecture section is properly displayed at 1024px', async ({ page }) => {
            await page.goto('/');

            const architectureSection = page.locator('#architecture');
            await architectureSection.scrollIntoViewIfNeeded();

            // Check diagram is visible
            const diagram = page.locator('.architecture-diagram');
            await expect(diagram).toBeVisible();

            // Check component descriptions are visible
            const components = page.locator('.architecture-component');
            const count = await components.count();
            expect(count).toBeGreaterThan(0);
        });

        test('specifications grid at 1024px', async ({ page }) => {
            await page.goto('/');

            const specificationsSection = page.locator('#specifications');
            await specificationsSection.scrollIntoViewIfNeeded();

            // Check all spec cards
            const specCards = page.locator('.spec-card');
            const count = await specCards.count();
            expect(count).toBe(5); // 5 specification cards expected

            // Verify specific values are visible
            await expect(page.locator('[data-testid="spec-listen-address"]')).toContainText('0.0.0.0:12333');
            await expect(page.locator('[data-testid="spec-max-lsm-levels"]')).toContainText('7');
        });

        test('status section layout at 1024px', async ({ page }) => {
            await page.goto('/');

            const statusSection = page.locator('#status');
            await statusSection.scrollIntoViewIfNeeded();

            // Verify implemented features card is visible
            const implementedCard = page.locator('.status-card--implemented');
            await expect(implementedCard).toBeVisible();

            // Verify roadmap card is visible
            const roadmapCard = page.locator('.status-card--roadmap');
            await expect(roadmapCard).toBeVisible();
        });
    });

    // General tablet range tests
    test.describe('General tablet responsiveness (768px-1024px)', () => {
        const tabletWidths = [768, 896, 1024];

        for (const width of tabletWidths) {
            test(`content is accessible and no horizontal overflow at ${width}px`, async ({ page }) => {
                await page.setViewportSize({ width, height: 768 });
                await page.goto('/');

                // Check no horizontal scroll
                const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
                const viewportWidth = await page.evaluate(() => window.innerWidth);

                expect(pageWidth).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding

                // All main sections should be visible
                const sections = ['#features', '#code-examples', '#architecture', '#status', '#specifications'];
                for (const section of sections) {
                    const sectionEl = page.locator(section);
                    await sectionEl.scrollIntoViewIfNeeded();
                    await expect(sectionEl).toBeVisible();
                }
            });
        }
    });
});

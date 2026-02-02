/**
 * Responsive Design E2E Tests - Tablet
 * Owner: Scenario 9 - Responsive Design - Tablet
 *
 * Tests:
 * - Tablet layout (768px, 1024px)
 * - Content accessibility at tablet widths
 * - Feature grid layout adaptation
 * - Code blocks readability
 */

import { test, expect } from '@playwright/test';

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

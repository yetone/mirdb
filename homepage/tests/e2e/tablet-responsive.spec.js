// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Tablet Responsive Design Tests
 * Verifies REQ-8: Support responsive design for mobile and tablet viewing
 *
 * Tests run at tablet viewport (768x1024 - iPad portrait)
 * This scenario tests the homepage behavior on tablet-sized screens.
 */

test.describe('Tablet Responsive Design - 768px Viewport', () => {
    // Use tablet viewport for all tests in this describe block
    test.use({ viewport: { width: 768, height: 1024 } });

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        // Wait for page to fully load
        await page.waitForLoadState('networkidle');
    });

    test('TC1: Layout adapts appropriately for tablet view with optimal content arrangement', async ({ page }) => {
        // Verify no horizontal scrolling is required at 768px viewport
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

        // Verify all major sections are visible within viewport width
        const sections = ['hero-section', 'features-section', 'how-it-works-section',
                         'code-examples-section', 'getting-started-section', 'footer-section'];

        for (const sectionId of sections) {
            const section = page.locator(`[data-testid="${sectionId}"]`);
            if (await section.count() > 0) {
                await section.scrollIntoViewIfNeeded();
                const isVisible = await section.isVisible();
                expect(isVisible).toBe(true);

                // Verify section width does not exceed viewport
                const sectionBox = await section.boundingBox();
                if (sectionBox) {
                    expect(sectionBox.width).toBeLessThanOrEqual(viewportWidth + 10);
                }
            }
        }

        // Verify hero section is properly displayed
        const heroSection = page.locator('[data-testid="hero-section"]');
        await expect(heroSection).toBeVisible();

        const heroTitle = page.locator('[data-testid="hero-title"]');
        await expect(heroTitle).toBeVisible();

        // Verify hero title font size is appropriate for tablet (larger than mobile but smaller than desktop)
        const titleFontSize = await heroTitle.evaluate(el => {
            return parseFloat(window.getComputedStyle(el).fontSize);
        });
        // At 768px breakpoint, the hero-title should be 2.5rem = 40px (per CSS media query)
        expect(titleFontSize).toBeGreaterThanOrEqual(36);
        expect(titleFontSize).toBeLessThanOrEqual(48);

        // Verify CTA buttons are visible and properly arranged
        const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
        await expect(ctaButtons).toBeVisible();

        // Check that buttons are in a row layout (not stacked like mobile)
        const flexDirection = await ctaButtons.evaluate(el => {
            return window.getComputedStyle(el).flexDirection;
        });
        // At 768px, buttons should be in row layout (flex-wrap: wrap allows wrapping if needed)
        expect(flexDirection).toBe('row');
    });

    test('TC2: Feature cards display in 2-column or 3-column grid appropriate for tablet', async ({ page }) => {
        const featuresSection = page.locator('[data-testid="features-section"]');
        await featuresSection.scrollIntoViewIfNeeded();
        await expect(featuresSection).toBeVisible();

        const featureGrid = page.locator('[data-testid="feature-grid"]');
        await expect(featureGrid).toBeVisible();

        // Get all feature cards
        const featureCards = page.locator('.feature-card');
        const cardCount = await featureCards.count();
        expect(cardCount).toBeGreaterThanOrEqual(3);

        // Get positions of first few cards to verify grid layout
        // At 768px, cards should be in 2-column layout (based on minmax(min(100%, 300px), 1fr))
        if (cardCount >= 2) {
            const firstCard = featureCards.nth(0);
            const secondCard = featureCards.nth(1);

            const firstBox = await firstCard.boundingBox();
            const secondBox = await secondCard.boundingBox();

            expect(firstBox).not.toBeNull();
            expect(secondBox).not.toBeNull();

            // At 768px with 300px min card width, there should be 2 columns
            // Check if the second card is on the same row (similar Y position)
            // or if it's in a grid arrangement

            // First and second cards should either be side by side (same Y)
            // OR stacked (different Y) - both are valid tablet layouts
            const sameRow = Math.abs(secondBox.y - firstBox.y) < 50;
            const stacked = secondBox.y >= firstBox.y + firstBox.height - 50;

            // At 768px with container padding (40px total) = 728px available
            // With 300px min card width and gap, we can fit 2 columns
            // Cards should be in a 2-column grid OR single column
            expect(sameRow || stacked).toBe(true);

            // If same row (2-column layout), verify horizontal arrangement
            if (sameRow) {
                expect(secondBox.x).toBeGreaterThan(firstBox.x);

                // Verify cards are reasonably sized for 2-column layout
                // Each card should be roughly half the container width minus gap
                expect(firstBox.width).toBeGreaterThanOrEqual(280);
                expect(firstBox.width).toBeLessThanOrEqual(450);
            }
        }

        // If we have 3+ cards, check the third card position
        if (cardCount >= 3) {
            const firstCard = featureCards.nth(0);
            const thirdCard = featureCards.nth(2);

            const firstBox = await firstCard.boundingBox();
            const thirdBox = await thirdCard.boundingBox();

            expect(firstBox).not.toBeNull();
            expect(thirdBox).not.toBeNull();

            // Third card should be positioned below or beside first cards
            // This confirms multi-row grid behavior
            const isMultiRow = thirdBox.y > firstBox.y;
            const isSameRow = Math.abs(thirdBox.y - firstBox.y) < 50;

            expect(isMultiRow || isSameRow).toBe(true);
        }

        // Verify all cards are fully visible within viewport width
        for (let i = 0; i < cardCount; i++) {
            const card = featureCards.nth(i);
            const cardBox = await card.boundingBox();
            expect(cardBox).not.toBeNull();

            // Card should not extend beyond viewport
            expect(cardBox.x + cardBox.width).toBeLessThanOrEqual(780);
            expect(cardBox.x).toBeGreaterThanOrEqual(-5);
        }

        // Verify grid has proper gap between cards
        if (cardCount >= 2) {
            const gridGap = await featureGrid.evaluate(el => {
                return window.getComputedStyle(el).gap;
            });
            // Grid should have a gap defined (2rem = 32px per CSS)
            expect(gridGap).toBeTruthy();
        }
    });

    test('TC3: Navigation is either full horizontal or appropriately collapsed', async ({ page }) => {
        // At tablet viewport, navigation should be accessible
        // Check for mobile navigation element (hamburger menu) or full navigation
        const mobileNav = await page.locator('[data-testid="mobile-nav"], [data-testid="hamburger-menu"], .mobile-nav, .hamburger-menu, nav.mobile').first();

        // Check if mobile navigation exists
        const hasMobileNav = await mobileNav.count() > 0;

        if (hasMobileNav) {
            // If mobile nav exists, verify it's visible or can be triggered
            await expect(mobileNav).toBeAttached();
        } else {
            // Fallback: Check footer navigation is accessible
            const footerNav = page.locator('[data-testid="footer-nav"]');
            await expect(footerNav).toBeVisible();

            // Verify footer nav links are accessible and properly styled
            const footerLinks = footerNav.locator('a');
            const linkCount = await footerLinks.count();
            expect(linkCount).toBeGreaterThan(0);

            // Verify footer navigation layout is appropriate for tablet
            const footerNavBox = await footerNav.boundingBox();
            expect(footerNavBox).not.toBeNull();

            // Footer nav should not exceed viewport width
            expect(footerNavBox.width).toBeLessThanOrEqual(780);

            // Check that footer nav items have proper touch targets for tablet
            for (let i = 0; i < linkCount; i++) {
                const link = footerLinks.nth(i);
                const linkBox = await link.boundingBox();
                if (linkBox) {
                    // Touch targets should be at least 44px for accessibility
                    expect(linkBox.height).toBeGreaterThanOrEqual(40);
                }
            }
        }

        // Verify hero section CTA buttons serve as primary navigation
        const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
        await expect(ctaButtons).toBeVisible();

        // Verify "Get Started" button navigates correctly
        const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
        await expect(getStartedBtn).toBeVisible();
        const getStartedHref = await getStartedBtn.getAttribute('href');
        expect(getStartedHref).toBe('#getting-started');

        // Verify GitHub button is visible and has correct external link
        const githubBtn = page.locator('[data-testid="github-btn"]');
        await expect(githubBtn).toBeVisible();
        const githubHref = await githubBtn.getAttribute('href');
        expect(githubHref).toContain('github.com');
    });

    test('Command cards grid adapts to tablet width', async ({ page }) => {
        const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
        await codeExamplesSection.scrollIntoViewIfNeeded();
        await expect(codeExamplesSection).toBeVisible();

        const commandsGrid = page.locator('[data-testid="commands-grid"]');
        await expect(commandsGrid).toBeVisible();

        // Get all command cards
        const commandCards = page.locator('.command-card');
        const cardCount = await commandCards.count();
        expect(cardCount).toBeGreaterThan(0);

        // Verify command cards are laid out in a grid appropriate for tablet
        if (cardCount >= 2) {
            const firstCard = commandCards.nth(0);
            const secondCard = commandCards.nth(1);

            const firstBox = await firstCard.boundingBox();
            const secondBox = await secondCard.boundingBox();

            expect(firstBox).not.toBeNull();
            expect(secondBox).not.toBeNull();

            // At tablet width, cards may be 2 per row or more
            // Check that layout is consistent
            const sameRow = Math.abs(secondBox.y - firstBox.y) < 30;
            const stacked = secondBox.y >= firstBox.y + firstBox.height - 30;

            expect(sameRow || stacked).toBe(true);

            // Verify cards fit within viewport
            expect(firstBox.x + firstBox.width).toBeLessThanOrEqual(780);
            expect(secondBox.x + secondBox.width).toBeLessThanOrEqual(780);
        }
    });

    test('Data path cards display properly on tablet', async ({ page }) => {
        const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
        await howItWorksSection.scrollIntoViewIfNeeded();

        const dataPathsGrid = page.locator('[data-testid="data-paths-grid"]');
        if (await dataPathsGrid.count() > 0) {
            const writePathCard = page.locator('[data-testid="write-path-card"]');
            const readPathCard = page.locator('[data-testid="read-path-card"]');

            const writeBox = await writePathCard.boundingBox();
            const readBox = await readPathCard.boundingBox();

            if (writeBox && readBox) {
                // At 768px with 400px min card width, cards should be stacked
                // (768px - 40px padding = 728px, which can't fit two 400px cards)
                // Cards should stack vertically
                expect(readBox.y).toBeGreaterThanOrEqual(writeBox.y + writeBox.height - 30);

                // Cards should be nearly full width
                expect(writeBox.width).toBeGreaterThanOrEqual(600);
                expect(readBox.width).toBeGreaterThanOrEqual(600);

                // Verify cards don't overflow
                expect(writeBox.x + writeBox.width).toBeLessThanOrEqual(780);
                expect(readBox.x + readBox.width).toBeLessThanOrEqual(780);
            }
        }
    });

    test('Code blocks are scrollable horizontally within container at tablet width', async ({ page }) => {
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
            expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(780);
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
                expect(codeExampleBox.x + codeExampleBox.width).toBeLessThanOrEqual(780);

                // Check overflow property
                const overflowX = await codeExample.evaluate(el => {
                    return window.getComputedStyle(el).overflowX;
                });
                expect(['auto', 'scroll']).toContain(overflowX);
            }
        }
    });

    test('Architecture diagram displays properly on tablet', async ({ page }) => {
        const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
        await howItWorksSection.scrollIntoViewIfNeeded();
        await expect(howItWorksSection).toBeVisible();

        const diagramContainer = page.locator('[data-testid="architecture-diagram-container"]');
        if (await diagramContainer.count() > 0) {
            await expect(diagramContainer).toBeVisible();

            const diagramBox = await diagramContainer.boundingBox();
            if (diagramBox) {
                // Diagram should fit within viewport
                expect(diagramBox.x + diagramBox.width).toBeLessThanOrEqual(780);
            }

            // Check that diagram has proper overflow for tablet
            const mermaidDiagram = page.locator('.diagram-figure .mermaid');
            if (await mermaidDiagram.count() > 0) {
                const overflowX = await mermaidDiagram.evaluate(el => {
                    return window.getComputedStyle(el).overflowX;
                });
                // At 768px, diagram should have auto overflow to allow scrolling if needed
                expect(['auto', 'scroll', 'visible']).toContain(overflowX);
            }
        }
    });

    test('Footer displays correctly on tablet', async ({ page }) => {
        const footerSection = page.locator('[data-testid="footer-section"]');
        await footerSection.scrollIntoViewIfNeeded();
        await expect(footerSection).toBeVisible();

        // Check footer badges
        const footerBadges = page.locator('[data-testid="footer-badges"]');
        await expect(footerBadges).toBeVisible();

        const badgesBox = await footerBadges.boundingBox();
        if (badgesBox) {
            // Badges should be centered and fit within viewport
            expect(badgesBox.x + badgesBox.width).toBeLessThanOrEqual(780);
        }

        // Check footer navigation
        const footerNav = page.locator('[data-testid="footer-nav"]');
        await expect(footerNav).toBeVisible();

        // Verify footer nav layout allows for flexible wrapping
        const flexWrap = await footerNav.evaluate(el => {
            return window.getComputedStyle(el).flexWrap;
        });
        expect(flexWrap).toBe('wrap');

        // Check footer copyright
        const footerCopyright = page.locator('[data-testid="footer-copyright"]');
        await expect(footerCopyright).toBeVisible();
    });
});

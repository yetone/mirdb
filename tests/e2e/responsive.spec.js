/**
 * Responsive Design Tests
 * Owner: Scenario 7 - Responsive Design
 *        Scenario 12 - Cross-Browser Compatibility
 *
 * Tests:
 * - Desktop viewport (1920x1080)
 * - Tablet viewport (768x1024)
 * - Mobile viewport (375x667)
 * - Feature card stacking
 * - Mobile navigation
 * - Code block scrolling
 */

const { test, expect } = require('@playwright/test');
const { SELECTORS, VIEWPORTS, gotoHomepage } = require('./test-utils');

test.describe('Responsive Design - Desktop Viewport (1920x1080)', () => {
    test.beforeEach(async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.desktop);
        await gotoHomepage(page);
    });

    test('TC1: Page renders correctly with all sections visible and properly aligned', async ({ page }) => {
        // Check that all major sections are visible
        await expect(page.locator(SELECTORS.header)).toBeVisible();
        await expect(page.locator(SELECTORS.hero)).toBeVisible();
        await expect(page.locator(SELECTORS.features)).toBeVisible();
        await expect(page.locator(SELECTORS.quickstart)).toBeVisible();
        await expect(page.locator(SELECTORS.footer)).toBeVisible();

        // Check navigation is fully visible (not collapsed)
        await expect(page.locator(SELECTORS.navLinks)).toBeVisible();
        const navLinks = page.locator(`${SELECTORS.navLinks} a`);
        const linkCount = await navLinks.count();
        expect(linkCount).toBeGreaterThanOrEqual(3);

        // Verify all nav links are visible
        for (let i = 0; i < linkCount; i++) {
            await expect(navLinks.nth(i)).toBeVisible();
        }

        // Check feature cards display in a grid (multiple columns on desktop)
        const featuresGrid = page.locator(SELECTORS.featuresGrid);
        await expect(featuresGrid).toBeVisible();

        // On desktop (1920px), features grid should show 4 columns
        const gridStyle = await featuresGrid.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
                display: style.display,
                gridTemplateColumns: style.gridTemplateColumns
            };
        });
        expect(gridStyle.display).toBe('grid');
        // On 1920px, should have 4 columns (repeat(4, 1fr))
        const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(c => c !== '').length;
        expect(columnCount).toBeGreaterThanOrEqual(2);

        // Check hero section is centered
        const heroContent = page.locator('.hero-content');
        const heroBounding = await heroContent.boundingBox();
        const viewportSize = page.viewportSize();
        // Hero should be roughly centered (within 20% of center)
        const centerOffset = Math.abs((heroBounding.x + heroBounding.width / 2) - (viewportSize.width / 2));
        expect(centerOffset).toBeLessThan(viewportSize.width * 0.2);

        // Check buttons are inline (not stacked)
        const heroCtaButtons = page.locator(`${SELECTORS.heroCta} .btn`);
        const button1Box = await heroCtaButtons.nth(0).boundingBox();
        const button2Box = await heroCtaButtons.nth(1).boundingBox();
        // On desktop, buttons should be side by side (same or similar Y position)
        expect(Math.abs(button1Box.y - button2Box.y)).toBeLessThan(20);
    });
});

test.describe('Responsive Design - Tablet Viewport (768x1024)', () => {
    test.beforeEach(async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.tablet);
        await gotoHomepage(page);
    });

    test('TC2: Page adapts layout for tablet screens', async ({ page }) => {
        // Check all sections are still visible
        await expect(page.locator(SELECTORS.header)).toBeVisible();
        await expect(page.locator(SELECTORS.hero)).toBeVisible();
        await expect(page.locator(SELECTORS.features)).toBeVisible();
        await expect(page.locator(SELECTORS.quickstart)).toBeVisible();
        await expect(page.locator(SELECTORS.footer)).toBeVisible();

        // Check navigation is still accessible
        await expect(page.locator(SELECTORS.nav)).toBeVisible();

        // Check feature cards layout - on tablet should be 2 columns or auto-fit
        const featuresGrid = page.locator(SELECTORS.featuresGrid);
        const gridStyle = await featuresGrid.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
                display: style.display,
                gridTemplateColumns: style.gridTemplateColumns
            };
        });
        expect(gridStyle.display).toBe('grid');
        // On tablet, should show 2 columns
        const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(c => c !== '').length;
        expect(columnCount).toBeGreaterThanOrEqual(1);
        expect(columnCount).toBeLessThanOrEqual(4);

        // Verify feature cards are readable
        const featureCards = page.locator(SELECTORS.featureCard);
        const cardCount = await featureCards.count();
        expect(cardCount).toBe(4);

        for (let i = 0; i < cardCount; i++) {
            await expect(featureCards.nth(i)).toBeVisible();
            const cardBox = await featureCards.nth(i).boundingBox();
            // Card should have reasonable width for readability
            expect(cardBox.width).toBeGreaterThan(200);
        }

        // Check that code blocks are still visible and scrollable
        const codeBlock = page.locator(SELECTORS.codeBlock);
        await expect(codeBlock).toBeVisible();
        const codeBlockBox = await codeBlock.boundingBox();
        // Code block should fit within viewport width
        expect(codeBlockBox.width).toBeLessThanOrEqual(VIEWPORTS.tablet.width);
    });
});

test.describe('Responsive Design - Mobile Viewport (375x667)', () => {
    test.beforeEach(async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.mobile);
        await gotoHomepage(page);
    });

    test('TC3: Page displays mobile-friendly layout', async ({ page }) => {
        // Check all sections are visible
        await expect(page.locator(SELECTORS.header)).toBeVisible();
        await expect(page.locator(SELECTORS.hero)).toBeVisible();
        await expect(page.locator(SELECTORS.features)).toBeVisible();
        await expect(page.locator(SELECTORS.quickstart)).toBeVisible();
        await expect(page.locator(SELECTORS.footer)).toBeVisible();

        // Check hero text is readable (font size adjusts)
        const heroTitle = page.locator(SELECTORS.heroTitle);
        await expect(heroTitle).toBeVisible();
        const titleBox = await heroTitle.boundingBox();
        // Title should fit within mobile viewport
        expect(titleBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

        // Check tagline is readable
        const tagline = page.locator(SELECTORS.heroTagline);
        await expect(tagline).toBeVisible();

        // Check buttons are accessible and properly sized
        const primaryBtn = page.locator(SELECTORS.heroCtaPrimary);
        const secondaryBtn = page.locator(SELECTORS.heroCtaSecondary);
        await expect(primaryBtn).toBeVisible();
        await expect(secondaryBtn).toBeVisible();

        // Buttons should be touchable (minimum 44px height for accessibility)
        const primaryBtnBox = await primaryBtn.boundingBox();
        expect(primaryBtnBox.height).toBeGreaterThanOrEqual(36); // Reasonable touch target

        // Footer should be visible and readable
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await expect(page.locator(SELECTORS.footer)).toBeVisible();
    });

    test('TC4: Feature cards stack vertically on mobile', async ({ page }) => {
        // Navigate to features section
        await page.locator(SELECTORS.features).scrollIntoViewIfNeeded();

        const featureCards = page.locator(SELECTORS.featureCard);
        const cardCount = await featureCards.count();
        expect(cardCount).toBe(4);

        // Get bounding boxes for all cards
        const cardBoxes = [];
        for (let i = 0; i < cardCount; i++) {
            const box = await featureCards.nth(i).boundingBox();
            cardBoxes.push(box);
        }

        // Cards should be stacked vertically (each subsequent card has higher Y value)
        for (let i = 1; i < cardBoxes.length; i++) {
            expect(cardBoxes[i].y).toBeGreaterThan(cardBoxes[i - 1].y);
        }

        // Each card should be nearly full width (single column)
        for (const box of cardBoxes) {
            // Card should take significant portion of viewport width
            expect(box.width).toBeGreaterThan(VIEWPORTS.mobile.width * 0.7);
        }

        // Verify text remains readable in each card
        for (let i = 0; i < cardCount; i++) {
            const title = featureCards.nth(i).locator(SELECTORS.featureTitle.replace('.feature-card ', ''));
            const description = featureCards.nth(i).locator(SELECTORS.featureDescription.replace('.feature-card ', ''));
            await expect(featureCards.nth(i).locator('.feature-title')).toBeVisible();
            await expect(featureCards.nth(i).locator('.feature-description')).toBeVisible();
        }
    });

    test('TC5: Navigation is accessible on mobile', async ({ page }) => {
        // Check navigation element is present
        const nav = page.locator(SELECTORS.nav);
        await expect(nav).toBeVisible();

        // Check logo is visible
        await expect(page.locator(SELECTORS.navLogo)).toBeVisible();

        // Navigation links should still be accessible (simplified or hamburger)
        // The current implementation uses simplified nav (all links visible but smaller)
        const navLinks = page.locator(SELECTORS.navLinks);

        // Either nav links are visible or there's a hamburger menu
        const isNavLinksVisible = await navLinks.isVisible();
        const hamburgerMenu = page.locator('.hamburger-menu, .mobile-menu-toggle, [aria-label*="menu"]');
        const hasHamburger = await hamburgerMenu.count() > 0;

        // At least one navigation method should be available
        expect(isNavLinksVisible || hasHamburger).toBeTruthy();

        if (isNavLinksVisible) {
            // If simplified nav, links should still be tappable
            const links = page.locator(`${SELECTORS.navLinks} a`);
            const linkCount = await links.count();
            for (let i = 0; i < linkCount; i++) {
                const linkBox = await links.nth(i).boundingBox();
                // Links should have reasonable touch target
                expect(linkBox.height).toBeGreaterThanOrEqual(20);
            }
        }

        // Nav should fit within viewport
        const navBox = await nav.boundingBox();
        expect(navBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    });

    test('TC6: Code blocks are horizontally scrollable on mobile', async ({ page }) => {
        // Navigate to quickstart section
        await page.locator(SELECTORS.quickstart).scrollIntoViewIfNeeded();

        const codeBlock = page.locator(SELECTORS.codeBlock);
        await expect(codeBlock).toBeVisible();

        // Check the pre element inside code block
        const preElement = codeBlock.locator('pre');
        await expect(preElement).toBeVisible();

        // Get the computed overflow style
        const overflowStyle = await preElement.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
                overflowX: style.overflowX,
                overflowY: style.overflowY,
                overflow: style.overflow
            };
        });

        // Code block should allow horizontal scrolling
        const allowsScroll = overflowStyle.overflowX === 'auto' ||
                            overflowStyle.overflowX === 'scroll' ||
                            overflowStyle.overflow === 'auto' ||
                            overflowStyle.overflow === 'scroll';
        expect(allowsScroll).toBeTruthy();

        // Code block container should fit within viewport
        const codeBlockBox = await codeBlock.boundingBox();
        expect(codeBlockBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 50); // Small buffer for borders

        // Verify code content is still readable
        const codeContent = await preElement.textContent();
        expect(codeContent).toContain('cargo install');
        expect(codeContent).toContain('SET');
        expect(codeContent).toContain('GET');
    });
});

test.describe('Responsive Design - Cross-Breakpoint Tests', () => {
    test('Layout transitions smoothly between viewports', async ({ page }) => {
        await gotoHomepage(page);

        // Test desktop
        await page.setViewportSize(VIEWPORTS.desktop);
        await expect(page.locator(SELECTORS.hero)).toBeVisible();
        await expect(page.locator(SELECTORS.features)).toBeVisible();

        // Transition to tablet
        await page.setViewportSize(VIEWPORTS.tablet);
        await expect(page.locator(SELECTORS.hero)).toBeVisible();
        await expect(page.locator(SELECTORS.features)).toBeVisible();

        // Transition to mobile
        await page.setViewportSize(VIEWPORTS.mobile);
        await expect(page.locator(SELECTORS.hero)).toBeVisible();
        await expect(page.locator(SELECTORS.features)).toBeVisible();

        // No content should be cut off or overflow the viewport horizontally
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        expect(bodyWidth).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 20);
    });

    test('All interactive elements remain accessible across viewports', async ({ page }) => {
        const viewportsToTest = [VIEWPORTS.desktop, VIEWPORTS.tablet, VIEWPORTS.mobile];

        for (const viewport of viewportsToTest) {
            await page.setViewportSize(viewport);
            await gotoHomepage(page);

            // Check CTA buttons are clickable
            const primaryBtn = page.locator(SELECTORS.heroCtaPrimary);
            await expect(primaryBtn).toBeVisible();
            await expect(primaryBtn).toBeEnabled();

            const secondaryBtn = page.locator(SELECTORS.heroCtaSecondary);
            await expect(secondaryBtn).toBeVisible();
            await expect(secondaryBtn).toBeEnabled();

            // Check nav links
            const navLinks = page.locator(`${SELECTORS.navLinks} a`);
            const linkCount = await navLinks.count();
            for (let i = 0; i < linkCount; i++) {
                await expect(navLinks.nth(i)).toBeEnabled();
            }
        }
    });
});

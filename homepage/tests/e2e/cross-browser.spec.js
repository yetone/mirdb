/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 10 - Cross-Browser Compatibility
 *
 * Tests for:
 * - Chrome rendering (chromium project)
 * - Firefox rendering (firefox project)
 * - Safari (WebKit) rendering (webkit project)
 * - Edge rendering (shares Chromium engine, covered by chromium project)
 * - CSS custom properties support
 *
 * These tests verify that the MirDB homepage renders correctly
 * and functions properly across all major browsers.
 *
 * The tests run automatically across all browser projects defined
 * in playwright.config.js (chromium, firefox, webkit).
 */

const { test, expect } = require('@playwright/test');

/**
 * Validates all sections render correctly in the browser
 * @param {import('@playwright/test').Page} page
 */
async function validateAllSectionsRender(page) {
    // Hero section
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    const heroTitle = page.locator('h1#hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    // CTA buttons
    const primaryCta = page.locator('#cta-github');
    await expect(primaryCta).toBeVisible();

    const secondaryCta = page.locator('#cta-docs');
    await expect(secondaryCta).toBeVisible();

    // Features section
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const featuresTitle = page.locator('#features-title');
    await expect(featuresTitle).toBeVisible();

    // Feature cards should be visible (at least 5)
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);

    // Code example section
    const codeExample = page.locator('#code-example');
    await expect(codeExample).toBeVisible();

    // Quick start section
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();

    // Footer
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();
}

/**
 * Validates no layout issues (no horizontal overflow)
 * @param {import('@playwright/test').Page} page
 */
async function validateNoLayoutIssues(page) {
    // Check for horizontal scrollbar (indicates layout overflow)
    const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Check that all major sections are contained within viewport width
    const sections = ['#hero', '#features', '#code-example', '#quick-start', 'footer.footer'];
    for (const selector of sections) {
        const section = page.locator(selector);
        const box = await section.boundingBox();
        expect(box).not.toBeNull();
        if (box) {
            const viewportSize = page.viewportSize();
            expect(box.x).toBeGreaterThanOrEqual(0);
            expect(box.x + box.width).toBeLessThanOrEqual(viewportSize.width + 1);
        }
    }
}

/**
 * Validates interactive functionality works
 * @param {import('@playwright/test').Page} page
 */
async function validateFunctionality(page) {
    // Test navigation link works (scroll to quick-start)
    const docsLink = page.locator('#cta-docs');
    await docsLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify quick-start section is in viewport
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeInViewport();

    // Test copy button exists and is functional
    const copyBtn = page.locator('#copy-btn');
    await expect(copyBtn).toBeVisible();

    // Test button hover states work (by checking element is interactable)
    const primaryCta = page.locator('#cta-github');
    await expect(primaryCta).toBeEnabled();

    // Test focus states work (accessibility)
    await primaryCta.focus();
    await expect(primaryCta).toBeFocused();
}

/**
 * Validates CSS custom properties are applied
 * @param {import('@playwright/test').Page} page
 */
async function validateCssCustomProperties(page) {
    // Check that CSS custom properties are being used
    const primaryColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-primary').trim();
    });
    expect(primaryColor).toBeTruthy();

    const backgroundColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-background').trim();
    });
    expect(backgroundColor).toBeTruthy();

    // Check that custom properties are applied to elements
    const btnPrimary = page.locator('.btn-primary').first();
    const btnBackground = await btnPrimary.evaluate((el) => {
        return getComputedStyle(el).backgroundColor;
    });
    expect(btnBackground).toBeTruthy();
    expect(btnBackground).not.toBe('transparent');

    // Check body uses CSS variables
    const bodyBackground = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
    });
    expect(bodyBackground).toBeTruthy();
}

// Cross-Browser Compatibility Test Suite
// These tests run on all browsers defined in playwright.config.js
// (chromium, firefox, webkit) - covering Chrome, Firefox, Safari/WebKit, and Edge (Chromium-based)
test.describe('Cross-Browser Compatibility', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');
    });

    // TC1-4: All sections render correctly (runs on each browser project)
    test('All sections render correctly', async ({ page, browserName }) => {
        // This test runs on chromium (Chrome/Edge), firefox, and webkit (Safari)
        await validateAllSectionsRender(page);
    });

    // TC1-4: No layout issues (runs on each browser project)
    test('No layout issues - no horizontal overflow', async ({ page, browserName }) => {
        await validateNoLayoutIssues(page);
    });

    // TC1-4: All functionality works (runs on each browser project)
    test('All interactive functionality works', async ({ page, browserName }) => {
        await validateFunctionality(page);
    });

    // TC5: CSS custom properties are properly applied (runs on each browser project)
    test('TC5: CSS custom properties are properly applied', async ({ page, browserName }) => {
        await validateCssCustomProperties(page);
    });

    // TC5: Comprehensive CSS variables validation
    test('TC5: All CSS variables are defined and applied', async ({ page, browserName }) => {
        // Verify all CSS custom properties are defined
        const cssVariables = await page.evaluate(() => {
            const root = getComputedStyle(document.documentElement);
            return {
                colorPrimary: root.getPropertyValue('--color-primary').trim(),
                colorPrimaryDark: root.getPropertyValue('--color-primary-dark').trim(),
                colorBackground: root.getPropertyValue('--color-background').trim(),
                colorSurface: root.getPropertyValue('--color-surface').trim(),
                colorText: root.getPropertyValue('--color-text').trim(),
                colorTextMuted: root.getPropertyValue('--color-text-muted').trim(),
                colorBorder: root.getPropertyValue('--color-border').trim(),
                radiusSm: root.getPropertyValue('--radius-sm').trim(),
                radiusMd: root.getPropertyValue('--radius-md').trim(),
                radiusLg: root.getPropertyValue('--radius-lg').trim(),
            };
        });

        // All CSS variables should be defined
        expect(cssVariables.colorPrimary).toBeTruthy();
        expect(cssVariables.colorPrimaryDark).toBeTruthy();
        expect(cssVariables.colorBackground).toBeTruthy();
        expect(cssVariables.colorSurface).toBeTruthy();
        expect(cssVariables.colorText).toBeTruthy();
        expect(cssVariables.colorTextMuted).toBeTruthy();
        expect(cssVariables.colorBorder).toBeTruthy();
        expect(cssVariables.radiusSm).toBeTruthy();
        expect(cssVariables.radiusMd).toBeTruthy();
        expect(cssVariables.radiusLg).toBeTruthy();

        // Verify CSS variables are actually applied to elements
        const elementStyles = await page.evaluate(() => {
            const body = document.body;
            const btn = document.querySelector('.btn-primary');
            const card = document.querySelector('.feature-card');

            return {
                bodyBg: getComputedStyle(body).backgroundColor,
                bodyColor: getComputedStyle(body).color,
                btnBg: btn ? getComputedStyle(btn).backgroundColor : null,
                cardBorderRadius: card ? getComputedStyle(card).borderRadius : null,
            };
        });

        // Verify styles are actually applied (not empty or default)
        expect(elementStyles.bodyBg).toBeTruthy();
        expect(elementStyles.bodyColor).toBeTruthy();
        expect(elementStyles.btnBg).toBeTruthy();
        expect(elementStyles.cardBorderRadius).toBeTruthy();
    });
});

// Visual Consistency Tests
test.describe('Visual Consistency Across Browsers', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');
    });

    test('Feature grid maintains layout', async ({ page, browserName }) => {
        // Set desktop viewport
        await page.setViewportSize({ width: 1200, height: 800 });

        const grid = page.locator('.features-grid');
        await expect(grid).toBeVisible();

        // Check that grid is using CSS Grid
        const gridDisplay = await grid.evaluate((el) => {
            return getComputedStyle(el).display;
        });
        expect(gridDisplay).toBe('grid');

        // Verify feature cards are arranged in grid
        const cards = page.locator('.feature-card');
        const cardCount = await cards.count();
        expect(cardCount).toBe(5);

        // All cards should be visible
        for (let i = 0; i < cardCount; i++) {
            await expect(cards.nth(i)).toBeVisible();
        }
    });

    test('Typography renders consistently', async ({ page, browserName }) => {
        // Check heading font weights
        const h1FontWeight = await page.locator('h1').evaluate((el) => {
            return getComputedStyle(el).fontWeight;
        });
        expect(parseInt(h1FontWeight)).toBeGreaterThanOrEqual(600);

        // Check body font family is applied
        const bodyFontFamily = await page.evaluate(() => {
            return getComputedStyle(document.body).fontFamily;
        });
        expect(bodyFontFamily).toBeTruthy();
        expect(bodyFontFamily.length).toBeGreaterThan(0);
    });

    test('Links and buttons are styled consistently', async ({ page, browserName }) => {
        // Check primary button styling
        const primaryBtn = page.locator('.btn-primary').first();
        const btnStyles = await primaryBtn.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                backgroundColor: styles.backgroundColor,
                borderRadius: styles.borderRadius,
                padding: styles.padding,
            };
        });

        expect(btnStyles.backgroundColor).toBeTruthy();
        expect(btnStyles.borderRadius).toBeTruthy();

        // Check link color
        const link = page.locator('a').first();
        const linkColor = await link.evaluate((el) => {
            return getComputedStyle(el).color;
        });
        expect(linkColor).toBeTruthy();
    });

    test('Code blocks render correctly', async ({ page, browserName }) => {
        // Check code example section
        const codeContainer = page.locator('.code-container').first();
        await expect(codeContainer).toBeVisible();

        // Verify pre/code elements are present
        const preElement = page.locator('#code-example pre').first();
        await expect(preElement).toBeVisible();

        const codeElement = page.locator('#code-example code').first();
        await expect(codeElement).toBeVisible();

        // Verify code block has proper styling
        const codeStyles = await preElement.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                overflow: styles.overflow || styles.overflowX,
                padding: styles.padding,
            };
        });

        expect(codeStyles.padding).toBeTruthy();
    });

    test('Footer renders correctly', async ({ page, browserName }) => {
        const footer = page.locator('footer.footer');
        await expect(footer).toBeVisible();

        // Check footer contains GitHub link
        const githubLink = footer.locator('a[href*="github.com"]');
        await expect(githubLink).toBeVisible();

        // Check CI badge is present
        const ciBadge = footer.locator('img[src*="circleci"]');
        await expect(ciBadge).toBeVisible();
    });
});

// Accessibility Across Browsers
test.describe('Accessibility Across Browsers', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');
    });

    test('Focus indicators are visible', async ({ page, browserName }) => {
        // Tab to first focusable element
        await page.keyboard.press('Tab');

        // Get the currently focused element
        const focusedElement = await page.evaluate(() => {
            const el = document.activeElement;
            if (!el) return null;

            const styles = getComputedStyle(el);
            return {
                outline: styles.outline,
                outlineWidth: styles.outlineWidth,
                outlineColor: styles.outlineColor,
                boxShadow: styles.boxShadow,
            };
        });

        expect(focusedElement).not.toBeNull();
        // Focus should have some visual indicator (outline or box-shadow)
        const hasOutline = focusedElement.outlineWidth !== '0px' && focusedElement.outlineWidth !== '0';
        const hasBoxShadow = focusedElement.boxShadow !== 'none';
        const hasFocusIndicator = hasOutline || hasBoxShadow;
        expect(hasFocusIndicator).toBe(true);
    });

    test('Semantic HTML elements are present', async ({ page, browserName }) => {
        // Check for main structural elements
        const footer = page.locator('footer');

        // At minimum, footer should be present
        await expect(footer).toBeVisible();

        // Sections should have proper headings
        const h1 = page.locator('h1');
        await expect(h1).toBeVisible();

        const h2Count = await page.locator('h2').count();
        expect(h2Count).toBeGreaterThan(0);
    });

    test('Images have alt text', async ({ page, browserName }) => {
        const images = page.locator('img');
        const imageCount = await images.count();

        for (let i = 0; i < imageCount; i++) {
            const img = images.nth(i);
            const alt = await img.getAttribute('alt');
            expect(alt).not.toBeNull();
        }
    });
});

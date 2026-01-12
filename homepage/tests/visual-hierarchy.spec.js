// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Visual Hierarchy and Scanability (NFR-4)', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Verify each major section has a heading
    test('TC1: Each major section has a clear heading', async ({ page }) => {
        // Define the expected sections with their headings
        const expectedSections = [
            { id: 'hero', headingTag: 'h1', expectedText: 'MirDB' },
            { id: 'quick-start', headingTag: 'h2', expectedText: 'Quick Start' },
            { id: 'features', headingTag: 'h2', expectedText: 'Key Features' },
            { id: 'architecture', headingTag: 'h2', expectedText: 'Architecture Overview' },
            { id: 'configuration', headingTag: 'h2', expectedText: 'Configuration' },
            { id: 'commands', headingTag: 'h2', expectedText: 'Commands Reference' },
            { id: 'project-status', headingTag: 'h2', expectedText: 'Project Status' },
        ];

        for (const section of expectedSections) {
            // Verify section exists
            const sectionElement = page.locator(`#${section.id}`);
            await expect(sectionElement).toBeVisible();

            // Verify heading exists and has correct text
            const heading = sectionElement.locator(section.headingTag).first();
            await expect(heading).toBeVisible();
            const headingText = await heading.textContent();
            expect(headingText).toContain(section.expectedText);
        }
    });

    // Test Case 1b: Verify headings have distinct styling from body text
    test('TC1b: Section headings have larger font size than body text', async ({ page }) => {
        // Get body font size
        const bodyFontSize = await page.evaluate(() => {
            const bodyElement = document.body;
            return parseFloat(window.getComputedStyle(bodyElement).fontSize);
        });

        // Check h2 headings are significantly larger than body text
        const h2Headings = page.locator('.section h2');
        const h2Count = await h2Headings.count();
        expect(h2Count).toBeGreaterThan(0);

        for (let i = 0; i < h2Count; i++) {
            const h2FontSize = await h2Headings.nth(i).evaluate((el) => {
                return parseFloat(window.getComputedStyle(el).fontSize);
            });
            // H2 should be at least 1.5x larger than body text
            expect(h2FontSize).toBeGreaterThan(bodyFontSize * 1.5);
        }
    });

    // Test Case 2: Check whitespace between sections
    test('TC2: Adequate spacing between sections for visual separation', async ({ page }) => {
        // Get all main sections
        const sections = page.locator('section.section');
        const sectionCount = await sections.count();
        expect(sectionCount).toBeGreaterThan(0);

        // Check each section has adequate padding
        for (let i = 0; i < sectionCount; i++) {
            const section = sections.nth(i);

            // Get computed padding
            const padding = await section.evaluate((el) => {
                const style = window.getComputedStyle(el);
                return {
                    top: parseFloat(style.paddingTop),
                    bottom: parseFloat(style.paddingBottom),
                };
            });

            // Sections should have at least 40px padding (minimum for visual separation)
            expect(padding.top).toBeGreaterThanOrEqual(40);
            expect(padding.bottom).toBeGreaterThanOrEqual(40);
        }
    });

    // Test Case 2b: Verify sections have visual distinction through backgrounds
    test('TC2b: Sections have distinct visual styling (alternating backgrounds)', async ({ page }) => {
        // Get sections and check they have different backgrounds for visual separation
        const quickStart = page.locator('#quick-start');
        const features = page.locator('#features');
        const architecture = page.locator('#architecture');
        const configuration = page.locator('#configuration');

        // Get background colors
        const quickStartBg = await quickStart.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });
        const featuresBg = await features.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });
        const architectureBg = await architecture.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });
        const configurationBg = await configuration.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });

        // At least some sections should have alternating backgrounds
        // Quick Start and Configuration should have same background (both are light grey)
        // Features and Architecture should have same background (both are white)
        const hasVariation = quickStartBg !== featuresBg || architectureBg !== configurationBg;
        expect(hasVariation).toBe(true);
    });

    // Test Case 3: Check text hierarchy within sections
    test('TC3: Headings, subheadings, and body text have distinct styling', async ({ page }) => {
        // Check the Quick Start section which has h2, h3, and p elements
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Get h2 styles
        const h2 = quickStartSection.locator('h2').first();
        const h2Styles = await h2.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
                fontSize: parseFloat(style.fontSize),
                fontWeight: parseInt(style.fontWeight),
            };
        });

        // Get h3 styles
        const h3 = quickStartSection.locator('h3').first();
        const h3Styles = await h3.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
                fontSize: parseFloat(style.fontSize),
                fontWeight: parseInt(style.fontWeight),
            };
        });

        // Get p styles
        const p = quickStartSection.locator('p').first();
        const pStyles = await p.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
                fontSize: parseFloat(style.fontSize),
                fontWeight: parseInt(style.fontWeight),
            };
        });

        // Verify hierarchy: h2 > h3 > p in terms of font size
        expect(h2Styles.fontSize).toBeGreaterThan(h3Styles.fontSize);
        expect(h3Styles.fontSize).toBeGreaterThan(pStyles.fontSize);

        // Verify headings have bolder font weight than body text
        expect(h2Styles.fontWeight).toBeGreaterThanOrEqual(600);
        expect(h3Styles.fontWeight).toBeGreaterThanOrEqual(400);
    });

    // Test Case 3b: Verify subsection headings exist in complex sections
    test('TC3b: Complex sections have subsection headings (h3)', async ({ page }) => {
        // Commands section should have h3 for command categories
        const commandsSection = page.locator('#commands');
        await expect(commandsSection).toBeVisible();

        const commandH3s = commandsSection.locator('h3');
        const commandH3Count = await commandH3s.count();

        // Should have multiple subsections: Storage Commands, Retrieval Commands, etc.
        expect(commandH3Count).toBeGreaterThanOrEqual(3);

        // Verify subsection headings are visible
        const expectedSubheadings = ['Storage Commands', 'Retrieval Commands', 'Deletion Commands'];
        for (const heading of expectedSubheadings) {
            await expect(commandsSection.getByText(heading, { exact: false })).toBeVisible();
        }

        // Architecture section should have Write Path and Read Path subsections
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        const archH3s = architectureSection.locator('h3');
        const archH3Count = await archH3s.count();
        expect(archH3Count).toBeGreaterThanOrEqual(2);

        await expect(architectureSection.getByText('Write Path', { exact: false })).toBeVisible();
        await expect(architectureSection.getByText('Read Path', { exact: false })).toBeVisible();
    });

    // Test Case 3c: Verify text line height for readability
    test('TC3c: Body text has adequate line height for readability', async ({ page }) => {
        // Check line height of paragraph text
        const paragraphs = page.locator('.section p');
        const pCount = await paragraphs.count();

        if (pCount > 0) {
            // Check first visible paragraph
            for (let i = 0; i < pCount; i++) {
                const p = paragraphs.nth(i);
                if (await p.isVisible()) {
                    const lineHeight = await p.evaluate((el) => {
                        const style = window.getComputedStyle(el);
                        const fontSize = parseFloat(style.fontSize);
                        const lineHeightValue = style.lineHeight;
                        if (lineHeightValue === 'normal') {
                            return 1.2; // Default line-height
                        }
                        return parseFloat(lineHeightValue) / fontSize;
                    });

                    // Line height should be at least 1.4 for good readability
                    expect(lineHeight).toBeGreaterThanOrEqual(1.4);
                    break;
                }
            }
        }
    });

    // Additional test: Verify feature cards have visual hierarchy
    test('Feature cards have clear visual hierarchy with heading and description', async ({ page }) => {
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        const featureCards = featuresSection.locator('.feature-card');
        const cardCount = await featureCards.count();
        expect(cardCount).toBeGreaterThanOrEqual(4);

        // Check first card has both h3 and p with proper hierarchy
        const firstCard = featureCards.first();
        const cardH3 = firstCard.locator('h3');
        const cardP = firstCard.locator('p');

        await expect(cardH3).toBeVisible();
        await expect(cardP).toBeVisible();

        // Get styles to verify hierarchy
        const cardH3Size = await cardH3.evaluate((el) => parseFloat(window.getComputedStyle(el).fontSize));
        const cardPSize = await cardP.evaluate((el) => parseFloat(window.getComputedStyle(el).fontSize));

        expect(cardH3Size).toBeGreaterThan(cardPSize);
    });

    // Additional test: Hero section has proper visual hierarchy
    test('Hero section has clear visual hierarchy (logo, title, tagline, CTAs)', async ({ page }) => {
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Check elements exist and are in proper order
        const logo = heroSection.locator('.hero-logo');
        const title = heroSection.locator('.hero-title');
        const tagline = heroSection.locator('.hero-tagline');
        const ctas = heroSection.locator('.hero-ctas');

        await expect(logo).toBeVisible();
        await expect(title).toBeVisible();
        await expect(tagline).toBeVisible();
        await expect(ctas).toBeVisible();

        // Verify title is larger than tagline
        const titleSize = await title.evaluate((el) => parseFloat(window.getComputedStyle(el).fontSize));
        const taglineSize = await tagline.evaluate((el) => parseFloat(window.getComputedStyle(el).fontSize));

        expect(titleSize).toBeGreaterThan(taglineSize);
    });

    // Additional test: Command tables have clear structure
    test('Command tables have clear header and row styling', async ({ page }) => {
        const commandsSection = page.locator('#commands');
        await expect(commandsSection).toBeVisible();

        const tables = commandsSection.locator('.commands-table');
        const tableCount = await tables.count();
        expect(tableCount).toBeGreaterThanOrEqual(3);

        // Check first table has distinct header styling
        const firstTable = tables.first();
        const tableHeader = firstTable.locator('thead');
        const tableBody = firstTable.locator('tbody');

        await expect(tableHeader).toBeVisible();
        await expect(tableBody).toBeVisible();

        // Get header background color
        const headerBg = await tableHeader.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });

        // Get body background color
        const bodyBg = await tableBody.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });

        // Header should have distinct styling from body (darker background)
        expect(headerBg).not.toBe(bodyBg);
    });
});

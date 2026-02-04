/**
 * No-JavaScript Fallback Tests
 * Scenario 14 - No-JavaScript Fallback
 *
 * Validates that core content is accessible and functional when JavaScript is disabled.
 * Tests NFR-4: Homepage must work without JavaScript for core content accessibility
 */
const { test, expect } = require('@playwright/test');

test.describe('No-JavaScript Fallback', () => {
    // Configure all tests in this file to run with JavaScript disabled
    test.use({ javaScriptEnabled: false });

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Page loads and displays all content sections with JavaScript disabled', async ({ page }) => {
        // Verify page loaded successfully
        await expect(page).toHaveTitle(/MirDB/);

        // Check all main sections are visible
        const heroSection = page.locator('#hero');
        const featuresSection = page.locator('#features');
        const codeExamplesSection = page.locator('#code-examples');
        const architectureSection = page.locator('#architecture');
        const roadmapSection = page.locator('#roadmap');
        const footer = page.locator('footer');

        await expect(heroSection).toBeVisible();
        await expect(featuresSection).toBeVisible();
        await expect(codeExamplesSection).toBeVisible();
        await expect(architectureSection).toBeVisible();
        await expect(roadmapSection).toBeVisible();
        await expect(footer).toBeVisible();
    });

    test('TC2: Hero section displays logo, heading, tagline, and CTA buttons without JS', async ({ page }) => {
        // Check logo is visible
        const logo = page.locator('.hero__logo');
        await expect(logo).toBeVisible();
        await expect(logo).toHaveAttribute('src', /logo\.gif/);
        await expect(logo).toHaveAttribute('alt', /MirDB/);

        // Check main heading
        const heading = page.locator('#hero-heading');
        await expect(heading).toBeVisible();
        await expect(heading).toHaveText('MirDB');

        // Check tagline
        const tagline = page.locator('.hero__tagline');
        await expect(tagline).toBeVisible();
        await expect(tagline).toContainText('Persistent Key-Value Store');
        await expect(tagline).toContainText('Memcached Protocol');

        // Check CTA buttons are visible and have correct hrefs
        const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
        const githubBtn = page.locator('[data-testid="cta-github"]');

        await expect(getStartedBtn).toBeVisible();
        await expect(getStartedBtn).toHaveText('Get Started');
        await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');

        await expect(githubBtn).toBeVisible();
        await expect(githubBtn).toContainText('GitHub');
        await expect(githubBtn).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
    });

    test('TC3: Code examples section displays at least one code example without JS', async ({ page }) => {
        // The first tab panel (SET) should be visible by default
        const setPanel = page.locator('#panel-set');
        await expect(setPanel).toBeVisible();

        // Verify code content is visible
        const codeBlock = setPanel.locator('.code-block');
        await expect(codeBlock).toBeVisible();

        // Check that the code contains expected commands
        const codeContent = await codeBlock.textContent();
        expect(codeContent).toContain('telnet');
        expect(codeContent).toContain('SET');
        expect(codeContent).toContain('STORED');

        // Verify section heading is visible
        const heading = page.locator('#code-examples-heading');
        await expect(heading).toBeVisible();
        await expect(heading).toHaveText('Quick Start');
    });

    test('TC4: All links navigate correctly without JavaScript', async ({ page }) => {
        // Test internal anchor links
        const getStartedLink = page.locator('[data-testid="cta-get-started"]');
        await expect(getStartedLink).toHaveAttribute('href', '#getting-started');

        // Test external GitHub links
        const githubLink = page.locator('[data-testid="cta-github"]');
        await expect(githubLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);
        await expect(githubLink).toHaveAttribute('target', '_blank');
        await expect(githubLink).toHaveAttribute('rel', /noopener/);

        // Test footer links
        const footerGithubLink = page.locator('[data-testid="github-link"]');
        await expect(footerGithubLink).toBeVisible();
        await expect(footerGithubLink).toHaveAttribute('target', '_blank');

        const licenseLink = page.locator('[data-testid="license-link"]');
        await expect(licenseLink).toBeVisible();
        await expect(licenseLink).toContainText('MIT');

        const issuesLink = page.locator('[data-testid="issues-link"]');
        await expect(issuesLink).toBeVisible();
        await expect(issuesLink).toContainText('Issues');

        // Test architecture documentation link
        const archDocLink = page.locator('.architecture__docs a');
        await expect(archDocLink).toBeVisible();
        await expect(archDocLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb/);

        // Verify internal anchor navigation works (click and check URL or scroll)
        // Note: Without JS, anchor links still work via browser native behavior
        const currentUrl = page.url();
        await getStartedLink.click();
        // The page should navigate to the anchor
        await expect(page).toHaveURL(/#getting-started/);
    });

    test('TC5: All 6 feature cards are visible with content without JS', async ({ page }) => {
        // Feature cards container
        const featureCards = page.locator('.feature-card');

        // Should have exactly 6 feature cards
        await expect(featureCards).toHaveCount(6);

        // Define expected features
        const expectedFeatures = [
            { attr: 'memcached-compatible', title: 'Memcached Compatible' },
            { attr: 'persistent-storage', title: 'Persistent Storage' },
            { attr: 'high-performance', title: 'High Performance' },
            { attr: 'efficient-compaction', title: 'Efficient Compaction' },
            { attr: 'memory-safe', title: 'Memory Safe' },
            { attr: 'configurable', title: 'Configurable' }
        ];

        // Check each feature card
        for (const feature of expectedFeatures) {
            const card = page.locator(`[data-feature="${feature.attr}"]`);
            await expect(card).toBeVisible();

            // Check title
            const title = card.locator('.feature-card__title');
            await expect(title).toBeVisible();
            await expect(title).toHaveText(feature.title);

            // Check description exists
            const description = card.locator('.feature-card__description');
            await expect(description).toBeVisible();
            const descText = await description.textContent();
            expect(descText.length).toBeGreaterThan(10);

            // Check icon exists
            const icon = card.locator('.feature-card__icon svg');
            await expect(icon).toBeVisible();
        }

        // Verify features section heading
        const heading = page.locator('#features-heading');
        await expect(heading).toBeVisible();
        await expect(heading).toHaveText('Features');
    });

    test('Architecture section is visible and readable without JS', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // SVG diagram should be visible
        const svgDiagram = page.locator('[data-testid="architecture-svg"]');
        await expect(svgDiagram).toBeVisible();

        // Write path explanation visible
        const writePath = page.locator('[data-testid="write-path"]');
        await expect(writePath).toBeVisible();
        const writePathText = await writePath.textContent();
        expect(writePathText).toContain('Write-Ahead Log');
        expect(writePathText).toContain('Memtable');

        // Read path explanation visible
        const readPath = page.locator('[data-testid="read-path"]');
        await expect(readPath).toBeVisible();
        const readPathText = await readPath.textContent();
        expect(readPathText).toContain('Memtable');
        expect(readPathText).toContain('SSTables');
    });

    test('Roadmap section displays progress without JS', async ({ page }) => {
        const roadmapSection = page.locator('#roadmap');
        await expect(roadmapSection).toBeVisible();

        // Check roadmap items
        const completedItems = page.locator('.roadmap-item--completed');
        const plannedItems = page.locator('.roadmap-item--planned');

        // Should have 4 completed and 1 planned item
        await expect(completedItems).toHaveCount(4);
        await expect(plannedItems).toHaveCount(1);

        // Verify progress indicator
        const progressBar = page.locator('[role="progressbar"]');
        await expect(progressBar).toBeVisible();
        await expect(progressBar).toHaveAttribute('aria-valuenow', '80');
    });

    test('Footer displays all required information without JS', async ({ page }) => {
        const footer = page.locator('[data-testid="footer"]');
        await expect(footer).toBeVisible();

        // Check copyright
        const copyright = page.locator('[data-testid="copyright"]');
        await expect(copyright).toBeVisible();
        await expect(copyright).toContainText('MirDB');

        // Check license info
        const licenseInfo = page.locator('[data-testid="license-info"]');
        await expect(licenseInfo).toBeVisible();
        await expect(licenseInfo).toContainText('MIT');
    });

    test('Page is accessible without JavaScript', async ({ page }) => {
        // Verify skip link exists (accessibility)
        const skipLink = page.locator('a[href="#main-content"]');
        // Skip link might be visually hidden but should exist
        await expect(skipLink).toHaveCount(1);

        // Main content landmark exists
        const main = page.locator('main[role="main"]');
        await expect(main).toBeVisible();

        // Check heading hierarchy
        const h1 = page.locator('h1');
        await expect(h1).toHaveCount(1); // Only one h1

        // Footer landmark exists
        const footerLandmark = page.locator('footer[role="contentinfo"]');
        await expect(footerLandmark).toBeVisible();
    });
});

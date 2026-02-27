// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Progressive Enhancement Tests
 *
 * These tests validate that the MirDB homepage works correctly
 * without JavaScript enabled. All core content and functionality
 * must be accessible without JS - JavaScript is an enhancement,
 * not a requirement.
 */

test.describe('Progressive Enhancement - No JavaScript', () => {
    // Disable JavaScript for all tests in this describe block
    test.use({ javaScriptEnabled: false });

    test('Test Case 1: Page loads and displays all content sections with JS disabled', async ({ page }) => {
        await page.goto('/');

        // Verify the page loads successfully
        await expect(page).toHaveTitle(/MirDB/);

        // Verify all main sections are present and visible
        const navigation = page.locator('nav.main-nav');
        await expect(navigation).toBeVisible();

        const heroSection = page.locator('#hero, .hero');
        await expect(heroSection).toBeVisible();

        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        const benchmarksSection = page.locator('#benchmarks');
        await expect(benchmarksSection).toBeVisible();

        const footer = page.locator('footer.main-footer');
        await expect(footer).toBeVisible();
    });

    test('Test Case 2: Hero content is fully visible and readable without JS', async ({ page }) => {
        await page.goto('/');

        // Check hero section is visible
        const heroSection = page.locator('.hero');
        await expect(heroSection).toBeVisible();

        // Check h1 heading is visible and contains MirDB
        const h1 = page.locator('.hero h1');
        await expect(h1).toBeVisible();
        await expect(h1).toHaveText('MirDB');

        // Check tagline is visible
        const tagline = page.locator('.hero-tagline');
        await expect(tagline).toBeVisible();
        await expect(tagline).toContainText('Persistent Memcached-Compatible');

        // Check description is visible
        const description = page.locator('.hero-description');
        await expect(description).toBeVisible();

        // Check CTA buttons are visible
        const primaryCta = page.locator('.hero .btn-primary');
        await expect(primaryCta).toBeVisible();
        await expect(primaryCta).toHaveText(/Get Started/);

        const secondaryCta = page.locator('.hero .btn-secondary');
        await expect(secondaryCta).toBeVisible();
        await expect(secondaryCta).toHaveText(/Quick Start/);
    });

    test('Test Case 3: All feature cards are visible without JS', async ({ page }) => {
        await page.goto('/');

        // Navigate to features section
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        // Check the features heading
        const featuresHeading = page.locator('#features h2');
        await expect(featuresHeading).toBeVisible();
        await expect(featuresHeading).toHaveText('Features');

        // Check all feature cards are visible (should be 6)
        const featureCards = page.locator('.feature-card');
        await expect(featureCards).toHaveCount(6);

        // Verify each card is visible
        for (let i = 0; i < 6; i++) {
            await expect(featureCards.nth(i)).toBeVisible();
        }

        // Check that feature titles are readable
        const featureTitles = page.locator('.feature-title');
        await expect(featureTitles).toHaveCount(6);

        // Verify expected feature titles are present
        const expectedTitles = [
            'Memcached Protocol',
            'Durable Persistence',
            'TTL Support',
            'Fault Tolerance',
            'Compaction',
            'High Performance'
        ];

        for (const title of expectedTitles) {
            const featureWithTitle = page.locator('.feature-title', { hasText: title });
            await expect(featureWithTitle).toBeVisible();
        }

        // Check that feature descriptions are readable
        const featureDescriptions = page.locator('.feature-description');
        await expect(featureDescriptions).toHaveCount(6);
        for (let i = 0; i < 6; i++) {
            await expect(featureDescriptions.nth(i)).toBeVisible();
        }
    });

    test('Test Case 4: Code is displayed in pre/code blocks, readable without highlighting', async ({ page }) => {
        await page.goto('/');

        // Check quick start section is visible
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Check code blocks exist and are visible
        const codeBlocks = page.locator('.code-block');
        await expect(codeBlocks.first()).toBeVisible();

        // Verify pre elements contain code
        const preElements = page.locator('#quick-start pre');
        const preCount = await preElements.count();
        expect(preCount).toBeGreaterThan(0);

        // Check that code elements within pre are visible
        const codeElements = page.locator('#quick-start pre code');
        const codeCount = await codeElements.count();
        expect(codeCount).toBeGreaterThan(0);

        // Verify Python code example is readable
        const pythonCode = page.locator('[data-language="python"] code');
        await expect(pythonCode).toBeVisible();
        const pythonText = await pythonCode.textContent();
        expect(pythonText).toContain('pymemcache');
        expect(pythonText).toContain('client.set');
        expect(pythonText).toContain('client.get');

        // Verify Go code example is readable
        const goCode = page.locator('[data-language="go"] code');
        await expect(goCode).toBeVisible();
        const goText = await goCode.textContent();
        expect(goText).toContain('memcache');
        expect(goText).toContain('mc.Set');
        expect(goText).toContain('mc.Get');
    });

    test('Test Case 5: Browser scrolls to section using standard anchor behavior', async ({ page }) => {
        await page.goto('/');

        // Get initial scroll position
        const initialScrollY = await page.evaluate(() => window.scrollY);

        // Click on a navigation link (should work without JS using standard anchor)
        await page.locator('a[href="#features"]').first().click();

        // Wait for potential navigation/scroll
        await page.waitForTimeout(500);

        // Verify the URL hash changed (standard anchor behavior)
        const currentUrl = page.url();
        expect(currentUrl).toContain('#features');

        // Verify the features section is now in view
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeInViewport();

        // Test another navigation link
        await page.locator('a[href="#benchmarks"]').first().click();
        await page.waitForTimeout(500);

        expect(page.url()).toContain('#benchmarks');
        const benchmarksSection = page.locator('#benchmarks');
        await expect(benchmarksSection).toBeInViewport();

        // Test quick-start navigation
        await page.locator('a[href="#quick-start"]').first().click();
        await page.waitForTimeout(500);

        expect(page.url()).toContain('#quick-start');
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeInViewport();
    });

    test('Test Case 6: SVG diagram is visible without JS', async ({ page }) => {
        await page.goto('/');

        // Check architecture section is visible
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check that the SVG diagram exists and is visible
        const svgDiagram = page.locator('.architecture-diagram');
        await expect(svgDiagram).toBeVisible();

        // Verify SVG has proper accessibility attributes
        const svgRole = await svgDiagram.getAttribute('role');
        expect(svgRole).toBe('img');

        const svgAriaLabel = await svgDiagram.getAttribute('aria-label');
        expect(svgAriaLabel).toBeTruthy();
        expect(svgAriaLabel).toContain('LSM');

        // Check that key SVG elements are present
        const svgTitle = page.locator('.architecture-diagram title');
        await expect(svgTitle).toHaveText(/MirDB.*Architecture/i);

        // Verify diagram contains key components (text elements)
        const diagramText = await svgDiagram.textContent();
        expect(diagramText).toContain('Client');
        expect(diagramText).toContain('Memtable');
        expect(diagramText).toContain('SSTable');
        expect(diagramText).toContain('WAL');
        expect(diagramText).toContain('Compaction');
    });

    test('Test Case 7: Page displays in default theme without JS', async ({ page }) => {
        await page.goto('/');

        // Without JavaScript, the theme should be applied via CSS
        // The page should still be readable and styled

        // Check that the page has proper styling applied
        const body = page.locator('body');
        await expect(body).toBeVisible();

        // Check that the body has a background color (theme applied)
        const bodyBgColor = await body.evaluate((el) =>
            window.getComputedStyle(el).backgroundColor
        );
        // Body background color should be set (not transparent) - comes from theme.css
        expect(bodyBgColor).not.toBe('rgba(0, 0, 0, 0)');

        // Check that the hero section is styled
        const hero = page.locator('.hero');
        await expect(hero).toBeVisible();

        // Hero uses a linear-gradient, so check the background-image property
        const heroBackground = await hero.evaluate((el) =>
            window.getComputedStyle(el).backgroundImage
        );
        // Should have a gradient or be 'none' (if solid color)
        // The hero uses linear-gradient so it should not be 'none'
        expect(heroBackground).toContain('linear-gradient');

        // Verify text is readable (has computed styles)
        const h1 = page.locator('h1');
        await expect(h1).toBeVisible();

        // Check h1 text color is set (not default browser black)
        const h1Color = await h1.evaluate((el) =>
            window.getComputedStyle(el).color
        );
        expect(h1Color).toBeTruthy();
        // Color should be from CSS variables (typically rgb format)
        expect(h1Color).toMatch(/rgb/);

        // Check that CSS variables are being used (theme.css is loaded)
        // By verifying that content has proper contrast/styling
        const heroContent = page.locator('.hero-content');
        await expect(heroContent).toBeVisible();

        // Verify the navigation is styled
        const nav = page.locator('.site-header');
        await expect(nav).toBeVisible();
        const navBgColor = await nav.evaluate((el) =>
            window.getComputedStyle(el).backgroundColor
        );
        // Nav background should be set
        expect(navBgColor).not.toBe('rgba(0, 0, 0, 0)');

        // Verify footer is styled
        const footer = page.locator('.main-footer');
        await expect(footer).toBeVisible();
        const footerBgColor = await footer.evaluate((el) =>
            window.getComputedStyle(el).backgroundColor
        );
        // Footer background should be set (dark color)
        expect(footerBgColor).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('Test Case 8: All links are clickable and navigate correctly without JS', async ({ page }) => {
        await page.goto('/');

        // Check external links in navigation
        const githubNavLink = page.locator('nav a[href*="github.com"]').first();
        await expect(githubNavLink).toBeVisible();
        const githubNavHref = await githubNavLink.getAttribute('href');
        expect(githubNavHref).toContain('github.com/mirdb/mirdb');

        // Verify external link has proper attributes
        const target = await githubNavLink.getAttribute('target');
        expect(target).toBe('_blank');
        const rel = await githubNavLink.getAttribute('rel');
        expect(rel).toContain('noopener');

        // Check hero CTA button link
        const heroCta = page.locator('.hero a[href*="github.com"]');
        await expect(heroCta).toBeVisible();
        const heroCtaHref = await heroCta.getAttribute('href');
        expect(heroCtaHref).toContain('github.com/mirdb/mirdb');

        // Check footer links
        const footerLinks = page.locator('footer a');
        const footerLinkCount = await footerLinks.count();
        expect(footerLinkCount).toBeGreaterThan(0);

        // Verify GitHub repository link in footer
        const githubRepoLink = page.locator('footer a[href*="github.com/mirdb/mirdb"]').first();
        await expect(githubRepoLink).toBeVisible();

        // Verify issues link in footer
        const issuesLink = page.locator('footer a[href*="github.com/mirdb/mirdb/issues"]');
        await expect(issuesLink).toBeVisible();
        const issuesHref = await issuesLink.getAttribute('href');
        expect(issuesHref).toContain('/issues');

        // Verify license link in footer
        const licenseLink = page.locator('footer a[href*="LICENSE"]');
        await expect(licenseLink).toBeVisible();

        // Verify internal anchor links work
        const internalLinks = page.locator('a[href^="#"]');
        const internalLinkCount = await internalLinks.count();
        expect(internalLinkCount).toBeGreaterThan(0);

        // Click an internal link and verify navigation works
        await page.locator('a[href="#architecture"]').first().click();
        await page.waitForTimeout(300);
        expect(page.url()).toContain('#architecture');
    });
});

test.describe('Progressive Enhancement - Content Accessibility', () => {
    test.use({ javaScriptEnabled: false });

    test('Skip link is accessible without JS', async ({ page }) => {
        await page.goto('/');

        // Check skip link exists
        const skipLink = page.locator('.skip-link');
        await expect(skipLink).toHaveCount(1);

        // Skip link should have proper href
        const skipHref = await skipLink.getAttribute('href');
        expect(skipHref).toBe('#main-content');
    });

    test('Main content landmark exists and is accessible', async ({ page }) => {
        await page.goto('/');

        // Check main element exists
        const main = page.locator('main#main-content');
        await expect(main).toBeVisible();
    });

    test('Navigation landmark exists with proper aria-label', async ({ page }) => {
        await page.goto('/');

        const nav = page.locator('nav[aria-label="Main navigation"]');
        await expect(nav).toBeVisible();
    });

    test('Footer navigation exists with proper aria-label', async ({ page }) => {
        await page.goto('/');

        const footerNav = page.locator('footer nav[aria-label="Footer navigation"]');
        await expect(footerNav).toBeVisible();
    });

    test('Benchmark table is accessible without JS', async ({ page }) => {
        await page.goto('/');

        // Check table exists and is visible
        const table = page.locator('.benchmarks-table');
        await expect(table).toBeVisible();

        // Check table has proper structure
        const thead = page.locator('.benchmarks-table thead');
        await expect(thead).toBeVisible();

        const tbody = page.locator('.benchmarks-table tbody');
        await expect(tbody).toBeVisible();

        // Check column headers exist
        const headers = page.locator('.benchmarks-table th[scope="col"]');
        const headerCount = await headers.count();
        expect(headerCount).toBeGreaterThan(0);

        // Check row headers exist
        const rowHeaders = page.locator('.benchmarks-table th[scope="row"]');
        const rowHeaderCount = await rowHeaders.count();
        expect(rowHeaderCount).toBe(3); // MirDB, Memcached, Redis

        // Check data is visible
        const mirdbRow = page.locator('.benchmarks-table tbody tr').first();
        await expect(mirdbRow).toContainText('MirDB');
        await expect(mirdbRow).toContainText('185,000');
    });

    test('Copy buttons are present but functionality depends on JS', async ({ page }) => {
        await page.goto('/');

        // Copy buttons should exist in the markup
        const copyButtons = page.locator('.copy-btn');
        const buttonCount = await copyButtons.count();
        expect(buttonCount).toBeGreaterThan(0);

        // Verify buttons have accessible labels
        for (let i = 0; i < buttonCount; i++) {
            const ariaLabel = await copyButtons.nth(i).getAttribute('aria-label');
            expect(ariaLabel).toContain('Copy');
        }
    });
});

/**
 * User Journey - Complete Flow E2E Tests
 * Verifies the complete user journey from landing to taking action works seamlessly
 * Scenario UUID: 296cc040-7b6c-42a9-92d9-b483f4a3fdf4
 */

const { test, expect } = require('@playwright/test');

test.describe('User Journey - Complete Flow', () => {
    test.beforeEach(async ({ page }) => {
        // Navigate to the homepage
        await page.goto('file://' + process.cwd() + '/index.html');
    });

    /**
     * Test Case 1: Complete user journey from landing to GitHub
     * Input: Complete user journey from landing to GitHub
     * Expected: User can navigate from hero to GitHub link within 3 clicks
     */
    test('TC1-E2E: User can navigate from hero to GitHub link within 3 clicks', async ({ page }) => {
        // Step 1: Verify user lands on homepage and sees hero section immediately
        const hero = page.locator('header.hero');
        await expect(hero).toBeVisible();

        // Verify product name and tagline are visible
        const productName = page.locator('header.hero h1');
        await expect(productName).toHaveText('MirDB');

        const tagline = page.locator('header.hero .tagline');
        await expect(tagline).toBeVisible();
        await expect(tagline).toContainText('Persistent Key-Value Store');

        // Step 2: User can click "View on GitHub" button directly (1 click)
        const viewOnGitHubBtn = page.locator('a.btn-secondary:has-text("View on GitHub")');
        await expect(viewOnGitHubBtn).toBeVisible();

        // Verify GitHub button has correct href
        const heroGithubHref = await viewOnGitHubBtn.getAttribute('href');
        expect(heroGithubHref).toBe('https://github.com/yetone/mirdb');

        // Alternative path: User can also reach GitHub via footer (max 2 clicks - scroll + click)
        const footerGitHubLink = page.locator('footer a:has-text("GitHub")');
        await expect(footerGitHubLink).toBeVisible();

        const footerGithubHref = await footerGitHubLink.getAttribute('href');
        expect(footerGithubHref).toBe('https://github.com/yetone/mirdb');

        // Count all GitHub links - user has multiple paths to GitHub
        const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
        const count = await githubLinks.count();
        expect(count).toBeGreaterThanOrEqual(1);
    });

    /**
     * Test Case 2: Navigate from hero to quick start
     * Input: Navigate from hero to quick start
     * Expected: Get Started CTA successfully scrolls to quick start section
     */
    test('TC2-E2E: Get Started CTA successfully scrolls to quick start section', async ({ page }) => {
        // Step 1: Verify "Get Started" button is visible in hero
        const getStartedBtn = page.locator('a.btn-primary:has-text("Get Started")');
        await expect(getStartedBtn).toBeVisible();

        // Verify the button links to #quick-start
        const href = await getStartedBtn.getAttribute('href');
        expect(href).toBe('#quick-start');

        // Step 2: Click the "Get Started" button
        await getStartedBtn.click();

        // Step 3: Wait for smooth scroll and verify quick-start section is now in viewport
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Verify the quick-start section is scrolled into view
        // The section should be near the top of the viewport after clicking
        await expect(quickStartSection).toBeInViewport();

        // Verify the quick-start section has expected content
        const quickStartHeading = page.locator('#quick-start h2');
        await expect(quickStartHeading).toHaveText('Quick Start');

        // Verify installation instructions are visible
        const installationSection = page.locator('#quick-start .installation');
        await expect(installationSection).toBeVisible();
    });

    /**
     * Test Case 3: Find installation instructions from landing
     * Input: Find installation instructions from landing
     * Expected: Installation commands are discoverable within first scroll
     */
    test('TC3-E2E: Installation commands are discoverable within first scroll', async ({ page }) => {
        // Step 1: Verify page structure allows quick discovery
        // The quick-start section should be placed right after features
        const quickStart = page.locator('#quick-start');
        await expect(quickStart).toBeVisible();

        // Step 2: Verify installation section contains installation commands
        const installationHeading = page.locator('#quick-start h3:has-text("Installation")');
        await expect(installationHeading).toBeVisible();

        // Step 3: Verify code blocks with installation commands are present
        const codeBlocks = page.locator('#quick-start pre code');
        const codeBlockCount = await codeBlocks.count();
        expect(codeBlockCount).toBeGreaterThanOrEqual(1);

        // Step 4: Verify specific installation commands are present
        const installationCode = page.locator('#quick-start .installation pre code');
        const codeContent = await installationCode.textContent();

        // Should contain git clone command
        expect(codeContent).toContain('git clone');
        expect(codeContent).toContain('github.com/yetone/mirdb');

        // Should contain cargo build command
        expect(codeContent).toContain('cargo build');

        // Should contain cargo run command
        expect(codeContent).toContain('cargo run');

        // Step 5: Verify "Get Started" CTA provides direct path to installation
        const getStartedBtn = page.locator('a.btn-primary:has-text("Get Started")');
        await expect(getStartedBtn).toBeVisible();

        // Clicking should take user directly to quick-start with installation
        await getStartedBtn.click();
        await expect(quickStart).toBeInViewport();
    });

    /**
     * Test Case 4: Verify content flow
     * Input: Verify content flow
     * Expected: Page content follows logical flow: hero -> features -> quick start -> footer
     */
    test('TC4-E2E: Page content follows logical flow: hero -> features -> quick start -> footer', async ({ page }) => {
        // Verify all major sections exist
        const hero = page.locator('header.hero');
        const features = page.locator('section.features');
        const quickStart = page.locator('section#quick-start');
        const footer = page.locator('footer');

        await expect(hero).toBeVisible();
        await expect(features).toBeVisible();
        await expect(quickStart).toBeVisible();
        await expect(footer).toBeVisible();

        // Verify sections appear in correct order by checking their vertical positions
        const heroBox = await hero.boundingBox();
        const featuresBox = await features.boundingBox();
        const quickStartBox = await quickStart.boundingBox();
        const footerBox = await footer.boundingBox();

        // Hero should be at the top
        expect(heroBox.y).toBeLessThan(featuresBox.y);

        // Features should come after hero
        expect(featuresBox.y).toBeLessThan(quickStartBox.y);

        // Quick start should come after features
        expect(quickStartBox.y).toBeLessThan(footerBox.y);

        // Verify hero section content (value proposition)
        const heroHeading = page.locator('header.hero h1');
        await expect(heroHeading).toHaveText('MirDB');

        const tagline = page.locator('header.hero .tagline');
        await expect(tagline).toContainText('Persistent Key-Value Store');
        await expect(tagline).toContainText('Memcached Protocol');

        // Verify features section content
        const featuresHeading = page.locator('section.features h2');
        await expect(featuresHeading).toHaveText('Key Features');

        // Verify there are feature cards
        const featureCards = page.locator('section.features .feature-card');
        const featureCount = await featureCards.count();
        expect(featureCount).toBeGreaterThanOrEqual(3);

        // Verify quick start section content
        const quickStartHeading = page.locator('section#quick-start h2');
        await expect(quickStartHeading).toHaveText('Quick Start');

        // Verify footer section content
        const footerLinks = page.locator('footer .footer-links a');
        const footerLinksCount = await footerLinks.count();
        expect(footerLinksCount).toBeGreaterThanOrEqual(3);

        // Verify footer has GitHub, Documentation, and License links
        await expect(page.locator('footer a:has-text("GitHub")')).toBeVisible();
        await expect(page.locator('footer a:has-text("Documentation")')).toBeVisible();
        await expect(page.locator('footer a:has-text("License")')).toBeVisible();
    });

    // Additional journey tests for comprehensive coverage

    /**
     * Test: User journey - Understanding value proposition in hero
     * Verifies step 2 of the user journey
     */
    test('TC-E2E: Hero section clearly communicates MirDB value proposition', async ({ page }) => {
        // User should understand within 5 seconds what MirDB is
        const hero = page.locator('header.hero');
        await expect(hero).toBeVisible();

        // Logo should be visible
        const logo = page.locator('header.hero .logo');
        await expect(logo).toBeVisible();

        // Check logo has alt text
        const logoAlt = await logo.getAttribute('alt');
        expect(logoAlt).toBeTruthy();
        expect(logoAlt.toLowerCase()).toContain('mirdb');

        // Product name should be prominent
        const productName = page.locator('header.hero h1');
        await expect(productName).toHaveText('MirDB');

        // Tagline should explain what the product is
        const tagline = page.locator('header.hero .tagline');
        await expect(tagline).toBeVisible();
        const taglineText = await tagline.textContent();

        // Key value propositions should be in tagline
        expect(taglineText).toContain('Persistent');
        expect(taglineText).toContain('Key-Value Store');
        expect(taglineText).toContain('Memcached');

        // CTAs should be clear
        const ctaButtons = page.locator('header.hero .cta-buttons a');
        const ctaCount = await ctaButtons.count();
        expect(ctaCount).toBe(2);
    });

    /**
     * Test: User journey - Exploring features
     * Verifies step 3 of the user journey
     */
    test('TC-E2E: Feature highlights build on value proposition', async ({ page }) => {
        // Scroll to features section
        const features = page.locator('section.features');
        await features.scrollIntoViewIfNeeded();

        // Features heading should be visible
        const featuresHeading = page.locator('section.features h2');
        await expect(featuresHeading).toHaveText('Key Features');

        // Verify 3 key feature cards are present
        const featureCards = page.locator('section.features .feature-card');
        const count = await featureCards.count();
        expect(count).toBe(3);

        // Verify each feature card has heading and description
        for (let i = 0; i < count; i++) {
            const card = featureCards.nth(i);
            const heading = card.locator('h3');
            const description = card.locator('p');

            await expect(heading).toBeVisible();
            await expect(description).toBeVisible();
        }

        // Verify specific features are mentioned
        const featureText = await features.textContent();
        expect(featureText).toContain('Memcached Compatible');
        expect(featureText).toContain('Persistent Storage');
        expect(featureText).toContain('High Performance');
    });

    /**
     * Test: User journey - Taking action (step 5)
     * Verifies clear next steps are available
     */
    test('TC-E2E: Clear next steps available for user action', async ({ page }) => {
        // Primary action: Get Started
        const getStartedBtn = page.locator('a.btn-primary:has-text("Get Started")');
        await expect(getStartedBtn).toBeVisible();

        // Secondary action: View on GitHub
        const viewOnGithubBtn = page.locator('a.btn-secondary:has-text("View on GitHub")');
        await expect(viewOnGithubBtn).toBeVisible();

        // Verify both CTAs are in the hero area
        const hero = page.locator('header.hero');
        await expect(hero.locator('a.btn-primary')).toBeVisible();
        await expect(hero.locator('a.btn-secondary')).toBeVisible();

        // Footer also provides action paths
        const footer = page.locator('footer');
        const footerLinks = footer.locator('a');
        const footerLinkCount = await footerLinks.count();
        expect(footerLinkCount).toBeGreaterThanOrEqual(3);
    });
});

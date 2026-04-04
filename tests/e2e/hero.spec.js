/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section Display and Content
 *        Scenario 10 - GitHub Link Functionality
 *
 * Tests:
 * - Product name visibility
 * - Logo display and alt text
 * - Tagline content
 * - Get Started CTA functionality
 * - View on GitHub CTA and link
 */

const { test, expect } = require('@playwright/test');
const { selectors, GITHUB_URL, gotoHomepage } = require('./test-utils');

test.describe('Hero Section Display and Content', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    // Test Case 1: Product name 'MirDB' is visible in hero section
    test('should display product name MirDB in hero section', async ({ page }) => {
        const heroTitle = page.locator(selectors.heroTitle);

        await expect(heroTitle).toBeVisible();
        await expect(heroTitle).toHaveText('MirDB');
    });

    // Test Case 2: Logo image is displayed with proper alt text
    test('should display logo image with proper alt text', async ({ page }) => {
        const logo = page.locator(selectors.heroLogo);

        await expect(logo).toBeVisible();

        // Check that the logo has proper alt text
        const altText = await logo.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.toLowerCase()).toContain('mirdb');

        // Check that the logo src points to logo.gif
        const src = await logo.getAttribute('src');
        expect(src).toMatch(/logo\.gif$/);
    });

    // Test Case 3: Tagline contains expected text
    test('should display tagline with Persistent Key-Value Store with Memcached Protocol', async ({ page }) => {
        const tagline = page.locator(selectors.heroTagline);

        await expect(tagline).toBeVisible();
        await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');
    });

    // Test Case 4: Get Started button exists and links to Quick Start section
    test('should have Get Started button linking to Quick Start section', async ({ page }) => {
        const getStartedBtn = page.locator(selectors.getStartedBtn);

        await expect(getStartedBtn).toBeVisible();
        await expect(getStartedBtn).toHaveText('Get Started');

        // Check that it links to the quickstart section
        const href = await getStartedBtn.getAttribute('href');
        expect(href).toBe('#quickstart');

        // Click the button and verify navigation to quickstart section
        await getStartedBtn.click();
        await expect(page).toHaveURL(/#quickstart$/);
    });

    // Test Case 5: View on GitHub button exists and links to correct repository
    test('should have View on GitHub button linking to correct repository', async ({ page }) => {
        const githubBtn = page.locator(selectors.githubBtn);

        await expect(githubBtn).toBeVisible();
        await expect(githubBtn).toHaveText('View on GitHub');

        // Check that it links to the correct GitHub repository
        const href = await githubBtn.getAttribute('href');
        expect(href).toBe(GITHUB_URL);

        // Check that it opens in a new tab
        const target = await githubBtn.getAttribute('target');
        expect(target).toBe('_blank');

        // Check for security rel attribute
        const rel = await githubBtn.getAttribute('rel');
        expect(rel).toContain('noopener');
    });

    // Additional test: Hero section is visible and has proper structure
    test('should have a properly structured hero section', async ({ page }) => {
        const heroSection = page.locator(selectors.hero);

        await expect(heroSection).toBeVisible();

        // Check ARIA label for accessibility
        const ariaLabel = await heroSection.getAttribute('aria-label');
        expect(ariaLabel).toBe('Hero');

        // Check role attribute
        const role = await heroSection.getAttribute('role');
        expect(role).toBe('region');
    });

    // Additional test: CTA buttons container exists with both buttons
    test('should have hero CTA container with both primary and secondary buttons', async ({ page }) => {
        const ctaContainer = page.locator(selectors.heroCta);

        await expect(ctaContainer).toBeVisible();

        // Check that both buttons exist in the CTA container
        const primaryBtn = ctaContainer.locator('.btn-primary');
        const secondaryBtn = ctaContainer.locator('.btn-secondary');

        await expect(primaryBtn).toBeVisible();
        await expect(secondaryBtn).toBeVisible();
    });
});

/**
 * GitHub Link Functionality Tests
 * Owner: Scenario 10 - GitHub Repository Link Functionality
 *
 * Tests REQ-5: Homepage shall link to GitHub repository for source code access
 * Validates all GitHub links point to correct repository with proper attributes
 */
test.describe('GitHub Link Functionality', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    // Test Case 1: Query all anchor elements with GitHub URLs
    // All GitHub links point to https://github.com/yetone/mirdb
    test('should have all GitHub links pointing to correct repository', async ({ page }) => {
        // Find all anchor elements with GitHub repository URL
        const githubLinks = page.locator(`a[href="${GITHUB_URL}"]`);

        // Should have multiple GitHub links (nav, hero CTA, footer)
        const count = await githubLinks.count();
        expect(count).toBeGreaterThanOrEqual(3);

        // Verify each link has the correct href
        for (let i = 0; i < count; i++) {
            const href = await githubLinks.nth(i).getAttribute('href');
            expect(href).toBe(GITHUB_URL);
        }
    });

    // Test Case 2: Check View on GitHub button href
    // Hero CTA links to https://github.com/yetone/mirdb
    test('should have View on GitHub button in hero linking to correct repository', async ({ page }) => {
        const githubBtn = page.locator(selectors.githubBtn);

        await expect(githubBtn).toBeVisible();
        await expect(githubBtn).toHaveText('View on GitHub');

        // Verify href points to correct repository
        const href = await githubBtn.getAttribute('href');
        expect(href).toBe(GITHUB_URL);
    });

    // Test Case 3: Verify links open in new tab
    // External GitHub links have target='_blank' and rel='noopener'
    test('should have all external GitHub links with proper security attributes', async ({ page }) => {
        // Find all links to GitHub repository
        const githubLinks = page.locator(`a[href="${GITHUB_URL}"]`);
        const count = await githubLinks.count();

        expect(count).toBeGreaterThanOrEqual(1);

        // Each GitHub link should open in new tab with security attributes
        for (let i = 0; i < count; i++) {
            const link = githubLinks.nth(i);

            // Verify target="_blank" for new tab
            const target = await link.getAttribute('target');
            expect(target).toBe('_blank');

            // Verify rel contains "noopener" for security
            const rel = await link.getAttribute('rel');
            expect(rel).toBeTruthy();
            expect(rel).toContain('noopener');
        }
    });

    // Additional test: Navigation GitHub link
    test('should have GitHub link in navigation menu', async ({ page }) => {
        const navGithubLink = page.locator('.nav-links a[href*="github.com"]');

        await expect(navGithubLink).toBeVisible();

        // Check text content
        const text = await navGithubLink.textContent();
        expect(text).toBe('GitHub');

        // Verify it links to correct repo
        const href = await navGithubLink.getAttribute('href');
        expect(href).toBe(GITHUB_URL);

        // Verify security attributes
        const target = await navGithubLink.getAttribute('target');
        const rel = await navGithubLink.getAttribute('rel');
        expect(target).toBe('_blank');
        expect(rel).toContain('noopener');
    });

    // Additional test: Footer GitHub link
    test('should have GitHub link in footer', async ({ page }) => {
        const footerGithubLink = page.locator(selectors.footerLink);

        await expect(footerGithubLink).toBeVisible();

        // Verify it links to correct repo
        const href = await footerGithubLink.getAttribute('href');
        expect(href).toBe(GITHUB_URL);

        // Verify text content
        const text = await footerGithubLink.textContent();
        expect(text).toBe('View on GitHub');

        // Verify security attributes
        const target = await footerGithubLink.getAttribute('target');
        const rel = await footerGithubLink.getAttribute('rel');
        expect(target).toBe('_blank');
        expect(rel).toContain('noopener');
    });
});

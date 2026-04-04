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

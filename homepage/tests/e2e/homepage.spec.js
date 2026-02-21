/**
 * Homepage E2E Tests
 * Owner: Multiple scenarios contribute to this file
 *
 * Test Sections:
 * - Hero section tests (Scenario 1)
 * - Features section tests (Scenario 2)
 * - Quick Start section tests (Scenario 3)
 * - Status section tests (Scenario 4)
 * - Theme toggle tests (Scenario 8)
 *
 * Framework: Playwright
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepageUrl = 'file://' + path.resolve(__dirname, '../../index.html');

/* ========================================
   Hero Section Tests (Scenario 1)
   ======================================== */
test.describe('Hero Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('hero section is visible with h1 containing MirDB', async ({ page }) => {
        // Check hero section exists and is visible
        const hero = page.locator('.hero');
        await expect(hero).toBeVisible();

        // Check h1 contains 'MirDB'
        const h1 = page.locator('.hero h1');
        await expect(h1).toBeVisible();
        await expect(h1).toContainText('MirDB');
    });

    test('tagline contains Persistent Key-Value Store and Memcached', async ({ page }) => {
        const tagline = page.locator('.hero-tagline');
        await expect(tagline).toBeVisible();

        const taglineText = await tagline.textContent();
        expect(taglineText).toContain('Persistent Key-Value Store');
        expect(taglineText).toContain('Memcached');
    });

    test('View Repository button links to GitHub and opens in new tab', async ({ page }) => {
        const viewRepoBtn = page.locator('a.btn-primary:has-text("View Repository")');
        await expect(viewRepoBtn).toBeVisible();

        // Check href is correct
        const href = await viewRepoBtn.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');

        // Check opens in new tab
        const target = await viewRepoBtn.getAttribute('target');
        expect(target).toBe('_blank');

        // Check has noopener noreferrer for security
        const rel = await viewRepoBtn.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
    });

    test('Get Started button links to Quick Start section', async ({ page }) => {
        const getStartedBtn = page.locator('a.btn-secondary:has-text("Get Started")');
        await expect(getStartedBtn).toBeVisible();

        // Check href points to quickstart section
        const href = await getStartedBtn.getAttribute('href');
        expect(href).toBe('#quickstart');
    });

    test('hero section has semantic header element with role', async ({ page }) => {
        // Check hero is a header element with banner role
        const hero = page.locator('header.hero');
        await expect(hero).toBeVisible();

        const role = await hero.getAttribute('role');
        expect(role).toBe('banner');
    });
});

/* ========================================
   Features Section Tests (Scenario 2)
   ======================================== */
test.describe('Features Section', () => {
    // Tests will be added by Scenario 2
});

/* ========================================
   Quick Start Section Tests (Scenario 3)
   ======================================== */
test.describe('Quick Start Section', () => {
    // Tests will be added by Scenario 3
});

/* ========================================
   Status Section Tests (Scenario 4)
   ======================================== */
test.describe('Status Section', () => {
    // Tests will be added by Scenario 4
});

/* ========================================
   Theme Toggle Tests (Scenario 8)
   ======================================== */
test.describe('Theme Toggle', () => {
    // Tests will be added by Scenario 8
});

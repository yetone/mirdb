/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section and Project Overview
 *
 * Test cases:
 * - Hero contains MirDB title and tagline
 * - Key differentiators visible (persistence, Memcached, Rust)
 * - CTA buttons for GitHub and Getting Started
 * - Semantic HTML structure (h1, header, nav)
 */
const { test, expect } = require('@playwright/test');

test.describe('Hero Section and Project Overview', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000');
  });

  test('TC1: Hero contains MirDB title and tagline mentioning persistent key-value store and Memcached protocol', async ({ page }) => {
    // Verify hero section exists
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify MirDB title
    const title = hero.locator('.hero__title');
    await expect(title).toContainText('MirDB');

    // Verify tagline contains required keywords
    const tagline = hero.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  test('TC2: Hero highlights key differentiators - persistence, Memcached protocol, Rust implementation', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Get all differentiator badges
    const badges = hero.locator('.hero__badge');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThanOrEqual(3);

    // Collect badge text
    const badgeTexts = [];
    for (let i = 0; i < badgeCount; i++) {
      const text = await badges.nth(i).textContent();
      badgeTexts.push(text.toLowerCase());
    }
    const combinedText = badgeTexts.join(' ');

    // Verify key differentiators are present
    expect(combinedText).toContain('persistence');
    expect(combinedText).toContain('memcached');
    expect(combinedText).toContain('rust');
  });

  test('TC3: At least two CTA buttons present - one for GitHub repo, one for Getting Started section', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Get CTA buttons
    const ctaButtons = hero.locator('.hero__btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);

    // Verify GitHub button exists and links to GitHub
    const githubBtn = hero.locator('a[href*="github"]');
    await expect(githubBtn).toBeVisible();
    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toContain('github');

    // Verify Getting Started button exists and links to the section
    const getStartedBtn = hero.locator('a[href="#getting-started"]');
    await expect(getStartedBtn).toBeVisible();
  });

  test('TC4: Hero section uses semantic HTML (header, h1, nav elements)', async ({ page }) => {
    // Verify hero is in a header element
    const heroHeader = page.locator('header#hero');
    await expect(heroHeader).toBeVisible();

    // Verify h1 exists (should be unique on the page)
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Verify h1 is inside hero
    const heroH1 = page.locator('#hero h1');
    await expect(heroH1).toBeVisible();
    await expect(heroH1).toContainText('MirDB');

    // Verify nav element exists on the page
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();
  });
});

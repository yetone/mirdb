/**
 * Roadmap Section E2E Tests
 * Owner: Scenario 5 - Status & Roadmap Section
 *
 * Tests:
 * - CI status badge visibility and link
 * - Badge tooltip/title attribute
 * - Completed features checklist with checked indicators
 * - Planned features with "Coming Soon" label
 * - Badge image load failure fallback
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');
const fileUrl = 'file://' + indexPath;

test.describe('Roadmap Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(fileUrl);
  });

  test('roadmap section is visible', async ({ page }) => {
    const roadmap = page.locator('section#roadmap');
    await expect(roadmap).toBeVisible();
  });

  test('section title is displayed correctly', async ({ page }) => {
    const title = page.locator('section#roadmap h2.section-title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Status & Roadmap');
  });

  test('CI badge link exists and points to CircleCI', async ({ page }) => {
    const badgeLink = page.locator('.ci-badge-link');
    await expect(badgeLink).toBeVisible();

    const href = await badgeLink.getAttribute('href');
    expect(href).toBe('https://circleci.com/gh/yetone/mirdb');
  });

  test('CI badge image has correct src', async ({ page }) => {
    const badgeImage = page.locator('.ci-badge');
    await expect(badgeImage).toHaveCount(1);

    const src = await badgeImage.getAttribute('src');
    expect(src).toBe('https://circleci.com/gh/yetone/mirdb.svg?style=shield');
  });

  test('CI badge link has title attribute for tooltip', async ({ page }) => {
    const badgeLink = page.locator('.ci-badge-link');
    const title = await badgeLink.getAttribute('title');
    expect(title).toBe('CircleCI build status: yetone/mirdb');
  });

  test('CI badge link has aria-label for accessibility', async ({ page }) => {
    const badgeLink = page.locator('.ci-badge-link');
    const ariaLabel = await badgeLink.getAttribute('aria-label');
    expect(ariaLabel).toBe('CircleCI build status for yetone/mirdb');
  });

  test('completed section has 4 completed items', async ({ page }) => {
    const completedItems = page.locator('.roadmap-completed .roadmap-item--completed');
    await expect(completedItems).toHaveCount(4);
  });

  test('completed items have check indicators', async ({ page }) => {
    const checkMarks = page.locator('.roadmap-item--completed .roadmap-check');
    const count = await checkMarks.count();
    expect(count).toBe(4);

    for (let i = 0; i < count; i++) {
      const text = await checkMarks.nth(i).textContent();
      expect(text).toContain('✓');
    }
  });

  test('all completed features are listed', async ({ page }) => {
    const completedTexts = page.locator('.roadmap-item--completed .roadmap-item-text');
    const texts = await completedTexts.allTextContents();

    expect(texts).toContain('Tokio with Memcached protocol');
    expect(texts).toContain('Memtable with SkipList');
    expect(texts).toContain('Minor compaction');
    expect(texts).toContain('Major compaction');
  });

  test('Raft consensus is listed as planned', async ({ page }) => {
    const plannedItems = page.locator('.roadmap-item--planned');
    await expect(plannedItems).toHaveCount(1);

    const text = await plannedItems.locator('.roadmap-item-text').textContent();
    expect(text).toBe('Raft consensus');
  });

  test('planned items have Coming Soon label', async ({ page }) => {
    const comingSoonLabel = page.locator('.roadmap-coming-soon-label');
    await expect(comingSoonLabel).toBeVisible();
    await expect(comingSoonLabel).toHaveText('Coming Soon');
  });

  test('planned items have unchecked indicator', async ({ page }) => {
    const unchecked = page.locator('.roadmap-item--planned .roadmap-check');
    const text = await unchecked.textContent();
    expect(text).toContain('◯');
  });

  test('badge fallback element exists and is hidden by default', async ({ page }) => {
    const fallback = page.locator('.ci-badge-fallback');
    await expect(fallback).toHaveCount(1);

    const display = await fallback.evaluate((el) => window.getComputedStyle(el).display);
    expect(display).toBe('none');
  });

  test('fallback shows status dot', async ({ page }) => {
    const dot = page.locator('.ci-badge-fallback-dot');
    await expect(dot).toBeVisible({ state: 'hidden' });
  });
});

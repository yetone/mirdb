/**
 * Project Status and Links Section Tests
 * Owner: Scenario 6 - Project Status and Links Section
 *
 * Test cases:
 * - Implemented features list (Tokio, Memtable, compaction)
 * - Planned features list (Raft consensus)
 * - GitHub repository link
 * - Documentation link
 * - Contribution guidelines link
 * - External links open in new tab with noopener
 */
const { test, expect } = require('@playwright/test');

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
  });

  test('TC1: Status shows Tokio-based async networking as implemented', async ({ page }) => {
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Check for implemented features list
    const implementedList = statusSection.locator('.status__implemented');
    await expect(implementedList).toBeVisible();

    // Verify Tokio networking is listed as implemented
    const tokioFeature = implementedList.locator('text=Tokio-based async networking with memcached protocol');
    await expect(tokioFeature).toBeVisible();
  });

  test('TC2: Status shows Memtable with skip list as implemented', async ({ page }) => {
    const statusSection = page.locator('#status');
    const implementedList = statusSection.locator('.status__implemented');
    await expect(implementedList).toBeVisible();

    // Verify Memtable is listed as implemented
    const memtableFeature = implementedList.locator('text=Memtable with skip list data structure');
    await expect(memtableFeature).toBeVisible();
  });

  test('TC3: Status shows minor and major compaction as implemented', async ({ page }) => {
    const statusSection = page.locator('#status');
    const implementedList = statusSection.locator('.status__implemented');
    await expect(implementedList).toBeVisible();

    // Verify minor compaction is listed
    const minorCompaction = implementedList.locator('text=Minor compaction');
    await expect(minorCompaction).toBeVisible();

    // Verify major compaction is listed
    const majorCompaction = implementedList.locator('text=Major compaction');
    await expect(majorCompaction).toBeVisible();
  });

  test('TC4: Status shows Raft consensus as planned/upcoming', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Check for planned features list
    const plannedList = statusSection.locator('.status__planned');
    await expect(plannedList).toBeVisible();

    // Verify Raft consensus is listed as planned
    const raftFeature = plannedList.locator('text=Raft consensus for distributed operation');
    await expect(raftFeature).toBeVisible();
  });
});

test.describe('Footer Links Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
  });

  test('TC5: Footer contains valid GitHub repository link that opens in new tab', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find the main GitHub repository link (not #readme or subpages)
    const githubLink = footer.locator('a[href="https://github.com/mirdb/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify it opens in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify it uses https
    const href = await githubLink.getAttribute('href');
    expect(href).toMatch(/^https:\/\//);
  });

  test('TC6: Footer contains link to documentation or README', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find documentation link - could be README or docs
    const docsLink = footer.locator('a').filter({ hasText: /documentation|docs|readme/i });
    await expect(docsLink).toBeVisible();
  });

  test('TC7: Footer contains link to contribution guidelines or issues page', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find the contributing guidelines link
    const contributeLink = footer.locator('a[href*="CONTRIBUTING"]');
    await expect(contributeLink).toBeVisible();

    // Also verify issues link exists
    const issuesLink = footer.locator('a[href*="issues"]');
    await expect(issuesLink).toBeVisible();
  });

  test('TC8: All external links use https and have target=_blank with rel=noopener', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Get all external links in footer
    const externalLinks = footer.locator('a[href^="https://"]');
    const linkCount = await externalLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Verify each external link has proper attributes
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);

      // Check target="_blank"
      await expect(link).toHaveAttribute('target', '_blank');

      // Check rel contains "noopener"
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});

test.describe('Status Section Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
  });

  test('Status section has proper heading hierarchy', async ({ page }) => {
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify section has h2 heading
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Project Status');
  });

  test('Feature lists use semantic list elements', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Verify implemented features use ul/li
    const implementedList = statusSection.locator('.status__implemented ul, .status__implemented ol');
    await expect(implementedList).toBeVisible();

    // Verify planned features use ul/li
    const plannedList = statusSection.locator('.status__planned ul, .status__planned ol');
    await expect(plannedList).toBeVisible();
  });
});

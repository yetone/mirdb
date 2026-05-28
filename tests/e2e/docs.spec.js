/**
 * E2E Tests: Documentation Links and Contributors
 * Owner: Scenario 8 - Documentation Links and Contributors
 *
 * Expected test coverage:
 * - GitHub repository link is present and navigates correctly
 * - Contributing information is present
 * - Contributor credits are displayed
 * - Sticky nav contains links to all major sections
 * - Footer contains copyright notice
 * - Documentation links section with organized links
 */

const { test, expect } = require('@playwright/test');

test.describe('Documentation Links and Contributors', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer contains GitHub repository link, contributor credits, and copyright notice', async ({ page }) => {
    const footer = page.locator('footer.site-footer');
    await expect(footer).toBeVisible();

    // GitHub repository link
    const githubLink = footer.locator('a[href="https://github.com/mirdb/mirdb"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('GitHub Repository');

    // Copyright notice
    const copyright = footer.locator('.copyright');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('2026 MirDB Contributors');
    await expect(copyright).toContainText('MIT License');
  });

  test('GitHub repository link opens in a new tab', async ({ page, context }) => {
    const githubLink = page.locator('footer.site-footer a[href="https://github.com/mirdb/mirdb"]');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify clicking opens a new page (we intercept the popup)
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click(),
    ]);

    // The new page URL should contain github.com/mirdb/mirdb
    expect(newPage.url()).toContain('github.com/mirdb/mirdb');
    await newPage.close();
  });

  test('contributing guide link is present in footer', async ({ page }) => {
    const contributingLink = page.locator('footer.site-footer a[href*="CONTRIBUTING.md"]');
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveText('Contributing Guide');
    await expect(contributingLink).toHaveAttribute('target', '_blank');
  });

  test('contributor names and avatars are displayed', async ({ page }) => {
    const contributorsSection = page.locator('section#contributors');
    await expect(contributorsSection).toBeVisible();

    // Check section heading
    const heading = contributorsSection.locator('h2');
    await expect(heading).toHaveText('Contributors');

    // Check that contributor cards exist
    const contributorCards = contributorsSection.locator('.contributor-card');
    await expect(contributorCards).toHaveCount(4);

    // Verify at least one contributor name is visible
    const firstName = contributorsSection.locator('.contributor-name').first();
    await expect(firstName).toBeVisible();
    await expect(firstName).not.toBeEmpty();

    // Verify at least one contributor avatar is visible
    const firstAvatar = contributorsSection.locator('.contributor-avatar').first();
    await expect(firstAvatar).toBeVisible();

    // Verify contributor roles are displayed
    const firstRole = contributorsSection.locator('.contributor-role').first();
    await expect(firstRole).toBeVisible();
    await expect(firstRole).not.toBeEmpty();
  });

  test('documentation links section contains organized links', async ({ page }) => {
    const docsSection = page.locator('section#docs');
    await expect(docsSection).toBeVisible();

    // Check section heading
    const heading = docsSection.locator('h2');
    await expect(heading).toHaveText('Documentation & Resources');

    // API Documentation link
    const apiLink = docsSection.locator('a:has-text("API Documentation")');
    await expect(apiLink).toBeVisible();
    await expect(apiLink).toHaveAttribute('target', '_blank');

    // Contributing Guide link
    const contributingLink = docsSection.locator('a:has-text("Contributing Guide")');
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveAttribute('target', '_blank');

    // Configuration Reference link
    const configLink = docsSection.locator('a:has-text("Configuration Reference")');
    await expect(configLink).toBeVisible();
    await expect(configLink).toHaveAttribute('target', '_blank');

    // Verify all docs cards have icons
    const docsCards = docsSection.locator('.docs-card');
    const cardCount = await docsCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < cardCount; i++) {
      const icon = docsCards.nth(i).locator('.docs-icon');
      await expect(icon).toBeVisible();
    }
  });

  test('sticky navigation contains anchor links to all major sections', async ({ page }) => {
    const nav = page.locator('nav.main-nav');
    await expect(nav).toBeVisible();

    // Verify sticky header behavior
    const header = page.locator('header.site-header');
    const position = await header.evaluate(el =>
      window.getComputedStyle(el).position
    );
    expect(position).toBe('sticky');

    // Check all major section links are present
    const expectedLinks = [
      { href: '#features', text: 'Features' },
      { href: '#quickstart', text: 'Quick Start' },
      { href: '#installation', text: 'Installation' },
      { href: '#architecture', text: 'Architecture' },
      { href: '#benchmarks', text: 'Benchmarks' },
      { href: '#configuration', text: 'Configuration' },
      { href: '#docs', text: 'Docs' },
    ];

    for (const link of expectedLinks) {
      const navLink = nav.locator(`a[href="${link.href}"]`);
      await expect(navLink).toBeVisible();
      await expect(navLink).toHaveText(link.text);
    }
  });

  test('nav link to docs section scrolls to docs', async ({ page }) => {
    const docsLink = page.locator('nav.main-nav a[href="#docs"]');
    await expect(docsLink).toBeVisible();

    await docsLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(300);

    // Verify docs section is in viewport
    const docsSection = page.locator('section#docs');
    const isInViewport = await docsSection.evaluate(el => {
      const rect = el.getBoundingClientRect();
      return rect.top >= 0 && rect.top < window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });

  test('footer contains issues and pull requests links', async ({ page }) => {
    const footer = page.locator('footer.site-footer');

    // Issues link
    const issuesLink = footer.locator('a[href*="/issues"]');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveText('Issues');

    // Pull requests link
    const prsLink = footer.locator('a[href*="/pulls"]');
    await expect(prsLink).toBeVisible();
    await expect(prsLink).toHaveText('Pull Requests');
  });

  test('footer contains documentation links', async ({ page }) => {
    const footer = page.locator('footer.site-footer');

    // API Docs link
    const apiLink = footer.locator('a:has-text("API Docs")');
    await expect(apiLink).toBeVisible();

    // Configuration link
    const configLink = footer.locator('a:has-text("Configuration")');
    await expect(configLink).toBeVisible();

    // Wiki link
    const wikiLink = footer.locator('a:has-text("Wiki")');
    await expect(wikiLink).toBeVisible();
  });

  test('contributors section has descriptive intro text', async ({ page }) => {
    const contributorsSection = page.locator('section#contributors');
    const intro = contributorsSection.locator('.section-intro');
    await expect(intro).toBeVisible();
    await expect(intro).toContainText('contributions');
  });
});

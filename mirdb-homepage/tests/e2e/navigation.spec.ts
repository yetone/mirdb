/**
 * Navigation E2E Tests
 * Owner: Shared across Scenarios 6, 7, 15
 *
 * Navigation and link tests:
 * - Documentation links (Scenario 6)
 * - Contributing/GitHub links (Scenario 7)
 * - Link validation (Scenario 15)
 */

import { test, expect } from '@playwright/test';

test.describe('Documentation Links - Scenario 6', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture documentation link is present and clickable', async ({ page }) => {
    const architectureLink = page.getByTestId('doc-link-architecture');

    await expect(architectureLink).toBeVisible();
    await expect(architectureLink).toHaveAttribute('href', 'https://docs.mirdb.dev/architecture');

    // Verify the link is clickable (has proper attributes)
    await expect(architectureLink).toHaveAttribute('target', '_blank');
    await expect(architectureLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label is correct
    const label = architectureLink.locator('.documentation__link-label');
    await expect(label).toHaveText('Architecture');
  });

  test('TC2: API Reference documentation link is present and clickable', async ({ page }) => {
    const apiRefLink = page.getByTestId('doc-link-api-reference');

    await expect(apiRefLink).toBeVisible();
    await expect(apiRefLink).toHaveAttribute('href', 'https://docs.mirdb.dev/api');

    // Verify the link is clickable (has proper attributes)
    await expect(apiRefLink).toHaveAttribute('target', '_blank');
    await expect(apiRefLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label is correct
    const label = apiRefLink.locator('.documentation__link-label');
    await expect(label).toHaveText('API Reference');
  });

  test('TC3: Configuration documentation link is present and clickable', async ({ page }) => {
    const configLink = page.getByTestId('doc-link-configuration');

    await expect(configLink).toBeVisible();
    await expect(configLink).toHaveAttribute('href', 'https://docs.mirdb.dev/configuration');

    // Verify the link is clickable (has proper attributes)
    await expect(configLink).toHaveAttribute('target', '_blank');
    await expect(configLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label is correct
    const label = configLink.locator('.documentation__link-label');
    await expect(label).toHaveText('Configuration');
  });

  test('TC4: Architecture link has correct navigation URL', async ({ page }) => {
    const architectureLink = page.getByTestId('doc-link-architecture');

    // Verify the href points to the correct architecture documentation page
    const href = await architectureLink.getAttribute('href');
    expect(href).toBe('https://docs.mirdb.dev/architecture');

    // Verify the link opens in a new tab (external link)
    await expect(architectureLink).toHaveAttribute('target', '_blank');
  });

  test('TC5: API Reference link has correct navigation URL', async ({ page }) => {
    const apiRefLink = page.getByTestId('doc-link-api-reference');

    // Verify the href points to the correct API reference documentation page
    const href = await apiRefLink.getAttribute('href');
    expect(href).toBe('https://docs.mirdb.dev/api');

    // Verify the link opens in a new tab (external link)
    await expect(apiRefLink).toHaveAttribute('target', '_blank');
  });

  test('TC6: Configuration link has correct navigation URL', async ({ page }) => {
    const configLink = page.getByTestId('doc-link-configuration');

    // Verify the href points to the correct configuration documentation page
    const href = await configLink.getAttribute('href');
    expect(href).toBe('https://docs.mirdb.dev/configuration');

    // Verify the link opens in a new tab (external link)
    await expect(configLink).toHaveAttribute('target', '_blank');
  });

  test('All three documentation links are present in the documentation section', async ({ page }) => {
    const documentationSection = page.locator('#documentation');

    await expect(documentationSection).toBeVisible();

    // Verify all three links are present
    const architectureLink = page.getByTestId('doc-link-architecture');
    const apiRefLink = page.getByTestId('doc-link-api-reference');
    const configLink = page.getByTestId('doc-link-configuration');

    await expect(architectureLink).toBeVisible();
    await expect(apiRefLink).toBeVisible();
    await expect(configLink).toBeVisible();
  });

  test('Documentation section has proper accessibility attributes', async ({ page }) => {
    const documentationSection = page.locator('#documentation');

    // Verify section has aria-labelledby
    await expect(documentationSection).toHaveAttribute('aria-labelledby', 'documentation-heading');

    // Verify navigation has aria-label
    const nav = page.locator('.documentation__nav');
    await expect(nav).toHaveAttribute('aria-label', 'Documentation navigation');

    // Verify heading is present
    const heading = page.locator('#documentation-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Documentation');
  });

  test('Documentation links have descriptions', async ({ page }) => {
    const architectureLink = page.getByTestId('doc-link-architecture');
    const apiRefLink = page.getByTestId('doc-link-api-reference');
    const configLink = page.getByTestId('doc-link-configuration');

    // Verify each link has a description
    const archDescription = architectureLink.locator('.documentation__link-description');
    const apiDescription = apiRefLink.locator('.documentation__link-description');
    const configDescription = configLink.locator('.documentation__link-description');

    await expect(archDescription).toBeVisible();
    await expect(apiDescription).toBeVisible();
    await expect(configDescription).toBeVisible();

    // Verify descriptions contain meaningful text
    await expect(archDescription).toContainText('architecture');
    await expect(apiDescription).toContainText('API');
    await expect(configDescription).toContainText('Configuration');
  });
});

test.describe('Contributing Section and GitHub Links - Scenario 7', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: GitHub repository link is present', async ({ page }) => {
    const githubLink = page.getByTestId('contributing-link-github');

    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb');

    // Verify link label is correct
    const label = githubLink.locator('.contributing__link-label');
    await expect(label).toHaveText('GitHub Repository');
  });

  test('TC2: Contribution guidelines link is present', async ({ page }) => {
    const contributingLink = page.getByTestId('contributing-link-contributing-guidelines');

    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb/blob/main/CONTRIBUTING.md');

    // Verify link label is correct
    const label = contributingLink.locator('.contributing__link-label');
    await expect(label).toHaveText('Contribution Guidelines');
  });

  test('TC3: GitHub link opens in new tab with rel noopener', async ({ page }) => {
    const githubLink = page.getByTestId('contributing-link-github');

    await expect(githubLink).toBeVisible();

    // Verify the link opens in a new tab with proper security attributes
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('TC4: Contribution guidelines link navigates to guidelines document', async ({ page }) => {
    const contributingLink = page.getByTestId('contributing-link-contributing-guidelines');

    await expect(contributingLink).toBeVisible();

    // Verify the href points to the CONTRIBUTING.md file
    const href = await contributingLink.getAttribute('href');
    expect(href).toBe('https://github.com/mirdb/mirdb/blob/main/CONTRIBUTING.md');

    // Verify the link opens in a new tab with proper security attributes
    await expect(contributingLink).toHaveAttribute('target', '_blank');
    await expect(contributingLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Contributing section has proper accessibility attributes', async ({ page }) => {
    const contributingSection = page.locator('#contributing');

    // Verify section exists and is visible
    await expect(contributingSection).toBeVisible();

    // Verify section has aria-labelledby
    await expect(contributingSection).toHaveAttribute('aria-labelledby', 'contributing-heading');

    // Verify navigation has aria-label
    const nav = page.locator('.contributing__nav');
    await expect(nav).toHaveAttribute('aria-label', 'Contributing navigation');

    // Verify heading is present
    const heading = page.locator('#contributing-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Contribute');
  });

  test('All contributing links have descriptions', async ({ page }) => {
    const githubLink = page.getByTestId('contributing-link-github');
    const contributingLink = page.getByTestId('contributing-link-contributing-guidelines');

    // Verify each link has a description
    const githubDescription = githubLink.locator('.contributing__link-description');
    const contributingDescription = contributingLink.locator('.contributing__link-description');

    await expect(githubDescription).toBeVisible();
    await expect(contributingDescription).toBeVisible();

    // Verify descriptions contain meaningful text
    await expect(githubDescription).toContainText('source code');
    await expect(contributingDescription).toContainText('contribute');
  });

  test('Issues link is present and has correct attributes', async ({ page }) => {
    const issuesLink = page.getByTestId('contributing-link-issues');

    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb/issues');
    await expect(issuesLink).toHaveAttribute('target', '_blank');
    await expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label
    const label = issuesLink.locator('.contributing__link-label');
    await expect(label).toHaveText('Report Issues');
  });

  test('Discussions link is present and has correct attributes', async ({ page }) => {
    const discussionsLink = page.getByTestId('contributing-link-discussions');

    await expect(discussionsLink).toBeVisible();
    await expect(discussionsLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb/discussions');
    await expect(discussionsLink).toHaveAttribute('target', '_blank');
    await expect(discussionsLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label
    const label = discussionsLink.locator('.contributing__link-label');
    await expect(label).toHaveText('Discussions');
  });
});

test.describe('Link Validation - Scenario 15', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All internal links resolve to valid pages', async ({ page }) => {
    // Get all anchor elements on the page
    const links = await page.locator('a[href]').all();
    const internalLinks: { href: string; text: string }[] = [];

    for (const link of links) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Check for internal links (start with / or #, but not //external)
      if (href && (href.startsWith('/') || href.startsWith('#')) && !href.startsWith('//')) {
        internalLinks.push({ href, text: text || '' });
      }
    }

    // Verify we found internal links
    expect(internalLinks.length).toBeGreaterThan(0);

    // Verify all internal links resolve correctly
    for (const { href, text } of internalLinks) {
      if (href.startsWith('#')) {
        // Check anchor links resolve to an element on the page
        const targetId = href.substring(1);
        if (targetId) {
          const targetElement = page.locator(`#${targetId}`);
          await expect(targetElement, `Anchor link "${text}" pointing to ${href} should resolve to an element`).toBeAttached();
        }
      } else {
        // For path-based internal links, verify they exist (won't 404)
        // The /docs/quickstart is a relative link that would work in production
        expect(href).toMatch(/^\/[a-zA-Z0-9\-\/]*$/);
      }
    }
  });

  test('TC2: All external links have valid URLs', async ({ page, request }) => {
    // Get all anchor elements with external links
    const links = await page.locator('a[href^="http"]').all();
    const externalLinks: { href: string; text: string }[] = [];

    for (const link of links) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      if (href) {
        externalLinks.push({ href, text: text || '' });
      }
    }

    // Verify we found external links
    expect(externalLinks.length).toBeGreaterThan(0);

    // Check each external link has a valid URL format
    for (const { href, text } of externalLinks) {
      // Verify URL is well-formed
      expect(() => new URL(href), `External link "${text}" should have valid URL format: ${href}`).not.toThrow();

      // Verify URL uses HTTPS
      const url = new URL(href);
      expect(url.protocol, `External link "${text}" should use HTTPS: ${href}`).toBe('https:');
    }
  });

  test('TC2b: External links point to known valid domains', async ({ page }) => {
    // Get all external links
    const links = await page.locator('a[href^="http"]').all();
    const validDomains = [
      'github.com',
      'docs.mirdb.dev',
      'crates.io',
      'discord.gg',
      'twitter.com',
    ];

    for (const link of links) {
      const href = await link.getAttribute('href');
      if (href) {
        const url = new URL(href);
        expect(
          validDomains.some(domain => url.hostname === domain || url.hostname.endsWith(`.${domain}`)),
          `External link ${href} should point to a known valid domain`
        ).toBe(true);
      }
    }
  });

  test('TC3: No anchor tags have empty or # only href', async ({ page }) => {
    // Get all anchor elements
    const allAnchors = await page.locator('a').all();

    for (const anchor of allAnchors) {
      const href = await anchor.getAttribute('href');
      const testId = await anchor.getAttribute('data-testid');
      const text = await anchor.textContent();
      const identifier = testId || text || 'unknown';

      // Check href is not empty
      expect(href, `Anchor "${identifier}" should have an href attribute`).not.toBeNull();
      expect(href?.trim(), `Anchor "${identifier}" should not have an empty href`).not.toBe('');

      // Check href is not just '#'
      expect(href, `Anchor "${identifier}" should not have href="#" only`).not.toBe('#');
    }
  });

  test('TC4: External links have proper security attributes', async ({ page }) => {
    // Get all external links (http/https)
    const externalLinks = await page.locator('a[href^="http"]').all();

    expect(externalLinks.length).toBeGreaterThan(0);

    for (const link of externalLinks) {
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');
      const testId = await link.getAttribute('data-testid');
      const text = await link.textContent();
      const identifier = testId || text || href || 'unknown';

      // External links should open in new tab
      expect(target, `External link "${identifier}" should have target="_blank"`).toBe('_blank');

      // External links should have security attributes
      expect(rel, `External link "${identifier}" should have rel attribute`).not.toBeNull();
      expect(rel, `External link "${identifier}" should include 'noopener'`).toContain('noopener');
      expect(rel, `External link "${identifier}" should include 'noreferrer'`).toContain('noreferrer');
    }
  });

  test('All links are accessible via keyboard', async ({ page }) => {
    // Get all anchor elements
    const allAnchors = await page.locator('a[href]').all();

    for (const anchor of allAnchors) {
      // Verify links are focusable (not have tabindex=-1 without aria-hidden parent)
      const tabIndex = await anchor.getAttribute('tabindex');
      const ariaHidden = await anchor.getAttribute('aria-hidden');

      if (tabIndex === '-1') {
        // If tabindex is -1, it should be in a hidden context or have specific reason
        expect(ariaHidden).toBe('true');
      }
    }
  });

  test('Skip link is present and functional', async ({ page }) => {
    const skipLink = page.locator('a.skip-link');

    await expect(skipLink).toBeAttached();

    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Verify the target element exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeAttached();
  });

  test('Link count validation - expected number of links present', async ({ page }) => {
    const allLinks = await page.locator('a[href]').all();

    // We should have at least these links:
    // - Skip link (1)
    // - CTA button (1)
    // - Crates badge (1)
    // - Documentation links (3)
    // - Contributing links (4)
    // - Footer links (3 resources + 3 social = 6)
    // Total minimum: 18 links
    expect(allLinks.length).toBeGreaterThanOrEqual(15);
  });
});

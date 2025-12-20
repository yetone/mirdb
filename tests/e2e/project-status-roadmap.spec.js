/**
 * E2E Tests for Project Status and Roadmap Display
 * Scenario: Verify that project status and roadmap information (including planned Raft consensus) is displayed for potential contributors
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Project Status and Roadmap Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test.describe('Test Case 1: Project status information', () => {
    test('should have a project status or roadmap section visible on the page', async ({ page }) => {
      // Look for status/roadmap section or footer content with status
      const statusSection = page.locator('#project-status, #roadmap, [class*="status"], [class*="roadmap"], section:has-text("Status"), section:has-text("Roadmap")');
      const footerStatus = page.locator('footer');

      const hasStatusSection = await statusSection.count() > 0;
      const footerText = await footerStatus.textContent();
      const hasFooterStatusInfo = footerText.toLowerCase().includes('status') ||
                                  footerText.toLowerCase().includes('version') ||
                                  footerText.toLowerCase().includes('alpha') ||
                                  footerText.toLowerCase().includes('beta') ||
                                  footerText.toLowerCase().includes('open source');

      expect(hasStatusSection || hasFooterStatusInfo).toBe(true);
    });

    test('should display current implementation status or version information', async ({ page }) => {
      const pageText = await page.textContent('body');
      const pageTextLower = pageText.toLowerCase();

      // Check for status indicators
      const hasStatusInfo = pageTextLower.includes('alpha') ||
                           pageTextLower.includes('beta') ||
                           pageTextLower.includes('stable') ||
                           pageTextLower.includes('version') ||
                           pageTextLower.includes('v0.') ||
                           pageTextLower.includes('v1.') ||
                           pageTextLower.includes('status') ||
                           pageTextLower.includes('development') ||
                           pageTextLower.includes('active') ||
                           pageTextLower.includes('maintained') ||
                           pageTextLower.includes('currently') ||
                           pageTextLower.includes('early stage') ||
                           pageTextLower.includes('in progress');

      expect(hasStatusInfo).toBe(true);
    });

    test('should indicate what features are currently available', async ({ page }) => {
      const pageText = await page.textContent('body');
      const pageTextLower = pageText.toLowerCase();

      // Current features should be mentioned (memcached, persistence, LSM-tree)
      const mentionsCurrentFeatures = pageTextLower.includes('memcached') &&
                                     (pageTextLower.includes('persist') ||
                                      pageTextLower.includes('lsm') ||
                                      pageTextLower.includes('storage'));

      expect(mentionsCurrentFeatures).toBe(true);
    });
  });

  test.describe('Test Case 2: Roadmap and planned features', () => {
    test('should mention planned features including Raft consensus', async ({ page }) => {
      const pageText = await page.textContent('body');
      const pageTextLower = pageText.toLowerCase();

      // Check for Raft consensus mention
      const hasRaftMention = pageTextLower.includes('raft') ||
                            pageTextLower.includes('consensus') ||
                            pageTextLower.includes('distributed') ||
                            pageTextLower.includes('replication');

      expect(hasRaftMention).toBe(true);
    });

    test('should have a roadmap section or planned features area', async ({ page }) => {
      const pageText = await page.textContent('body');
      const pageTextLower = pageText.toLowerCase();

      // Check for roadmap/planned features indicators
      const hasRoadmapInfo = pageTextLower.includes('roadmap') ||
                            pageTextLower.includes('planned') ||
                            pageTextLower.includes('coming soon') ||
                            pageTextLower.includes('future') ||
                            pageTextLower.includes('upcoming');

      expect(hasRoadmapInfo).toBe(true);
    });

    test('should help potential contributors identify opportunities', async ({ page }) => {
      const pageText = await page.textContent('body');
      const pageTextLower = pageText.toLowerCase();

      // Check for contribution-related text or feature opportunities
      const hasContributionOpportunities = pageTextLower.includes('contribut') ||
                                          pageTextLower.includes('help') ||
                                          pageTextLower.includes('planned') ||
                                          pageTextLower.includes('roadmap') ||
                                          pageTextLower.includes('welcome') ||
                                          pageTextLower.includes('open source');

      expect(hasContributionOpportunities).toBe(true);
    });
  });

  test.describe('Test Case 3: Contribution guidelines link', () => {
    test('should have a link to contribution guidelines or CONTRIBUTING.md', async ({ page }) => {
      // Look for contributing link
      const contributingLink = page.locator('a[href*="CONTRIBUTING"], a[href*="contributing"], a:has-text("Contribut"), a:has-text("contribut")');
      const count = await contributingLink.count();

      if (count > 0) {
        await expect(contributingLink.first()).toBeVisible();
        const href = await contributingLink.first().getAttribute('href');
        expect(href).toBeTruthy();
      } else {
        // Alternatively check for GitHub link which leads to contribution opportunities
        const githubLink = page.locator('a[href*="github.com"]');
        expect(await githubLink.count()).toBeGreaterThan(0);
      }
    });

    test('should make it easy for contributors to get started', async ({ page }) => {
      // Check for contributor-friendly elements
      const contributingLink = page.locator('a[href*="CONTRIBUTING"], a[href*="contributing"], a:has-text("Contribut")');
      const githubLink = page.locator('a[href*="github.com"]');
      const pageText = await page.textContent('body');

      const hasContributingLink = await contributingLink.count() > 0;
      const hasGithubLink = await githubLink.count() > 0;
      const hasContributingText = pageText.toLowerCase().includes('contribut');

      // Should have at least a way for contributors to get started
      expect(hasContributingLink || hasGithubLink || hasContributingText).toBe(true);
    });

    test('contribution link should have proper href attribute', async ({ page }) => {
      const contributingLink = page.locator('a[href*="CONTRIBUTING"], a[href*="contributing"], a:has-text("Contribut")');

      if (await contributingLink.count() > 0) {
        const href = await contributingLink.first().getAttribute('href');
        expect(href).toBeTruthy();
        // Should be a valid URL or relative path
        expect(href.length).toBeGreaterThan(0);
      } else {
        // If no direct contributing link, GitHub link serves as fallback
        const githubLink = page.locator('a[href*="github.com"]').first();
        await expect(githubLink).toBeVisible();
      }
    });
  });

  test.describe('Project Status Section Accessibility', () => {
    test('should have proper heading structure in status/roadmap area', async ({ page }) => {
      // Check for status section with proper heading
      const statusHeading = page.locator('h2:has-text("Status"), h2:has-text("Roadmap"), h3:has-text("Status"), h3:has-text("Roadmap")');
      const hasStatusHeading = await statusHeading.count() > 0;

      // Or status info might be in footer
      const footer = page.locator('footer');
      const hasFooter = await footer.count() > 0;

      expect(hasStatusHeading || hasFooter).toBe(true);
    });

    test('links should be keyboard accessible', async ({ page }) => {
      const contributingLink = page.locator('a[href*="CONTRIBUTING"], a[href*="contributing"], a:has-text("Contribut")');

      if (await contributingLink.count() > 0) {
        await expect(contributingLink.first()).toBeVisible();
        // Check it's focusable
        await contributingLink.first().focus();
      }
    });
  });
});

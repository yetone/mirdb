/**
 * E2E Tests for Error Handling - Broken Links
 * Scenario: Verify that all internal and external links on the page are valid and functional
 *
 * Test Case 1: Extract all href attributes and filter internal links (E2E navigation)
 * Test Case 2: Check external links for valid HTTP responses
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Get the absolute path to index.html
const htmlPath = path.resolve(__dirname, '../../index.html');
const fileUrl = `file://${htmlPath}`;

test.describe('Error Handling - Broken Links (E2E Tests)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(fileUrl);
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Extract all href attributes and filter internal links
   * Expected: All internal links (#anchors) navigate to existing sections
   */
  test.describe('Test Case 1: Internal Link Navigation (E2E)', () => {
    test('should have all internal anchor links navigate to existing sections', async ({ page }) => {
      // Get all internal anchor links
      const internalLinks = await page.$$eval('a[href^="#"]', links =>
        links.map(link => ({
          href: link.getAttribute('href'),
          text: link.textContent.trim()
        }))
      );

      const brokenLinks = [];

      for (const link of internalLinks) {
        // Skip empty hash (scroll to top)
        if (link.href === '#') continue;

        const targetId = link.href.replace('#', '');
        const targetExists = await page.$(`#${targetId}`);

        if (!targetExists) {
          brokenLinks.push(link);
        }
      }

      expect(brokenLinks).toHaveLength(0);
    });

    test('should navigate smoothly when clicking internal anchor links', async ({ page }) => {
      // Get navigation links
      const navLinks = await page.$$('nav a[href^="#"]');

      for (const navLink of navLinks) {
        const href = await navLink.getAttribute('href');
        if (!href || href === '#') continue;

        const targetId = href.replace('#', '');

        // Click the link
        await navLink.click();

        // Verify the target section is visible (in viewport or scrolled to)
        const targetElement = page.locator(`#${targetId}`);
        await expect(targetElement).toBeVisible();
      }
    });

    test('should have Features link navigate to features section', async ({ page }) => {
      const featuresLink = page.locator('nav a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      await featuresLink.click();

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });

    test('should have Quick Start link navigate to quick-start section', async ({ page }) => {
      const quickStartLink = page.locator('nav a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();

      await quickStartLink.click();

      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();
    });

    test('should have Configuration link navigate to configuration section', async ({ page }) => {
      const configLink = page.locator('nav a[href="#configuration"]');
      await expect(configLink).toBeVisible();

      await configLink.click();

      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();
    });

    test('should have Get Started CTA navigate to quick-start section', async ({ page }) => {
      const ctaButton = page.locator('.hero-cta a[href="#quick-start"]');
      await expect(ctaButton).toBeVisible();

      await ctaButton.click();

      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();
    });
  });

  /**
   * Test Case 2: Check external links for valid HTTP responses
   * Expected: All external links return HTTP 200 or appropriate redirect
   */
  test.describe('Test Case 2: External Link HTTP Validation', () => {
    test('should collect all external links from the page', async ({ page }) => {
      const externalLinks = await page.$$eval('a[href^="https://"]', links =>
        links.map(link => ({
          href: link.getAttribute('href'),
          text: link.textContent.trim()
        }))
      );

      // Should have external links
      expect(externalLinks.length).toBeGreaterThan(0);

      // Log for visibility
      console.log('External links found:', externalLinks);
    });

    test('should verify GitHub repository link is valid', async ({ request }) => {
      // Test the main GitHub repository link
      const response = await request.get('https://github.com/yetone/mirdb', {
        timeout: 10000,
        ignoreHTTPSErrors: true
      });

      // Should return 200 or redirect (301, 302)
      const validStatuses = [200, 301, 302, 304];
      expect(validStatuses).toContain(response.status());
    });

    test('should verify GitHub documentation link is valid', async ({ request }) => {
      const response = await request.get('https://github.com/yetone/mirdb/blob/master/README.md', {
        timeout: 10000,
        ignoreHTTPSErrors: true
      });

      // Include 429 (rate limiting) as acceptable - doesn't indicate broken link
      const validStatuses = [200, 301, 302, 304, 429];
      expect(validStatuses).toContain(response.status());
    });

    test('should verify GitHub license link is valid', async ({ request }) => {
      // License link points to main repo since no explicit LICENSE file exists
      const response = await request.get('https://github.com/yetone/mirdb', {
        timeout: 10000,
        ignoreHTTPSErrors: true
      });

      const validStatuses = [200, 301, 302, 304];
      expect(validStatuses).toContain(response.status());
    });

    test('should verify rustup.rs link is valid', async ({ request }) => {
      const response = await request.get('https://rustup.rs/', {
        timeout: 10000,
        ignoreHTTPSErrors: true
      });

      const validStatuses = [200, 301, 302, 304];
      expect(validStatuses).toContain(response.status());
    });

    test('should verify all external links return valid HTTP responses', async ({ page, request }) => {
      // Get all unique external HTTPS links
      const externalLinks = await page.$$eval('a[href^="https://"]', links => {
        const uniqueHrefs = new Set();
        links.forEach(link => {
          const href = link.getAttribute('href');
          if (href) uniqueHrefs.add(href);
        });
        return Array.from(uniqueHrefs);
      });

      const failedLinks = [];
      // Include 429 (rate limiting) as acceptable - doesn't indicate broken link
      const validStatuses = [200, 301, 302, 304, 429];

      for (const href of externalLinks) {
        try {
          const response = await request.get(href, {
            timeout: 15000,
            ignoreHTTPSErrors: true
          });

          if (!validStatuses.includes(response.status())) {
            failedLinks.push({
              href: href,
              status: response.status()
            });
          }
        } catch (error) {
          // Network errors or timeouts
          failedLinks.push({
            href: href,
            error: error.message
          });
        }
      }

      if (failedLinks.length > 0) {
        console.error('Failed external links:', failedLinks);
      }

      expect(failedLinks).toHaveLength(0);
    });
  });

  /**
   * Additional E2E Link Validations
   */
  test.describe('Link Accessibility and Functionality', () => {
    test('should have all links clickable', async ({ page }) => {
      const links = await page.$$('a[href]');

      for (const link of links) {
        const isVisible = await link.isVisible();
        const isEnabled = await link.isEnabled();

        // Links should be visible and enabled
        if (isVisible) {
          expect(isEnabled).toBe(true);
        }
      }
    });

    test('should open external links in new tab', async ({ page }) => {
      const externalLinks = await page.$$('a[href^="https://"]');

      for (const link of externalLinks) {
        const target = await link.getAttribute('target');
        expect(target).toBe('_blank');
      }
    });

    test('should have proper rel attributes for security on external links', async ({ page }) => {
      const externalLinks = await page.$$('a[href^="https://"][target="_blank"]');

      for (const link of externalLinks) {
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });

    test('should have keyboard accessible links', async ({ page }) => {
      // Focus on the page and tab through links
      await page.keyboard.press('Tab');

      // Should be able to focus on links
      const focusedElement = await page.evaluate(() => document.activeElement?.tagName);
      expect(['A', 'BUTTON', 'INPUT']).toContain(focusedElement);
    });

    test('should have GitHub CTA button functional', async ({ page, context }) => {
      const githubCta = page.locator('.hero-cta a[href*="github.com"]');
      await expect(githubCta).toBeVisible();

      // Verify the link attributes
      const href = await githubCta.getAttribute('href');
      const target = await githubCta.getAttribute('target');

      expect(href).toContain('github.com');
      expect(target).toBe('_blank');
    });
  });

  /**
   * Comprehensive Link Health Check
   */
  test.describe('Link Health Summary', () => {
    test('should generate comprehensive link report', async ({ page }) => {
      const linkReport = await page.evaluate(() => {
        const allLinks = document.querySelectorAll('a[href]');
        const report = {
          totalLinks: allLinks.length,
          internalLinks: [],
          externalLinks: [],
          brokenInternalLinks: [],
          malformedLinks: []
        };

        allLinks.forEach(link => {
          const href = link.getAttribute('href');

          if (href.startsWith('#')) {
            report.internalLinks.push(href);

            // Check if target exists
            if (href !== '#') {
              const targetId = href.replace('#', '');
              const target = document.getElementById(targetId);
              if (!target) {
                report.brokenInternalLinks.push(href);
              }
            }
          } else if (href.startsWith('http')) {
            report.externalLinks.push(href);
          } else if (href === '' || href.startsWith('javascript:')) {
            report.malformedLinks.push(href);
          }
        });

        return report;
      });

      console.log('Link Health Report:', JSON.stringify(linkReport, null, 2));

      // Assertions
      expect(linkReport.brokenInternalLinks).toHaveLength(0);
      expect(linkReport.malformedLinks).toHaveLength(0);
      expect(linkReport.totalLinks).toBeGreaterThan(0);
    });
  });
});

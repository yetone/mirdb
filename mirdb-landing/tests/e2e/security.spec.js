/**
 * Security E2E Tests
 * Owner: Scenario 19 - Security Headers and Links
 *
 * Tests:
 * - No mixed content warnings (all resources HTTPS)
 * - External link security attributes (noopener noreferrer)
 * - No inline event handlers in the rendered page
 */

import { test, expect } from '@playwright/test';

test.describe('Security - External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All external links with target="_blank" have rel="noopener noreferrer"', async ({ page }) => {
    // Find all links with target="_blank"
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    // Verify at least one external link exists
    expect(count).toBeGreaterThan(0);

    const violations = [];

    // Check each external link has rel="noopener noreferrer"
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      const href = await link.getAttribute('href');

      if (!rel) {
        violations.push(`Link to ${href}: Missing rel attribute`);
      } else {
        if (!rel.includes('noopener')) {
          violations.push(`Link to ${href}: Missing 'noopener' in rel attribute`);
        }
        if (!rel.includes('noreferrer')) {
          violations.push(`Link to ${href}: Missing 'noreferrer' in rel attribute`);
        }
      }
    }

    expect(violations).toEqual([]);
  });
});

test.describe('Security - Mixed Content', () => {
  test('TC2: No mixed content - all resources loaded over HTTPS', async ({ page }) => {
    const mixedContentWarnings = [];

    // Listen for console warnings about mixed content
    page.on('console', (msg) => {
      const text = msg.text();
      if (text.toLowerCase().includes('mixed content') ||
          text.toLowerCase().includes('insecure content')) {
        mixedContentWarnings.push(text);
      }
    });

    // Listen for request failures due to mixed content
    page.on('requestfailed', (request) => {
      const failure = request.failure();
      if (failure && failure.errorText.toLowerCase().includes('mixed')) {
        mixedContentWarnings.push(`Request failed: ${request.url()} - ${failure.errorText}`);
      }
    });

    // Navigate to the page
    await page.goto('/');

    // Wait for all resources to load
    await page.waitForLoadState('networkidle');

    // Check for any HTTP resources in the page
    const httpResources = await page.evaluate(() => {
      const resources = [];

      // Check all link elements (CSS)
      document.querySelectorAll('link[href^="http://"]').forEach(el => {
        resources.push(`Link: ${el.href}`);
      });

      // Check all script elements
      document.querySelectorAll('script[src^="http://"]').forEach(el => {
        resources.push(`Script: ${el.src}`);
      });

      // Check all images
      document.querySelectorAll('img[src^="http://"]').forEach(el => {
        resources.push(`Image: ${el.src}`);
      });

      // Check all iframes
      document.querySelectorAll('iframe[src^="http://"]').forEach(el => {
        resources.push(`Iframe: ${el.src}`);
      });

      // Check all video/audio sources
      document.querySelectorAll('video source[src^="http://"], audio source[src^="http://"]').forEach(el => {
        resources.push(`Media: ${el.src}`);
      });

      // Check inline styles for http:// URLs
      const allElements = document.querySelectorAll('*');
      allElements.forEach(el => {
        const style = el.getAttribute('style');
        if (style && style.includes('http://')) {
          resources.push(`Inline style with HTTP URL on ${el.tagName}`);
        }
      });

      return resources;
    });

    // Combine all issues
    const allIssues = [...mixedContentWarnings, ...httpResources];

    expect(allIssues).toEqual([]);
  });

  test('TC2b: All external resources use HTTPS', async ({ page }) => {
    await page.goto('/');

    // Check all external links point to HTTPS
    const externalLinks = await page.evaluate(() => {
      const links = document.querySelectorAll('a[href^="http://"]');
      return Array.from(links).map(link => link.href);
    });

    // No HTTP links should exist (all should be HTTPS)
    expect(externalLinks.length).toBe(0);
  });
});

test.describe('Security - No Inline Event Handlers', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC3: No inline JavaScript event handlers on any elements', async ({ page }) => {
    // List of inline event handlers to check
    const inlineEventHandlers = [
      'onclick',
      'ondblclick',
      'onmousedown',
      'onmouseup',
      'onmouseover',
      'onmouseout',
      'onmousemove',
      'onmouseenter',
      'onmouseleave',
      'onkeydown',
      'onkeyup',
      'onkeypress',
      'onfocus',
      'onblur',
      'onchange',
      'oninput',
      'onsubmit',
      'onreset',
      'onload',
      'onerror',
      'onscroll',
      'onresize',
      'ontouchstart',
      'ontouchend',
      'ontouchmove'
    ];

    const violations = await page.evaluate((handlers) => {
      const issues = [];

      handlers.forEach(handler => {
        const elements = document.querySelectorAll(`[${handler}]`);

        elements.forEach(el => {
          const tagName = el.tagName.toLowerCase();
          const id = el.id ? `#${el.id}` : '';
          const className = el.className && typeof el.className === 'string'
            ? `.${el.className.replace(/\s+/g, '.')}`
            : '';

          issues.push(`Element <${tagName}${id}${className}> has inline ${handler} handler`);
        });
      });

      return issues;
    }, inlineEventHandlers);

    expect(violations).toEqual([]);
  });

  test('TC3b: No javascript: protocol in href attributes', async ({ page }) => {
    const javascriptLinks = await page.evaluate(() => {
      const links = document.querySelectorAll('a[href^="javascript:"]');
      return Array.from(links).map(link => link.outerHTML);
    });

    expect(javascriptLinks).toEqual([]);
  });
});

test.describe('Security - Additional Checks', () => {
  test('External links open in new tab safely', async ({ page }) => {
    await page.goto('/');

    // Find all external links that open in new tabs
    const externalLinks = page.locator('a[target="_blank"][href^="https://"]');
    const count = await externalLinks.count();

    // Verify we have external links
    expect(count).toBeGreaterThan(0);

    // Verify each one has proper security
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('rel', /noopener/);
      await expect(link).toHaveAttribute('rel', /noreferrer/);
    }
  });

  test('CircleCI badge uses HTTPS', async ({ page }) => {
    await page.goto('/');

    // Check for CircleCI badge
    const circleCIBadge = page.locator('img[src*="circleci.com"]');
    const count = await circleCIBadge.count();

    if (count > 0) {
      const src = await circleCIBadge.first().getAttribute('src');
      expect(src).toMatch(/^https:\/\//);
    }
  });
});

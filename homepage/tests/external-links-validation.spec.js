// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('External Links Validation', () => {
  // Store all external links extracted from the page
  let externalLinks = [];

  test.beforeEach(async ({ page }) => {
    await page.goto('/');

    // Extract all external links from the page
    externalLinks = await page.evaluate(() => {
      const anchors = document.querySelectorAll('a[href^="http"]');
      return Array.from(anchors).map(anchor => ({
        href: anchor.getAttribute('href'),
        text: anchor.textContent?.trim() || '',
        target: anchor.getAttribute('target'),
        rel: anchor.getAttribute('rel'),
      }));
    });
  });

  test('TC1: All external links return HTTP 200 or redirect to valid page', async ({ page, request }) => {
    // Test Case 1: Extract all external links and verify they return 200 status
    // Expected: All external links return HTTP 200 or redirect to valid page

    expect(externalLinks.length).toBeGreaterThan(0);

    const linkResults = [];

    for (const link of externalLinks) {
      if (link.href) {
        try {
          // Use fetch with redirect following to check link validity
          const response = await request.get(link.href, {
            timeout: 30000,
            maxRedirects: 5,
          });

          // Accept 200, 201, or redirect status codes (301, 302, 307, 308)
          // After following redirects, we should get a successful response
          const isValid = response.status() >= 200 && response.status() < 400;

          linkResults.push({
            url: link.href,
            status: response.status(),
            valid: isValid,
            text: link.text,
          });
        } catch (error) {
          linkResults.push({
            url: link.href,
            status: 'error',
            valid: false,
            text: link.text,
            error: error.message,
          });
        }
      }
    }

    // Log all results for debugging
    console.log('External link validation results:', JSON.stringify(linkResults, null, 2));

    // All links should be valid
    const invalidLinks = linkResults.filter(r => !r.valid);
    expect(invalidLinks, `Invalid links found: ${JSON.stringify(invalidLinks)}`).toHaveLength(0);
  });

  test('TC2: GitHub repository link points to MirDB repo', async ({ page, request }) => {
    // Test Case 2: Verify GitHub repository link points to MirDB repo
    // Expected: GitHub link resolves to correct MirDB repository

    // Find GitHub links
    const githubLinks = externalLinks.filter(link =>
      link.href && link.href.includes('github.com')
    );

    expect(githubLinks.length).toBeGreaterThan(0);

    // Find the main GitHub repo link (not a specific file link)
    const mainRepoLink = githubLinks.find(link =>
      link.href && link.href.match(/github\.com\/[^/]+\/mirdb\/?$/)
    );

    expect(mainRepoLink, 'Main MirDB repository link should exist').toBeTruthy();
    expect(mainRepoLink?.href).toContain('mirdb');

    // Verify the GitHub link is accessible
    if (mainRepoLink?.href) {
      const response = await request.get(mainRepoLink.href, {
        timeout: 30000,
        maxRedirects: 5,
      });

      // GitHub should return 200 OK
      expect(response.status()).toBeLessThan(400);

      // Optionally verify the page contains MirDB content
      const body = await response.text();
      expect(body.toLowerCase()).toContain('mirdb');
    }
  });

  test('TC3: Zero broken links (no 404 responses)', async ({ page, request }) => {
    // Test Case 3: Check that no links are broken (404)
    // Expected: Zero broken links on the page

    expect(externalLinks.length).toBeGreaterThan(0);

    const brokenLinks = [];

    for (const link of externalLinks) {
      if (link.href) {
        try {
          const response = await request.get(link.href, {
            timeout: 30000,
            maxRedirects: 5,
          });

          // Check for 404 Not Found or other client errors
          if (response.status() === 404) {
            brokenLinks.push({
              url: link.href,
              status: response.status(),
              text: link.text,
            });
          }
        } catch (error) {
          // Network errors are also considered broken
          brokenLinks.push({
            url: link.href,
            status: 'network_error',
            text: link.text,
            error: error.message,
          });
        }
      }
    }

    // Log broken links for debugging
    if (brokenLinks.length > 0) {
      console.log('Broken links found:', JSON.stringify(brokenLinks, null, 2));
    }

    // There should be zero broken links
    expect(brokenLinks, `Broken links found: ${JSON.stringify(brokenLinks)}`).toHaveLength(0);
  });

  test('TC4: External links have appropriate rel attributes for security', async ({ page }) => {
    // Test Case 4: Verify external links have appropriate rel attributes
    // Expected: External links include rel='noopener noreferrer' for security

    expect(externalLinks.length).toBeGreaterThan(0);

    const linksWithoutSecurityAttrs = [];

    for (const link of externalLinks) {
      // External links opening in new tab should have noopener noreferrer
      if (link.href && link.target === '_blank') {
        const rel = link.rel || '';
        const hasNoopener = rel.includes('noopener');
        const hasNoreferrer = rel.includes('noreferrer');

        if (!hasNoopener || !hasNoreferrer) {
          linksWithoutSecurityAttrs.push({
            url: link.href,
            text: link.text,
            target: link.target,
            rel: link.rel,
            missingNoopener: !hasNoopener,
            missingNoreferrer: !hasNoreferrer,
          });
        }
      }
    }

    // Log results for debugging
    if (linksWithoutSecurityAttrs.length > 0) {
      console.log('Links missing security attributes:', JSON.stringify(linksWithoutSecurityAttrs, null, 2));
    }

    // All external links with target="_blank" should have noopener noreferrer
    expect(
      linksWithoutSecurityAttrs,
      `External links missing rel="noopener noreferrer": ${JSON.stringify(linksWithoutSecurityAttrs)}`
    ).toHaveLength(0);
  });

  test('All external links should use HTTPS', async ({ page }) => {
    // Additional security check: all external links should use HTTPS
    const httpLinks = externalLinks.filter(link =>
      link.href && link.href.startsWith('http://') && !link.href.includes('localhost')
    );

    if (httpLinks.length > 0) {
      console.log('Insecure HTTP links found:', JSON.stringify(httpLinks, null, 2));
    }

    expect(httpLinks, 'All external links should use HTTPS').toHaveLength(0);
  });

  test('External links have meaningful text content', async ({ page }) => {
    // Accessibility check: external links should have meaningful text
    const linksWithoutText = externalLinks.filter(link =>
      link.href && (!link.text || link.text.trim() === '')
    );

    if (linksWithoutText.length > 0) {
      console.log('Links without text:', JSON.stringify(linksWithoutText, null, 2));
    }

    expect(linksWithoutText, 'External links should have meaningful text').toHaveLength(0);
  });
});

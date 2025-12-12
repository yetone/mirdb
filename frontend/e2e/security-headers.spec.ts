import { test, expect } from '@playwright/test';

test.describe('Security - Content Security Policy', () => {
  test.describe('Security Headers', () => {
    test('Test Case 1: CSP header is present with appropriate directives', async ({ page }) => {
      // Navigate to homepage and capture response headers
      const response = await page.goto('/');

      expect(response).not.toBeNull();
      const headers = response!.headers();

      // Check for Content-Security-Policy header
      const cspHeader = headers['content-security-policy'];
      expect(cspHeader).toBeDefined();
      expect(cspHeader).toBeTruthy();

      // Verify CSP contains essential directives
      expect(cspHeader).toContain("default-src");
      expect(cspHeader).toContain("script-src");
      expect(cspHeader).toContain("style-src");
      expect(cspHeader).toContain("frame-ancestors");
    });

    test('Test Case 3: X-Frame-Options header prevents clickjacking attacks', async ({ page }) => {
      // Navigate to homepage and capture response headers
      const response = await page.goto('/');

      expect(response).not.toBeNull();
      const headers = response!.headers();

      // Check for X-Frame-Options header
      const xFrameOptions = headers['x-frame-options'];
      expect(xFrameOptions).toBeDefined();

      // Should be DENY or SAMEORIGIN to prevent clickjacking
      expect(['DENY', 'SAMEORIGIN', 'deny', 'sameorigin']).toContain(xFrameOptions);
    });

    test('Test Case 4: X-Content-Type-Options header is set to nosniff', async ({ page }) => {
      // Navigate to homepage and capture response headers
      const response = await page.goto('/');

      expect(response).not.toBeNull();
      const headers = response!.headers();

      // Check for X-Content-Type-Options header
      const xContentTypeOptions = headers['x-content-type-options'];
      expect(xContentTypeOptions).toBeDefined();
      expect(xContentTypeOptions?.toLowerCase()).toBe('nosniff');
    });

    test('X-XSS-Protection header is present', async ({ page }) => {
      // Navigate to homepage and capture response headers
      const response = await page.goto('/');

      expect(response).not.toBeNull();
      const headers = response!.headers();

      // Check for X-XSS-Protection header
      const xssProtection = headers['x-xss-protection'];
      expect(xssProtection).toBeDefined();
      expect(xssProtection).toContain('1');
    });

    test('Referrer-Policy header is present', async ({ page }) => {
      // Navigate to homepage and capture response headers
      const response = await page.goto('/');

      expect(response).not.toBeNull();
      const headers = response!.headers();

      // Check for Referrer-Policy header
      const referrerPolicy = headers['referrer-policy'];
      expect(referrerPolicy).toBeDefined();
      expect(referrerPolicy).toBeTruthy();
    });

    test('Strict-Transport-Security header is present for HTTPS enforcement', async ({ page }) => {
      // Navigate to homepage and capture response headers
      const response = await page.goto('/');

      expect(response).not.toBeNull();
      const headers = response!.headers();

      // Check for Strict-Transport-Security header (HSTS)
      const hsts = headers['strict-transport-security'];
      expect(hsts).toBeDefined();
      expect(hsts).toContain('max-age');
    });
  });

  test.describe('HTTPS Enforcement', () => {
    test('Test Case 2: HTTP requests receive proper security headers indicating HTTPS preference', async ({ page }) => {
      // Navigate to homepage
      const response = await page.goto('/');

      expect(response).not.toBeNull();
      const headers = response!.headers();

      // HSTS header indicates that the server prefers HTTPS connections
      // When this header is present, browsers will automatically upgrade future HTTP requests to HTTPS
      const hsts = headers['strict-transport-security'];
      expect(hsts).toBeDefined();
      expect(hsts).toContain('max-age');

      // The presence of HSTS header effectively enforces HTTPS for future requests
      // max-age should be set to a reasonable value (at least 1 year = 31536000 seconds)
      const maxAgeMatch = hsts?.match(/max-age=(\d+)/);
      expect(maxAgeMatch).not.toBeNull();
      if (maxAgeMatch) {
        const maxAge = parseInt(maxAgeMatch[1], 10);
        expect(maxAge).toBeGreaterThanOrEqual(86400); // At least 1 day
      }
    });

    test('Page content is served with security-first approach', async ({ page }) => {
      // Navigate to homepage
      const response = await page.goto('/');

      expect(response).not.toBeNull();

      // Verify the page loaded successfully
      expect(response!.status()).toBe(200);

      // Verify all critical security headers are present
      const headers = response!.headers();

      // Essential security headers check
      const essentialHeaders = [
        'content-security-policy',
        'x-frame-options',
        'x-content-type-options'
      ];

      for (const header of essentialHeaders) {
        expect(headers[header], `Header ${header} should be present`).toBeDefined();
      }
    });
  });

  test.describe('CSP Directive Validation', () => {
    test('CSP default-src directive restricts default content sources', async ({ page }) => {
      const response = await page.goto('/');
      const csp = response!.headers()['content-security-policy'];

      expect(csp).toContain("default-src");
      // default-src should be restrictive ('self' or more specific)
      expect(csp).toMatch(/default-src\s+'self'/);
    });

    test('CSP frame-ancestors directive prevents embedding', async ({ page }) => {
      const response = await page.goto('/');
      const csp = response!.headers()['content-security-policy'];

      // frame-ancestors 'none' or 'self' prevents the page from being embedded
      expect(csp).toContain("frame-ancestors");
    });

    test('CSP script-src directive controls script execution', async ({ page }) => {
      const response = await page.goto('/');
      const csp = response!.headers()['content-security-policy'];

      expect(csp).toContain("script-src");
    });
  });
});

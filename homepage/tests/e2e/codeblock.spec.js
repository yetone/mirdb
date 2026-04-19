/**
 * Code Example Section E2E Tests (String-based)
 * Owner: Scenario 4 - Code Example Section with Copy Functionality
 *
 * Tests for:
 * - Code example section display
 * - SET/GET command examples
 * - Telnet connection example
 * - Copy button structure
 * - Code text selectability CSS
 *
 * Using HTML string testing approach to avoid Playwright environment issues.
 * See: jest-html-string-testing skill
 */

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Code Example Section', () => {
  let html;
  let css;

  beforeAll(() => {
    const htmlPath = join(__dirname, '../../index.html');
    html = readFileSync(htmlPath, 'utf-8');

    const cssPath = join(__dirname, '../../css/components/codeblock.css');
    css = readFileSync(cssPath, 'utf-8');
  });

  test('TC1: Code block contains SET command', () => {
    // Check for SET command in code blocks
    expect(html).toContain('SET mykey');
  });

  test('TC2: Code block contains GET command', () => {
    // Check for GET command in code blocks
    expect(html).toContain('GET mykey');
  });

  test('TC3: Code block contains telnet connection example', () => {
    // Check for telnet connection command
    expect(html).toContain('telnet localhost 12333');
    expect(html).toContain('data-testid="code-block-connection"');
  });

  test('TC4: Copy button exists with proper attributes for clipboard functionality', () => {
    // Verify copy buttons exist with required attributes
    expect(html).toContain('data-copy-button');
    expect(html).toContain('aria-label="Copy code to clipboard"');
    expect(html).toContain('class="code-block__copy-text">Copy</span>');

    // Verify code content has data-code-content for copy functionality
    expect(html).toContain('data-code-content');
  });

  test('TC5: Code text CSS ensures selectability', () => {
    // Verify CSS has user-select: text for code content
    expect(css).toContain('user-select: text');
    expect(css).toContain('-webkit-user-select: text');
    expect(css).toContain('-moz-user-select: text');
  });

  test('Code example section has proper heading', () => {
    expect(html).toContain('id="code-example-heading"');
    expect(html).toContain('>Code Examples</h2>');
  });

  test('Code blocks have language labels', () => {
    // Connection block has Terminal label
    expect(html).toMatch(/data-testid="code-block-connection"[\s\S]*?class="code-block__language">Terminal<\/span>/);

    // Commands block has Memcached Protocol label
    expect(html).toMatch(/data-testid="code-block-commands"[\s\S]*?class="code-block__language">Memcached Protocol<\/span>/);
  });

  test('Copy buttons have accessible labels', () => {
    // All copy buttons should have aria-label
    const copyButtonMatches = html.match(/data-copy-button/g);
    expect(copyButtonMatches).not.toBeNull();
    expect(copyButtonMatches.length).toBeGreaterThan(0);

    // Check all have aria-label
    const ariaLabelMatches = html.match(/aria-label="Copy code to clipboard"/g);
    expect(ariaLabelMatches).not.toBeNull();
    expect(ariaLabelMatches.length).toBe(copyButtonMatches.length);
  });

  test('Copy buttons are keyboard accessible (have button type)', () => {
    // Copy buttons should have type="button" for proper keyboard handling
    const buttonTypeMatches = html.match(/class="code-block__copy"[^>]*type="button"/g);
    expect(buttonTypeMatches).not.toBeNull();
    expect(buttonTypeMatches.length).toBeGreaterThan(0);
  });

  test('CSS defines focus styles for copy buttons', () => {
    // Verify focus styles exist for keyboard accessibility
    expect(css).toContain('.code-block__copy:focus');
    expect(css).toContain('outline:');
  });

  test('Code example section is properly structured', () => {
    // Section has aria-labelledby
    expect(html).toContain('aria-labelledby="code-example-heading"');

    // Section has proper structure
    expect(html).toContain('class="code-example__container"');
    expect(html).toContain('class="code-example__intro"');
    expect(html).toContain('class="code-example__blocks"');
  });

  test('Code example section has proper intro text', () => {
    expect(html).toContain('Connect to MiRDB using the standard memcached protocol');
  });

  test('Code blocks have proper CSS classes', () => {
    expect(html).toContain('class="code-block"');
    expect(html).toContain('class="code-block__header"');
    expect(html).toContain('class="code-block__content"');
    expect(html).toContain('class="code-block__code"');
  });

  test('CSS defines copied state styling', () => {
    // Verify copied state styles exist
    expect(css).toContain('.code-block__copy--copied');
    expect(css).toContain('var(--color-success)');
  });
});

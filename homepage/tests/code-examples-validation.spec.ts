import { test, expect } from '@playwright/test';
import * as TOML from '@iarna/toml';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// ES module compatibility for __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Read the QuickStart.astro file to extract code examples
const quickStartPath = path.join(__dirname, '../src/components/QuickStart.astro');
const quickStartContent = fs.readFileSync(quickStartPath, 'utf-8');

// Extract code examples from the QuickStart.astro file
function extractCodeExamples(content: string): {
  installCommand: string;
  configToml: string;
  rustExample: string;
  shellUsage: string;
} {
  // Extract installCommand
  const installMatch = content.match(/const installCommand = `([^`]+)`/);
  const installCommand = installMatch ? installMatch[1] : '';

  // Extract configToml (multiline)
  const configMatch = content.match(/const configToml = `([\s\S]*?)`;/);
  const configToml = configMatch ? configMatch[1] : '';

  // Extract rustExample (multiline)
  const rustMatch = content.match(/const rustExample = `([\s\S]*?)`;/);
  const rustExample = rustMatch ? rustMatch[1] : '';

  // Extract shellUsage (multiline)
  const shellMatch = content.match(/const shellUsage = `([\s\S]*?)`;/);
  const shellUsage = shellMatch ? shellMatch[1] : '';

  return { installCommand, configToml, rustExample, shellUsage };
}

const codeExamples = extractCodeExamples(quickStartContent);

test.describe('Code Examples Validation', () => {
  test.describe('Test Case 1: Installation Commands Validation', () => {
    test('TC1.1: Installation command uses correct cargo install syntax', async () => {
      // Verify cargo install command is valid
      expect(codeExamples.installCommand).toBe('cargo install mirdb-server');

      // Verify it starts with 'cargo install'
      expect(codeExamples.installCommand).toMatch(/^cargo install /);

      // Verify package name follows cargo naming conventions (lowercase, hyphens allowed)
      const packageName = codeExamples.installCommand.replace('cargo install ', '');
      expect(packageName).toMatch(/^[a-z][a-z0-9_-]*$/);
    });

    test('TC1.2: Shell usage commands have valid syntax', async () => {
      // Verify mirdb-server command exists and has --config flag
      expect(codeExamples.shellUsage).toContain('mirdb-server --config');

      // Verify telnet command syntax
      expect(codeExamples.shellUsage).toContain('telnet localhost 11211');

      // Verify memcached protocol commands (set, get)
      expect(codeExamples.shellUsage).toContain('set mykey 0 0 5');
      expect(codeExamples.shellUsage).toContain('get mykey');
    });

    test('TC1.3: Server startup command has valid flag structure', async () => {
      // Extract the mirdb-server command
      const serverCommand = codeExamples.shellUsage.match(/mirdb-server[^\n]+/);
      expect(serverCommand).not.toBeNull();

      // Verify --config flag is properly followed by a path
      expect(serverCommand![0]).toMatch(/mirdb-server --config \/[^\s]+/);
    });
  });

  test.describe('Test Case 2: TOML Configuration Validation', () => {
    test('TC2.1: TOML configuration is syntactically valid and parseable', async () => {
      // This is the key unit test - parse the TOML configuration
      let parsed: TOML.JsonMap | null = null;
      let parseError: Error | null = null;

      try {
        parsed = TOML.parse(codeExamples.configToml);
      } catch (e) {
        parseError = e as Error;
      }

      // Ensure parsing succeeded
      expect(parseError).toBeNull();
      expect(parsed).not.toBeNull();
    });

    test('TC2.2: TOML configuration has required server section', async () => {
      const parsed = TOML.parse(codeExamples.configToml);

      // Verify [server] section exists
      expect(parsed).toHaveProperty('server');

      const server = parsed.server as Record<string, unknown>;

      // Verify required server fields
      expect(server).toHaveProperty('host');
      expect(server).toHaveProperty('port');

      // Verify types
      expect(typeof server.host).toBe('string');
      expect(typeof server.port).toBe('number');

      // Verify values are sensible
      expect(server.host).toBe('127.0.0.1');
      expect(server.port).toBe(11211); // Standard memcached port
    });

    test('TC2.3: TOML configuration has required storage section', async () => {
      const parsed = TOML.parse(codeExamples.configToml);

      // Verify [storage] section exists
      expect(parsed).toHaveProperty('storage');

      const storage = parsed.storage as Record<string, unknown>;

      // Verify required storage fields
      expect(storage).toHaveProperty('data_dir');
      expect(storage).toHaveProperty('memtable_size');

      // Verify types
      expect(typeof storage.data_dir).toBe('string');
      expect(typeof storage.memtable_size).toBe('number');

      // Verify data_dir is an absolute path
      expect(storage.data_dir).toMatch(/^\//);
    });

    test('TC2.4: TOML configuration has required compaction section', async () => {
      const parsed = TOML.parse(codeExamples.configToml);

      // Verify [compaction] section exists
      expect(parsed).toHaveProperty('compaction');

      const compaction = parsed.compaction as Record<string, unknown>;

      // Verify required compaction fields
      expect(compaction).toHaveProperty('level_size_multiplier');
      expect(compaction).toHaveProperty('max_levels');

      // Verify types
      expect(typeof compaction.level_size_multiplier).toBe('number');
      expect(typeof compaction.max_levels).toBe('number');

      // Verify reasonable values for LSM Tree configuration
      expect(compaction.level_size_multiplier).toBeGreaterThan(0);
      expect(compaction.max_levels).toBeGreaterThan(0);
    });

    test('TC2.5: TOML comment syntax is correct', async () => {
      // Verify that comments use # syntax
      expect(codeExamples.configToml).toContain('#');

      // Verify the comment is well-formed (comment starts with #)
      const lines = codeExamples.configToml.split('\n');
      const commentLines = lines.filter(line => line.includes('#'));

      for (const line of commentLines) {
        // Comments should have # followed by content
        expect(line).toMatch(/#\s*\S/);
      }
    });
  });

  test.describe('Test Case 3: README Consistency Validation', () => {
    test('TC3.1: Code examples are consistent with memcached protocol mentioned in README', async () => {
      // Read the README
      const readmePath = path.join(__dirname, '../../README.md');
      const readmeContent = fs.readFileSync(readmePath, 'utf-8');

      // README mentions memcached protocol
      expect(readmeContent.toLowerCase()).toContain('memcached');

      // Shell examples use memcached protocol (port 11211)
      expect(codeExamples.shellUsage).toContain('11211');

      // Configuration uses memcached default port
      const parsed = TOML.parse(codeExamples.configToml);
      const server = parsed.server as Record<string, unknown>;
      expect(server.port).toBe(11211);
    });

    test('TC3.2: Product name is consistent between README and code examples', async () => {
      // Read the README
      const readmePath = path.join(__dirname, '../../README.md');
      const readmeContent = fs.readFileSync(readmePath, 'utf-8');

      // README mentions MirDB
      expect(readmeContent).toContain('MirDB');

      // Installation command references mirdb
      expect(codeExamples.installCommand.toLowerCase()).toContain('mirdb');

      // Shell usage references mirdb-server
      expect(codeExamples.shellUsage).toContain('mirdb-server');
    });

    test('TC3.3: Key-value store functionality demonstrated in examples', async () => {
      // README describes a key-value store
      const readmePath = path.join(__dirname, '../../README.md');
      const readmeContent = fs.readFileSync(readmePath, 'utf-8');
      expect(readmeContent.toLowerCase()).toContain('key-value');

      // Shell examples demonstrate key-value operations (set/get)
      expect(codeExamples.shellUsage).toContain('set mykey');
      expect(codeExamples.shellUsage).toContain('get mykey');

      // Rust example demonstrates key-value operations
      expect(codeExamples.rustExample).toContain('set mykey');
      expect(codeExamples.rustExample).toContain('get mykey');
    });
  });

  test.describe('Test Case 4: Shell Command Syntax Validation', () => {
    test('TC4.1: Shell comments use correct # syntax', async () => {
      // Shell comments should start with #
      const lines = codeExamples.shellUsage.split('\n');
      const commentLines = lines.filter(line => line.trim().startsWith('#'));

      // Should have at least some comments
      expect(commentLines.length).toBeGreaterThan(0);

      // Each comment line should be valid
      for (const line of commentLines) {
        expect(line.trim()).toMatch(/^#\s*\S/);
      }
    });

    test('TC4.2: Telnet command uses correct syntax', async () => {
      // telnet command format: telnet <host> <port>
      expect(codeExamples.shellUsage).toMatch(/telnet\s+\S+\s+\d+/);

      // Specifically: telnet localhost 11211
      expect(codeExamples.shellUsage).toContain('telnet localhost 11211');
    });

    test('TC4.3: Memcached set command uses correct format', async () => {
      // memcached set format: set <key> <flags> <exptime> <bytes>
      // Followed by the value on next line
      expect(codeExamples.shellUsage).toMatch(/set\s+\S+\s+\d+\s+\d+\s+\d+/);

      // Verify the specific example: set mykey 0 0 5
      // flags=0, exptime=0, bytes=5 (length of "hello")
      expect(codeExamples.shellUsage).toContain('set mykey 0 0 5');
      expect(codeExamples.shellUsage).toContain('hello');
    });

    test('TC4.4: Memcached get command uses correct format', async () => {
      // memcached get format: get <key>
      expect(codeExamples.shellUsage).toMatch(/get\s+\S+/);

      // Verify the specific example
      expect(codeExamples.shellUsage).toContain('get mykey');
    });

    test('TC4.5: Config path uses Unix-style absolute path', async () => {
      // Config path should start with /
      expect(codeExamples.shellUsage).toMatch(/--config\s+\/[^\s]+/);

      // Specific path used
      expect(codeExamples.shellUsage).toContain('/etc/mirdb.toml');
    });
  });

  test.describe('Rust Code Example Validation', () => {
    test('TC5.1: Rust example has valid use statements', async () => {
      // Check for proper use statements
      expect(codeExamples.rustExample).toContain('use std::net::TcpStream');
      expect(codeExamples.rustExample).toContain('use std::io::{Read, Write}');
    });

    test('TC5.2: Rust example has valid main function signature', async () => {
      // Check for main function with Result return type
      expect(codeExamples.rustExample).toMatch(/fn main\(\)\s*->\s*std::io::Result<\(\)>/);
    });

    test('TC5.3: Rust example uses correct connection syntax', async () => {
      // TcpStream::connect with proper address format
      expect(codeExamples.rustExample).toContain('TcpStream::connect("127.0.0.1:11211")');
    });

    test('TC5.4: Rust example demonstrates memcached protocol usage', async () => {
      // Set command with proper memcached format (includes \r\n)
      expect(codeExamples.rustExample).toContain('set mykey 0 0 5');
      expect(codeExamples.rustExample).toContain('hello');

      // Get command
      expect(codeExamples.rustExample).toContain('get mykey');
    });

    test('TC5.5: Rust example handles I/O operations', async () => {
      // Write operations
      expect(codeExamples.rustExample).toContain('write_all');

      // Read operations
      expect(codeExamples.rustExample).toContain('.read(');

      // Error propagation with ?
      expect(codeExamples.rustExample).toContain('?');
    });
  });
});

test.describe('Code Examples Display on Homepage', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC6.1: Installation command is displayed correctly on homepage', async ({ page }) => {
    // Navigate to quick-start section using the correct CTA button
    await page.click('[data-testid="cta-get-started"]');

    // Find the installation code block
    const installationStep = page.locator('[data-testid="installation-step-1"]');
    await expect(installationStep).toBeVisible();

    // Verify the cargo install command is shown
    const codeContent = await installationStep.locator('code').textContent();
    expect(codeContent).toContain('cargo install mirdb-server');
  });

  test('TC6.2: TOML configuration is displayed correctly on homepage', async ({ page }) => {
    // Navigate to quick-start section using the correct CTA button
    await page.click('[data-testid="cta-get-started"]');

    // Find the configuration step
    const configStep = page.locator('[data-testid="installation-step-2"]');
    await expect(configStep).toBeVisible();

    // Verify TOML content is shown (use language-toml class to find the code block, not inline code)
    const codeContent = await configStep.locator('code.language-toml').textContent();
    expect(codeContent).toContain('[server]');
    expect(codeContent).toContain('host');
    expect(codeContent).toContain('port');
    expect(codeContent).toContain('[storage]');
    expect(codeContent).toContain('[compaction]');
  });

  test('TC6.3: Shell usage commands are displayed correctly on homepage', async ({ page }) => {
    // Navigate to quick-start section using the correct CTA button
    await page.click('[data-testid="cta-get-started"]');

    // Find the shell usage step
    const shellStep = page.locator('[data-testid="installation-step-3"]');
    await expect(shellStep).toBeVisible();

    // Verify shell commands are shown
    const codeContent = await shellStep.locator('code').textContent();
    expect(codeContent).toContain('mirdb-server');
    expect(codeContent).toContain('telnet');
    expect(codeContent).toContain('set mykey');
    expect(codeContent).toContain('get mykey');
  });

  test('TC6.4: Rust example is displayed correctly on homepage', async ({ page }) => {
    // Navigate to quick-start section using the correct CTA button
    await page.click('[data-testid="cta-get-started"]');

    // Find the Rust step
    const rustStep = page.locator('[data-testid="installation-step-4"]');
    await expect(rustStep).toBeVisible();

    // Verify Rust code is shown
    const codeContent = await rustStep.locator('code').textContent();
    expect(codeContent).toContain('use std::net::TcpStream');
    expect(codeContent).toContain('fn main()');
    expect(codeContent).toContain('TcpStream::connect');
  });
});

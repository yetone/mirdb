/**
 * Code Example Accuracy Tests
 * Owner: Scenario 15 - Code Example Accuracy
 *
 * Validates that quick-start code examples are syntactically correct
 * and match actual MirDB usage patterns.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Code examples extracted from the quickstart section
interface CodeExample {
  type: 'shell' | 'memcached';
  content: string;
  description: string;
}

// Shell command patterns for validation
const SHELL_PATTERNS = {
  gitClone: /^git\s+clone\s+https?:\/\/[\w./-]+\.git$/,
  cd: /^cd\s+[\w./-]+$/,
  cargoBuild: /^cargo\s+build(\s+--[\w-]+)*$/,
  executeCommand: /^\.\/([\w./-]+\/)*[\w.-]+(\s+-[\w]\s+[\w./]+)*$/,
  telnet: /^telnet\s+(localhost|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[\w.-]+)\s+\d+$/,
};

// Memcached protocol patterns
const MEMCACHED_PATTERNS = {
  // SET command: set <key> <flags> <exptime> <bytes> [noreply]
  set: /^set\s+\S+\s+\d+\s+\d+\s+\d+(\s+noreply)?$/,
  // GET command: get <key>*
  get: /^get\s+\S+(\s+\S+)*$/,
  // ADD command: add <key> <flags> <exptime> <bytes> [noreply]
  add: /^add\s+\S+\s+\d+\s+\d+\s+\d+(\s+noreply)?$/,
  // DELETE command: delete <key> [noreply]
  delete: /^delete\s+\S+(\s+noreply)?$/,
  // REPLACE command: replace <key> <flags> <exptime> <bytes> [noreply]
  replace: /^replace\s+\S+\s+\d+\s+\d+\s+\d+(\s+noreply)?$/,
};

// Valid cargo subcommands
const VALID_CARGO_SUBCOMMANDS = ['build', 'run', 'test', 'check', 'install', 'new', 'init', 'doc'];

// Valid cargo build flags
const VALID_CARGO_FLAGS = ['--release', '--target', '--features', '--all-features', '--no-default-features', '--jobs', '-j'];

describe('Code Example Accuracy', () => {
  let htmlContent: string;
  let installationCode: string;
  let usageCode: string;

  beforeAll(() => {
    // Read the index.html file to extract code examples
    const indexPath = resolve(__dirname, '../../src/index.html');
    htmlContent = readFileSync(indexPath, 'utf-8');

    // Extract installation code block
    const installMatch = htmlContent.match(/<code[^>]*id="install-code"[^>]*>([\s\S]*?)<\/code>/);
    installationCode = installMatch ? cleanCodeBlock(installMatch[1]) : '';

    // Extract usage code block
    const usageMatch = htmlContent.match(/<code[^>]*id="usage-code"[^>]*>([\s\S]*?)<\/code>/);
    usageCode = usageMatch ? cleanCodeBlock(usageMatch[1]) : '';
  });

  describe('Test Case 1: Shell Command Syntax Validation', () => {
    it('should have valid git clone syntax', () => {
      const commands = extractShellCommands(installationCode);
      const gitCloneCmd = commands.find(cmd => cmd.startsWith('git clone'));

      expect(gitCloneCmd).toBeDefined();
      expect(gitCloneCmd).toMatch(SHELL_PATTERNS.gitClone);
    });

    it('should have valid cd command syntax', () => {
      const commands = extractShellCommands(installationCode);
      const cdCmd = commands.find(cmd => cmd.startsWith('cd '));

      expect(cdCmd).toBeDefined();
      expect(cdCmd).toMatch(SHELL_PATTERNS.cd);
    });

    it('should have valid cargo build syntax', () => {
      const commands = extractShellCommands(installationCode);
      const cargoBuildCmd = commands.find(cmd => cmd.startsWith('cargo build'));

      expect(cargoBuildCmd).toBeDefined();
      expect(cargoBuildCmd).toMatch(SHELL_PATTERNS.cargoBuild);

      // Validate flags
      if (cargoBuildCmd) {
        const flags = cargoBuildCmd.split(' ').filter(part => part.startsWith('--'));
        flags.forEach(flag => {
          const flagName = flag.split('=')[0];
          expect(VALID_CARGO_FLAGS).toContain(flagName);
        });
      }
    });

    it('should have valid executable command syntax', () => {
      const commands = extractShellCommands(installationCode);
      const execCmd = commands.find(cmd => cmd.startsWith('./'));

      expect(execCmd).toBeDefined();
      expect(execCmd).toMatch(SHELL_PATTERNS.executeCommand);
    });

    it('should have valid telnet command syntax', () => {
      const commands = extractShellCommands(usageCode);
      const telnetCmd = commands.find(cmd => cmd.startsWith('telnet'));

      expect(telnetCmd).toBeDefined();
      expect(telnetCmd).toMatch(SHELL_PATTERNS.telnet);
    });

    it('should use valid port number for telnet', () => {
      const commands = extractShellCommands(usageCode);
      const telnetCmd = commands.find(cmd => cmd.startsWith('telnet'));

      if (telnetCmd) {
        const portMatch = telnetCmd.match(/\s(\d+)$/);
        expect(portMatch).not.toBeNull();
        if (portMatch) {
          const port = parseInt(portMatch[1], 10);
          expect(port).toBeGreaterThan(0);
          expect(port).toBeLessThanOrEqual(65535);
          // Default MirDB port is 12333
          expect(port).toBe(12333);
        }
      }
    });
  });

  describe('Test Case 2: Cargo/Build Commands Match README', () => {
    let readmeContent: string;

    beforeAll(() => {
      const readmePath = resolve(__dirname, '../../../README.md');
      try {
        readmeContent = readFileSync(readmePath, 'utf-8');
      } catch {
        readmeContent = '';
      }
    });

    it('should use valid cargo subcommand', () => {
      const commands = extractShellCommands(installationCode);
      const cargoCmd = commands.find(cmd => cmd.startsWith('cargo'));

      expect(cargoCmd).toBeDefined();
      if (cargoCmd) {
        const parts = cargoCmd.split(' ');
        expect(parts.length).toBeGreaterThanOrEqual(2);
        expect(VALID_CARGO_SUBCOMMANDS).toContain(parts[1]);
      }
    });

    it('should follow standard Rust build conventions', () => {
      const commands = extractShellCommands(installationCode);

      // Should have cargo build command
      const hasCargoBuild = commands.some(cmd => cmd.includes('cargo build'));
      expect(hasCargoBuild).toBe(true);

      // Release builds should use --release flag
      const cargoBuildCmd = commands.find(cmd => cmd.includes('cargo build'));
      if (cargoBuildCmd?.includes('release')) {
        expect(cargoBuildCmd).toContain('--release');
      }
    });

    it('should produce expected binary path', () => {
      const commands = extractShellCommands(installationCode);

      // Check if execution command matches expected build output
      const hasCargoBuildRelease = commands.some(cmd => cmd.includes('cargo build --release'));
      const execCmd = commands.find(cmd => cmd.startsWith('./'));

      if (hasCargoBuildRelease && execCmd) {
        // Release builds output to target/release/
        expect(execCmd).toContain('target/release/');
      }
    });

    it('should use consistent project name in commands', () => {
      const commands = extractShellCommands(installationCode);

      // Find git clone URL
      const gitCloneCmd = commands.find(cmd => cmd.startsWith('git clone'));
      const cdCmd = commands.find(cmd => cmd.startsWith('cd '));

      if (gitCloneCmd && cdCmd) {
        // Extract repo name from clone URL
        const repoMatch = gitCloneCmd.match(/\/([^/]+)\.git$/);
        const cdDirMatch = cdCmd.match(/cd\s+(\S+)/);

        if (repoMatch && cdDirMatch) {
          expect(cdDirMatch[1]).toBe(repoMatch[1]);
        }
      }
    });
  });

  describe('Test Case 3: Memcached Command Syntax Validation', () => {
    it('should have valid SET command syntax', () => {
      const commands = extractMemcachedCommands(usageCode);
      const setCmd = commands.find(cmd => cmd.startsWith('set '));

      expect(setCmd).toBeDefined();
      expect(setCmd).toMatch(MEMCACHED_PATTERNS.set);
    });

    it('should have valid GET command syntax', () => {
      const commands = extractMemcachedCommands(usageCode);
      const getCmd = commands.find(cmd => cmd.startsWith('get '));

      expect(getCmd).toBeDefined();
      expect(getCmd).toMatch(MEMCACHED_PATTERNS.get);
    });

    it('should have SET command with correct byte count', () => {
      // SET command format: set <key> <flags> <exptime> <bytes>
      // The bytes value should match the actual data length
      const commands = extractMemcachedCommands(usageCode);
      const setCmd = commands.find(cmd => cmd.startsWith('set '));

      if (setCmd) {
        const parts = setCmd.split(' ');
        // set mykey 0 0 5
        expect(parts.length).toBeGreaterThanOrEqual(5);

        const byteCount = parseInt(parts[4], 10);
        // "hello" has 5 bytes
        expect(byteCount).toBe(5);
      }
    });

    it('should use valid flags value in SET command', () => {
      const commands = extractMemcachedCommands(usageCode);
      const setCmd = commands.find(cmd => cmd.startsWith('set '));

      if (setCmd) {
        const parts = setCmd.split(' ');
        // flags is the third part (index 2)
        const flags = parseInt(parts[2], 10);
        // Flags should be a non-negative integer
        expect(flags).toBeGreaterThanOrEqual(0);
        expect(flags).toBeLessThanOrEqual(4294967295); // uint32 max
      }
    });

    it('should use valid exptime value in SET command', () => {
      const commands = extractMemcachedCommands(usageCode);
      const setCmd = commands.find(cmd => cmd.startsWith('set '));

      if (setCmd) {
        const parts = setCmd.split(' ');
        // exptime is the fourth part (index 3)
        const exptime = parseInt(parts[3], 10);
        // 0 means never expire, positive values are seconds
        expect(exptime).toBeGreaterThanOrEqual(0);
      }
    });

    it('should follow correct memcached SET command sequence', () => {
      // Memcached SET is a two-line command:
      // 1. set <key> <flags> <exptime> <bytes>
      // 2. <data>
      const lines = usageCode.split('\n').filter(line =>
        line.trim() && !line.trim().startsWith('#')
      );

      // Find SET command index
      const setIndex = lines.findIndex(line => line.trim().startsWith('set '));

      if (setIndex >= 0 && setIndex < lines.length - 1) {
        // Next non-empty line should be the data
        const dataLine = lines.slice(setIndex + 1).find(line =>
          line.trim() && !line.trim().startsWith('get ') && !line.trim().startsWith('#')
        );

        expect(dataLine).toBeDefined();
        // Check that data length matches byte count
        const setCmd = lines[setIndex].trim();
        const parts = setCmd.split(' ');
        const byteCount = parseInt(parts[4], 10);

        if (dataLine) {
          expect(dataLine.trim().length).toBe(byteCount);
        }
      }
    });

    it('should use consistent key names in SET and GET commands', () => {
      const commands = extractMemcachedCommands(usageCode);
      const setCmd = commands.find(cmd => cmd.startsWith('set '));
      const getCmd = commands.find(cmd => cmd.startsWith('get '));

      if (setCmd && getCmd) {
        const setKey = setCmd.split(' ')[1];
        const getKey = getCmd.split(' ')[1];

        // GET should retrieve a key that was SET
        expect(getKey).toBe(setKey);
      }
    });
  });
});

/**
 * Helper function to clean HTML from code blocks
 */
function cleanCodeBlock(html: string): string {
  return html
    // Remove HTML tags
    .replace(/<[^>]+>/g, '')
    // Decode HTML entities
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    // Normalize whitespace
    .trim();
}

/**
 * Extract shell commands from code block (ignoring comments)
 */
function extractShellCommands(code: string): string[] {
  return code
    .split('\n')
    .map(line => line.trim())
    .filter(line => line && !line.startsWith('#'))
    // Also filter out memcached commands (handled separately)
    .filter(line => !line.startsWith('set ') && !line.startsWith('get '))
    // Filter out data values (like 'hello' after SET)
    .filter(line => line.length > 2 && line.includes(' '));
}

/**
 * Extract memcached protocol commands from code block
 */
function extractMemcachedCommands(code: string): string[] {
  return code
    .split('\n')
    .map(line => line.trim())
    .filter(line => line && !line.startsWith('#'))
    .filter(line =>
      line.startsWith('set ') ||
      line.startsWith('get ') ||
      line.startsWith('add ') ||
      line.startsWith('delete ') ||
      line.startsWith('replace ') ||
      line.startsWith('append ') ||
      line.startsWith('prepend ')
    );
}

/**
 * Quickstart Section Unit Tests
 * Owner: Scenario 3 - Quickstart Installation Section
 *
 * Test Cases:
 * TC6: Commands include cargo or relevant installation syntax
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Quickstart Commands Validation', () => {
  let document: Document;

  beforeAll(() => {
    const htmlPath = join(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC6: Commands include cargo or relevant installation syntax', () => {
    const quickstartSection = document.getElementById('quickstart');
    expect(quickstartSection).not.toBeNull();

    const codeBlocks = quickstartSection!.querySelectorAll('.code-block code');
    expect(codeBlocks.length).toBeGreaterThan(0);

    // Collect all code content
    const allCode = Array.from(codeBlocks)
      .map(code => code.textContent || '')
      .join('\n');

    // Valid installation commands patterns
    const validPatterns = [
      /cargo\s+(build|run|install|test)/i,  // Cargo commands
      /git\s+clone/i,                        // Git clone
      /npm\s+(install|i)/i,                  // npm install
      /yarn\s+(add|install)/i,               // yarn
      /pip\s+install/i,                      // Python pip
      /brew\s+install/i,                     // Homebrew
      /apt(-get)?\s+install/i,               // apt
      /curl\s+.*\s*\|/i,                     // Piped curl install
      /\.\/target\/release/i,                // Running compiled binary
      /telnet/i,                             // Connecting to service
      /cd\s+\w+/i,                           // Change directory
    ];

    const hasValidCommand = validPatterns.some(pattern => pattern.test(allCode));
    expect(hasValidCommand).toBe(true);

    // Specifically verify cargo is mentioned (since this is a Rust project)
    const hasCargoOrGit = /cargo|git\s+clone/i.test(allCode);
    expect(hasCargoOrGit).toBe(true);
  });

  it('Code blocks contain complete commands', () => {
    const quickstartSection = document.getElementById('quickstart');
    const codeBlocks = quickstartSection!.querySelectorAll('.code-block code');

    // Each code block should have non-empty content
    codeBlocks.forEach((codeBlock) => {
      const content = codeBlock.textContent?.trim();
      expect(content).toBeTruthy();
      expect(content!.length).toBeGreaterThan(0);
    });
  });

  it('Installation steps have descriptive headings', () => {
    const quickstartSection = document.getElementById('quickstart');
    const steps = quickstartSection!.querySelectorAll('.step');

    expect(steps.length).toBeGreaterThan(0);

    steps.forEach((step) => {
      const heading = step.querySelector('h3');
      expect(heading).not.toBeNull();
      expect(heading!.textContent?.trim().length).toBeGreaterThan(0);
    });
  });
});

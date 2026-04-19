/**
 * Component Unit Tests
 * Owner: Scenario 3 - Quick Start and Installation Section
 *
 * Tests for:
 * - QuickStartSection component rendering
 * - Installation steps in correct order
 */

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('QuickStartSection', () => {
  let html;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = join(__dirname, '../../index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  test('TC-5: should render QuickStartSection component with installation steps in correct order', () => {
    // Verify section exists with quickstart id and class
    expect(html).toContain('id="quickstart"');
    expect(html).toContain('class="quickstart"');

    // Verify steps list exists
    expect(html).toContain('class="quickstart__steps"');

    // Count the number of steps (data-step attributes)
    const stepMatches = html.match(/data-step="\d+"/g);
    expect(stepMatches).not.toBeNull();
    expect(stepMatches.length).toBeGreaterThanOrEqual(3);

    // Extract step numbers and verify sequential order
    const stepNumbers = stepMatches.map(match => {
      const num = match.match(/data-step="(\d+)"/);
      return num ? parseInt(num[1], 10) : 0;
    });

    // Check steps are in sequential order
    for (let i = 0; i < stepNumbers.length - 1; i++) {
      expect(stepNumbers[i]).toBeLessThan(stepNumbers[i + 1]);
    }

    // Extract quickstart section content
    const sectionMatch = html.match(/<section id="quickstart"[\s\S]*?<!-- END: Scenario 3/);
    const sectionContent = sectionMatch ? sectionMatch[0] : '';

    // Verify step titles exist in order (Download -> Cargo -> Run)
    const downloadIndex = sectionContent.indexOf('Download Pre-built Binary');
    const cargoIndex = sectionContent.indexOf('Install via Cargo');
    const runIndex = sectionContent.indexOf('Run the Server');

    expect(downloadIndex).toBeGreaterThan(-1);
    expect(cargoIndex).toBeGreaterThan(-1);
    expect(runIndex).toBeGreaterThan(-1);

    // Steps should be in order
    expect(downloadIndex).toBeLessThan(cargoIndex);
    expect(cargoIndex).toBeLessThan(runIndex);
  });

  test('should have section heading', () => {
    expect(html).toContain('id="quickstart-heading"');
    expect(html).toMatch(/Quick Start/i);
  });

  test('should contain cargo install command', () => {
    // Check for cargo install in a code block
    expect(html).toContain('cargo install mirdb');
    expect(html).toMatch(/<code>cargo install/);
  });

  test('should contain mirdb-server run command', () => {
    // Check for mirdb-server command in a code block
    expect(html).toContain('mirdb-server');
    expect(html).toMatch(/<code>mirdb-server<\/code>/);
  });

  test('should have link to GitHub releases', () => {
    expect(html).toContain('github.com/yetone/mirdb/releases');
  });

  test('should have proper accessibility attributes', () => {
    // Check for aria-labelledby on section
    expect(html).toContain('aria-labelledby="quickstart-heading"');

    // Check for role="list" on steps
    expect(html).toContain('role="list"');
  });
});

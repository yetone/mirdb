/**
 * Unit tests for UsageExamples component.
 * Owner: Scenario 3 - Usage Examples Section
 *
 * Tests:
 * - Renders section with heading
 * - Contains tabbed interface with Basic, Advanced, Benchmark tabs
 * - Displays SET, GET, DELETE command examples
 * - Proper ARIA attributes for accessibility
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Read the component source for static analysis
const componentPath = resolve(__dirname, '../../../src/components/UsageExamples.astro');
const componentSource = readFileSync(componentPath, 'utf-8');

// Read the examples data
const examplesPath = resolve(__dirname, '../../../src/content/examples.json');
const examplesData = JSON.parse(readFileSync(examplesPath, 'utf-8'));

describe('UsageExamples Component', () => {
  describe('Structure', () => {
    it('should have section with id="examples"', () => {
      expect(componentSource).toContain('id="examples"');
    });

    it('should have heading with id="examples-heading"', () => {
      expect(componentSource).toContain('id="examples-heading"');
    });

    it('should have aria-labelledby pointing to heading', () => {
      expect(componentSource).toContain('aria-labelledby="examples-heading"');
    });
  });

  describe('Tab Interface', () => {
    it('should have tablist role', () => {
      expect(componentSource).toContain('role="tablist"');
    });

    it('should have tab buttons for Basic, Advanced, and Benchmark', () => {
      expect(componentSource).toContain("label: 'Basic'");
      expect(componentSource).toContain("label: 'Advanced'");
      expect(componentSource).toContain("label: 'Benchmark'");
    });

    it('should have tabpanel role for each tab content', () => {
      expect(componentSource).toContain('role="tabpanel"');
    });

    it('should have proper ARIA attributes on tab buttons', () => {
      expect(componentSource).toContain('role="tab"');
      expect(componentSource).toContain('aria-selected');
      expect(componentSource).toContain('aria-controls');
    });
  });

  describe('Accessibility', () => {
    it('should support keyboard navigation', () => {
      expect(componentSource).toContain('ArrowLeft');
      expect(componentSource).toContain('ArrowRight');
      expect(componentSource).toContain('Home');
      expect(componentSource).toContain('End');
    });

    it('should manage tabindex for tab navigation', () => {
      expect(componentSource).toContain('tabindex');
    });
  });

  describe('CodeBlock Integration', () => {
    it('should import CodeBlock component', () => {
      expect(componentSource).toContain("import CodeBlock from './CodeBlock.astro'");
    });

    it('should render CodeBlock for each example', () => {
      expect(componentSource).toContain('<CodeBlock');
    });
  });
});

describe('Examples Content Data', () => {
  describe('Basic Examples', () => {
    it('should have basic examples array', () => {
      expect(examplesData.basic).toBeDefined();
      expect(Array.isArray(examplesData.basic)).toBe(true);
      expect(examplesData.basic.length).toBeGreaterThan(0);
    });

    it('should contain SET command example', () => {
      const hasSet = examplesData.basic.some((ex: { code: string }) =>
        ex.code.toLowerCase().includes('set ')
      );
      expect(hasSet).toBe(true);
    });

    it('should contain GET command example', () => {
      const hasGet = examplesData.basic.some((ex: { code: string }) =>
        ex.code.toLowerCase().includes('get ')
      );
      expect(hasGet).toBe(true);
    });

    it('should contain DELETE command example', () => {
      const hasDelete = examplesData.basic.some((ex: { code: string }) =>
        ex.code.toLowerCase().includes('delete ')
      );
      expect(hasDelete).toBe(true);
    });

    it('should show STORED response for SET command', () => {
      const hasStored = examplesData.basic.some((ex: { code: string }) =>
        ex.code.includes('STORED')
      );
      expect(hasStored).toBe(true);
    });

    it('should show VALUE response format for GET command', () => {
      const hasValue = examplesData.basic.some((ex: { code: string }) =>
        ex.code.includes('VALUE')
      );
      expect(hasValue).toBe(true);
    });
  });

  describe('Advanced Examples', () => {
    it('should have advanced examples array', () => {
      expect(examplesData.advanced).toBeDefined();
      expect(Array.isArray(examplesData.advanced)).toBe(true);
      expect(examplesData.advanced.length).toBeGreaterThan(0);
    });

    it('should contain multiple key operations', () => {
      const hasMultiple = examplesData.advanced.some((ex: { title: string }) =>
        ex.title.toLowerCase().includes('multiple')
      );
      expect(hasMultiple).toBe(true);
    });
  });

  describe('Benchmark Examples', () => {
    it('should have benchmark examples array', () => {
      expect(examplesData.benchmark).toBeDefined();
      expect(Array.isArray(examplesData.benchmark)).toBe(true);
      expect(examplesData.benchmark.length).toBeGreaterThan(0);
    });

    it('should contain performance metrics', () => {
      const hasMetrics = examplesData.benchmark.some((ex: { code: string }) =>
        ex.code.includes('Throughput') || ex.code.includes('ops/sec')
      );
      expect(hasMetrics).toBe(true);
    });
  });

  describe('Example Structure', () => {
    const allExamples = [
      ...examplesData.basic,
      ...examplesData.advanced,
      ...examplesData.benchmark
    ];

    it('each example should have title property', () => {
      allExamples.forEach((ex: { title?: string }) => {
        expect(ex.title).toBeDefined();
        expect(typeof ex.title).toBe('string');
        expect(ex.title!.length).toBeGreaterThan(0);
      });
    });

    it('each example should have description property', () => {
      allExamples.forEach((ex: { description?: string }) => {
        expect(ex.description).toBeDefined();
        expect(typeof ex.description).toBe('string');
        expect(ex.description!.length).toBeGreaterThan(0);
      });
    });

    it('each example should have code property', () => {
      allExamples.forEach((ex: { code?: string }) => {
        expect(ex.code).toBeDefined();
        expect(typeof ex.code).toBe('string');
        expect(ex.code!.length).toBeGreaterThan(0);
      });
    });
  });
});

describe('Memcached Protocol Accuracy', () => {
  it('SET command should match Memcached syntax: set <key> <flags> <ttl> <bytes>', () => {
    // SET command format: set key flags ttl bytes
    const setExample = examplesData.basic.find((ex: { code: string }) =>
      ex.code.toLowerCase().includes('set ')
    );
    expect(setExample).toBeDefined();

    // Check for proper format with flags (0), ttl (0), and bytes count
    expect(setExample.code).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/i);
  });

  it('GET command should match Memcached syntax: get <key>', () => {
    const getExample = examplesData.basic.find((ex: { code: string }) =>
      ex.code.toLowerCase().includes('get ')
    );
    expect(getExample).toBeDefined();
    expect(getExample.code).toMatch(/get\s+\w+/i);
  });

  it('VALUE response should match format: VALUE <key> <flags> <bytes>', () => {
    const valueExample = examplesData.basic.find((ex: { code: string }) =>
      ex.code.includes('VALUE')
    );
    expect(valueExample).toBeDefined();
    expect(valueExample.code).toMatch(/VALUE\s+\w+\s+\d+\s+\d+/);
  });

  it('DELETE command response should be DELETED', () => {
    const deleteExample = examplesData.basic.find((ex: { code: string }) =>
      ex.code.toLowerCase().includes('delete ')
    );
    expect(deleteExample).toBeDefined();
    expect(deleteExample.code).toContain('DELETED');
  });
});

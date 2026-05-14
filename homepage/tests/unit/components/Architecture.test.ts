import { describe, it, expect } from 'vitest';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const HOMEPAGE_DIR = join(__dirname, '..', '..', '..');
const ARCHITECTURE_JSON = join(HOMEPAGE_DIR, 'src', 'data', 'architecture.json');
const ARCHITECTURE_ASTRO = join(
  HOMEPAGE_DIR,
  'src',
  'components',
  'Architecture',
  'Architecture.astro',
);
const ARCHITECTURE_CSS = join(
  HOMEPAGE_DIR,
  'src',
  'components',
  'Architecture',
  'Architecture.module.css',
);

function readArchitectureJson() {
  const raw = readFileSync(ARCHITECTURE_JSON, 'utf-8');
  return JSON.parse(raw);
}

function readArchitectureAstro() {
  return readFileSync(ARCHITECTURE_ASTRO, 'utf-8');
}

function readArchitectureCss() {
  return readFileSync(ARCHITECTURE_CSS, 'utf-8');
}

const EXPECTED_COMPONENT_TERMS = [
  'memcached',
  'protocol',
  'memtable',
  'wal',
  'lsm',
];

describe('Architecture Overview', () => {
  describe('Test Case 1: Architecture diagram', () => {
    it('has a diagram image with a non-empty alt attribute', () => {
      const source = readArchitectureAstro();

      // Should have an img element
      expect(source).toContain('<img');
      expect(source).toContain('alt=');
      expect(source).toContain('diagram-img');

      // The alt text should be non-empty (from data)
      const data = readArchitectureJson();
      expect(data).toHaveProperty('diagramAlt');
      expect(typeof data.diagramAlt).toBe('string');
      expect(data.diagramAlt.length).toBeGreaterThan(0);

      // Should have diagramSrc
      expect(data).toHaveProperty('diagramSrc');
      expect(typeof data.diagramSrc).toBe('string');
      expect(data.diagramSrc.length).toBeGreaterThan(0);
    });

    it('the diagram src references an SVG asset', () => {
      const data = readArchitectureJson();
      expect(data.diagramSrc).toMatch(/\.svg$/);
    });
  });

  describe('Test Case 2: Component descriptions', () => {
    it('has at least 4 architectural components in the data', () => {
      const data = readArchitectureJson();
      expect(Array.isArray(data.components)).toBe(true);
      expect(data.components.length).toBeGreaterThanOrEqual(4);
    });

    it('each component has a non-empty name and description', () => {
      const data = readArchitectureJson();
      data.components.forEach((component: any) => {
        expect(component).toHaveProperty('id');
        expect(typeof component.id).toBe('string');
        expect(component.id.length).toBeGreaterThan(0);

        expect(component).toHaveProperty('name');
        expect(typeof component.name).toBe('string');
        expect(component.name.length).toBeGreaterThan(0);

        expect(component).toHaveProperty('description');
        expect(typeof component.description).toBe('string');
        // Description should have technical depth - at least 50 chars
        expect(component.description.length).toBeGreaterThanOrEqual(50);
      });
    });

    it('renders each component with an h3 heading element', () => {
      const source = readArchitectureAstro();

      // Components should be rendered with individual headings
      expect(source).toContain('<h3');
      expect(source).toContain('component-heading');

      // Should map over components and render each one
      expect(source).toContain('components.map');
    });
  });

  describe('Test Case 3: Required component categories', () => {
    it('mentions client protocol layer (memcached)', () => {
      const data = readArchitectureJson();
      const namesAndDescs = data.components
        .map((c: any) => `${c.name} ${c.description}`.toLowerCase())
        .join(' ');

      expect(namesAndDescs).toMatch(/memcached|protocol layer/);
    });

    it('mentions memtable / skip-list memtable', () => {
      const data = readArchitectureJson();
      const namesAndDescs = data.components
        .map((c: any) => `${c.name} ${c.description}`.toLowerCase())
        .join(' ');

      expect(namesAndDescs).toMatch(/memtable|skip-list/);
    });

    it('mentions WAL / write-ahead log', () => {
      const data = readArchitectureJson();
      const namesAndDescs = data.components
        .map((c: any) => `${c.name} ${c.description}`.toLowerCase())
        .join(' ');

      expect(namesAndDescs).toMatch(/wal|write-ahead log|write ahead log/);
    });

    it('mentions LSM tree storage', () => {
      const data = readArchitectureJson();
      const namesAndDescs = data.components
        .map((c: any) => `${c.name} ${c.description}`.toLowerCase())
        .join(' ');

      expect(namesAndDescs).toMatch(/lsm tree|lsm-tree/);
    });
  });

  describe('Test Case 4: External documentation links', () => {
    it('has at least one documentation link', () => {
      const data = readArchitectureJson();
      expect(Array.isArray(data.documentationLinks)).toBe(true);
      expect(data.documentationLinks.length).toBeGreaterThanOrEqual(1);
    });

    it('each link has a label and an external URL', () => {
      const data = readArchitectureJson();
      data.documentationLinks.forEach((link: any) => {
        expect(link).toHaveProperty('label');
        expect(typeof link.label).toBe('string');
        expect(link.label.length).toBeGreaterThan(0);

        expect(link).toHaveProperty('url');
        expect(typeof link.url).toBe('string');
        expect(link.url.startsWith('http')).toBe(true);
        expect(link.url.startsWith('#')).toBe(false);
      });
    });

    it('renders links with target="_blank" and rel="noopener noreferrer"', () => {
      const source = readArchitectureAstro();

      // Links should open in new tabs
      expect(source).toContain('target="_blank"');
      expect(source).toContain('rel="noopener noreferrer"');

      // Links should map over documentationLinks
      expect(source).toContain('documentationLinks');
    });
  });

  describe('Test Case 5: Missing diagram graceful fallback', () => {
    it('renders a fallback when diagramSrc is empty', () => {
      const source = readArchitectureAstro();

      // Should have conditional logic for diagramSrc
      expect(source).toContain('diagramSrc');

      // Should have a fallback/placeholder element
      expect(source).toContain('diagram-fallback');
      expect(source).toContain('diagram-fallback-icon');
      expect(source).toContain('diagram-fallback-text');
    });

    it('architecture.json handles empty diagramSrc without breaking', () => {
      const original = readFileSync(ARCHITECTURE_JSON, 'utf-8');

      try {
        const data = readArchitectureJson();
        // Set diagramSrc to empty
        const modified = { ...data, diagramSrc: '' };
        writeFileSync(ARCHITECTURE_JSON, JSON.stringify(modified, null, 2), 'utf-8');

        // Re-read - should not throw
        const reloaded = readArchitectureJson();
        expect(reloaded.diagramSrc).toBe('');
        // Section title and components should still be intact
        expect(reloaded.sectionTitle.length).toBeGreaterThan(0);
        expect(reloaded.components.length).toBeGreaterThanOrEqual(4);
      } finally {
        writeFileSync(ARCHITECTURE_JSON, original, 'utf-8');
      }
    });

    it('renders section content even when components array is empty', () => {
      const source = readArchitectureAstro();

      // Should have empty state handling
      expect(source).toContain('components.length');
      expect(source).toContain('architecture-empty');
      expect(source).toContain('Architecture details coming soon');
    });
  });

  describe('Test Case: CSS styling', () => {
    it('has responsive grid layout for component cards', () => {
      const css = readArchitectureCss();

      // Desktop: 2-column grid
      expect(css).toMatch(/grid-template-columns\s*:\s*repeat\s*\(\s*2\s*,\s*1fr\s*\)/);

      // Should have tablet breakpoint
      expect(css).toContain('max-width: 1279px');

      // Should have mobile breakpoint
      expect(css).toContain('max-width: 767px');
    });

    it('styles the diagram image with border and shadow', () => {
      const css = readArchitectureCss();
      expect(css).toContain('.diagram-img');
      expect(css).toContain('border-radius');
      expect(css).toContain('box-shadow');
    });

    it('styles documentation links with hover effect', () => {
      const css = readArchitectureCss();
      expect(css).toContain('.doc-link');
      expect(css).toContain('.doc-link:hover');
    });
  });
});

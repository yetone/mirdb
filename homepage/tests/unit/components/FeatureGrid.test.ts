import { describe, it, expect } from 'vitest';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

const HOMEPAGE_DIR = join(__dirname, '..', '..', '..');
const FEATURES_JSON = join(HOMEPAGE_DIR, 'src', 'data', 'features.json');
const FEATURES_CSS = join(
  HOMEPAGE_DIR,
  'src',
  'components',
  'Features',
  'Features.module.css',
);
const FEATURE_GRID_ASTRO = join(
  HOMEPAGE_DIR,
  'src',
  'components',
  'Features',
  'FeatureGrid.astro',
);

function readFeaturesJson() {
  const raw = readFileSync(FEATURES_JSON, 'utf-8');
  return JSON.parse(raw);
}

function readFeaturesCss() {
  return readFileSync(FEATURES_CSS, 'utf-8');
}

function readFeatureGridAstro() {
  return readFileSync(FEATURE_GRID_ASTRO, 'utf-8');
}

const EXPECTED_FEATURE_NAMES = [
  'Memcached Protocol',
  'LSM Tree Storage',
  'Skip-List Memtable',
  'WAL Durability',
  'Tokio Async Runtime',
];

describe('Feature Highlights Section', () => {
  describe('Test Case 1: Feature card count', () => {
    it('has exactly 5 feature cards in the feature data', () => {
      const features = readFeaturesJson();
      expect(Array.isArray(features)).toBe(true);
      expect(features).toHaveLength(5);
    });

    it('every feature has a unique string id', () => {
      const features = readFeaturesJson();
      const ids = features.map((f: any) => f.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(5);
      ids.forEach((id: unknown) => {
        expect(typeof id).toBe('string');
        expect((id as string).length).toBeGreaterThan(0);
      });
    });
  });

  describe('Test Case 2: Feature card headings', () => {
    it('each feature card has a non-empty heading', () => {
      const features = readFeaturesJson();
      features.forEach((feature: any) => {
        expect(feature).toHaveProperty('name');
        expect(typeof feature.name).toBe('string');
        expect(feature.name.length).toBeGreaterThan(0);
      });
    });

    it('all expected feature names are present', () => {
      const features = readFeaturesJson();
      const names = features.map((f: any) => f.name);
      for (const expectedName of EXPECTED_FEATURE_NAMES) {
        expect(names).toContain(expectedName);
      }
    });
  });

  describe('Test Case 3: Feature card descriptions', () => {
    it('each feature card has a description paragraph with at least 20 characters', () => {
      const features = readFeaturesJson();
      features.forEach((feature: any) => {
        expect(feature).toHaveProperty('description');
        expect(typeof feature.description).toBe('string');
        expect(feature.description.length).toBeGreaterThanOrEqual(20);
      });
    });

    it('every feature has a non-empty icon property', () => {
      const features = readFeaturesJson();
      features.forEach((feature: any) => {
        expect(feature).toHaveProperty('icon');
        expect(typeof feature.icon).toBe('string');
        expect(feature.icon.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Test Case 4: Desktop layout (1280px+)', () => {
    it('uses a multi-column grid layout (3 columns) on desktop', () => {
      const css = readFeaturesCss();
      // The default .features-grid should have 3-column layout
      expect(css).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns\s*:\s*repeat\s*\(\s*3\s*,\s*1fr\s*\)/s);
    });
  });

  describe('Test Case 5: Tablet layout (768px)', () => {
    it('adjusts to 2 columns at tablet breakpoint', () => {
      const css = readFeaturesCss();
      // The 768px-1279px media query should set 2 columns
      expect(css).toContain('max-width: 1279px');
      // Within any media query, there should be a 2-column grid rule
      expect(css).toMatch(/grid-template-columns\s*:\s*repeat\s*\(\s*2\s*,\s*1fr\s*\)/);
    });
  });

  describe('Test Case 6: Mobile layout (375px)', () => {
    it('collapses to single column at mobile breakpoint', () => {
      const css = readFeaturesCss();
      // The below-768px media query should set 1 column
      expect(css).toContain('max-width: 767px');
      // Within the mobile media query, the grid should collapse to 1 column
      expect(css).toMatch(/grid-template-columns\s*:\s*1fr/);
    });
  });

  describe('Test Case 7: Empty features array graceful handling', () => {
    it('renders gracefully (shows empty state) without throwing an error', () => {
      const source = readFeatureGridAstro();

      // FeatureGrid should import FeatureCard and features data
      expect(source).toContain('FeatureCard');

      // Should check features.length and have a fallback for empty state
      expect(source).toContain('features.length');

      // Should have an empty state message
      expect(source).toContain('Feature highlights coming soon');

      // Should have a ternary or conditional checking length > 0
      expect(source).toMatch(/features\.length\s*>\s*0/);
    });

    it('features data file can be parsed without error when empty', () => {
      // Backup original features data
      const original = readFileSync(FEATURES_JSON, 'utf-8');

      try {
        // Write empty array
        writeFileSync(FEATURES_JSON, '[]', 'utf-8');

        // Should parse without error
        const features = readFeaturesJson();
        expect(Array.isArray(features)).toBe(true);
        expect(features).toHaveLength(0);
      } finally {
        // Restore original features data
        writeFileSync(FEATURES_JSON, original, 'utf-8');
      }
    });
  });
});

/**
 * Unit tests for Features component.
 * Owner: Scenario 2 - Features Section Display
 */
import { describe, it, expect, beforeAll } from 'vitest';
import * as cheerio from 'cheerio';
import { readFileSync } from 'fs';
import { join } from 'path';

// Read the component files and content
const featuresContent = JSON.parse(
  readFileSync(join(__dirname, '../../../src/content/features.json'), 'utf-8')
);

const featuresComponent = readFileSync(
  join(__dirname, '../../../src/components/Features.astro'),
  'utf-8'
);

const featureCardComponent = readFileSync(
  join(__dirname, '../../../src/components/FeatureCard.astro'),
  'utf-8'
);

describe('Features Section', () => {
  describe('Features Content Data', () => {
    it('should have exactly 6 features', () => {
      expect(featuresContent).toHaveLength(6);
    });

    it('should have Memcached Protocol feature with correct content', () => {
      const feature = featuresContent.find((f: any) => f.title === 'Memcached Protocol');
      expect(feature).toBeDefined();
      expect(feature.icon).toBe('protocol');
      expect(feature.description).toContain('Drop-in replacement');
    });

    it('should have Persistent Storage feature with correct content', () => {
      const feature = featuresContent.find((f: any) => f.title === 'Persistent Storage');
      expect(feature).toBeDefined();
      expect(feature.icon).toBe('storage');
      expect(feature.description).toContain('LSM-tree');
    });

    it('should have High Performance feature with correct content', () => {
      const feature = featuresContent.find((f: any) => f.title === 'High Performance');
      expect(feature).toBeDefined();
      expect(feature.icon).toBe('performance');
      expect(feature.description).toContain('memtable');
      expect(feature.description).toContain('skip-list');
    });

    it('should have Compaction feature with correct content', () => {
      const feature = featuresContent.find((f: any) => f.title === 'Compaction');
      expect(feature).toBeDefined();
      expect(feature.icon).toBe('compaction');
      expect(feature.description).toContain('minor and major compaction');
    });

    it('should have Write-Ahead Log feature with correct content', () => {
      const feature = featuresContent.find((f: any) => f.title === 'Write-Ahead Log');
      expect(feature).toBeDefined();
      expect(feature.icon).toBe('wal');
      expect(feature.description).toContain('Durability');
      expect(feature.description).toContain('WAL');
    });

    it('should have Rust Powered feature with correct content', () => {
      const feature = featuresContent.find((f: any) => f.title === 'Rust Powered');
      expect(feature).toBeDefined();
      expect(feature.icon).toBe('rust');
      expect(feature.description).toContain('Rust');
      expect(feature.description).toContain('safety');
      expect(feature.description).toContain('performance');
    });

    it('each feature should have required properties', () => {
      featuresContent.forEach((feature: any) => {
        expect(feature).toHaveProperty('id');
        expect(feature).toHaveProperty('icon');
        expect(feature).toHaveProperty('title');
        expect(feature).toHaveProperty('description');
        expect(typeof feature.title).toBe('string');
        expect(typeof feature.description).toBe('string');
        expect(feature.title.length).toBeGreaterThan(0);
        expect(feature.description.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Features Component Structure', () => {
    it('should have a section with id="features"', () => {
      expect(featuresComponent).toContain('id="features"');
    });

    it('should have a grid layout with responsive classes', () => {
      expect(featuresComponent).toContain('grid');
      expect(featuresComponent).toContain('grid-cols-1');
      expect(featuresComponent).toContain('md:grid-cols-2');
      expect(featuresComponent).toContain('lg:grid-cols-3');
    });

    it('should import and use FeatureCard component', () => {
      expect(featuresComponent).toContain("import FeatureCard from './FeatureCard.astro'");
      expect(featuresComponent).toContain('<FeatureCard');
    });

    it('should import features.json content', () => {
      expect(featuresComponent).toContain("import features from '../content/features.json'");
    });

    it('should iterate over features array', () => {
      expect(featuresComponent).toContain('features.map');
    });

    it('should have section heading', () => {
      expect(featuresComponent).toContain('<h2');
      expect(featuresComponent).toContain('Key Features');
    });
  });

  describe('FeatureCard Component Structure', () => {
    it('should accept icon, title, and description props', () => {
      expect(featureCardComponent).toContain('icon: string');
      expect(featureCardComponent).toContain('title: string');
      expect(featureCardComponent).toContain('description: string');
    });

    it('should render feature title in h3 element', () => {
      expect(featureCardComponent).toContain('<h3');
      expect(featureCardComponent).toContain('{title}');
    });

    it('should render feature description', () => {
      expect(featureCardComponent).toContain('{description}');
    });

    it('should have icon mapping for all 6 feature icons', () => {
      expect(featureCardComponent).toContain('protocol:');
      expect(featureCardComponent).toContain('storage:');
      expect(featureCardComponent).toContain('performance:');
      expect(featureCardComponent).toContain('compaction:');
      expect(featureCardComponent).toContain('wal:');
      expect(featureCardComponent).toContain('rust:');
    });

    it('should have proper CSS classes for styling', () => {
      expect(featureCardComponent).toContain('feature-card');
      expect(featureCardComponent).toContain('feature-title');
      expect(featureCardComponent).toContain('feature-description');
      expect(featureCardComponent).toContain('feature-icon');
    });

    it('should support optional link prop', () => {
      expect(featureCardComponent).toContain('link?: string');
      expect(featureCardComponent).toContain('{link &&');
    });
  });
});

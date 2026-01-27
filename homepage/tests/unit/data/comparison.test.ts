/**
 * Unit tests for comparison.json data file.
 * Owner: Scenario 18 - Competitive Positioning Table
 *
 * Tests:
 * - Data structure validation
 * - Required comparison features presence
 * - Feature value verification for competitive positioning
 */

import { describe, it, expect } from 'vitest';
import comparisonData from '../../../src/data/comparison.json';

describe('comparison.json', () => {
  describe('data structure', () => {
    it('should have products array', () => {
      expect(comparisonData).toHaveProperty('products');
      expect(Array.isArray(comparisonData.products)).toBe(true);
    });

    it('should have features array', () => {
      expect(comparisonData).toHaveProperty('features');
      expect(Array.isArray(comparisonData.features)).toBe(true);
    });

    it('should include MirDB, Memcached, and Redis in products', () => {
      expect(comparisonData.products).toContain('MirDB');
      expect(comparisonData.products).toContain('Memcached');
      expect(comparisonData.products).toContain('Redis');
    });

    it('should have non-empty features list', () => {
      expect(comparisonData.features.length).toBeGreaterThan(0);
    });

    it('each feature should have required fields', () => {
      comparisonData.features.forEach((feature) => {
        expect(feature).toHaveProperty('name');
        expect(feature).toHaveProperty('mirdb');
        expect(feature).toHaveProperty('memcached');
        expect(feature).toHaveProperty('redis');
        expect(typeof feature.name).toBe('string');
        expect(typeof feature.mirdb).toBe('boolean');
        expect(typeof feature.memcached).toBe('boolean');
        expect(typeof feature.redis).toBe('boolean');
        expect(feature.name.length).toBeGreaterThan(0);
      });
    });
  });

  describe('MirDB persistence advantage (Test Case 2)', () => {
    it('should include Persistence feature', () => {
      const persistence = comparisonData.features.find(
        (f) => f.name === 'Persistence'
      );
      expect(persistence).toBeDefined();
    });

    it('should show MirDB has persistence while Memcached does not', () => {
      const persistence = comparisonData.features.find(
        (f) => f.name === 'Persistence'
      );
      expect(persistence?.mirdb).toBe(true);
      expect(persistence?.memcached).toBe(false);
    });
  });

  describe('MirDB memcached protocol compatibility (Test Case 3)', () => {
    it('should include Memcached Protocol feature', () => {
      const protocol = comparisonData.features.find(
        (f) => f.name === 'Memcached Protocol'
      );
      expect(protocol).toBeDefined();
    });

    it('should show MirDB and Memcached have protocol compatibility, Redis does not', () => {
      const protocol = comparisonData.features.find(
        (f) => f.name === 'Memcached Protocol'
      );
      expect(protocol?.mirdb).toBe(true);
      expect(protocol?.memcached).toBe(true);
      expect(protocol?.redis).toBe(false);
    });
  });

  describe('Rust implementation highlight (Test Case 4)', () => {
    it('should include Written in Rust feature', () => {
      const rust = comparisonData.features.find(
        (f) => f.name === 'Written in Rust'
      );
      expect(rust).toBeDefined();
    });

    it('should show MirDB is written in Rust', () => {
      const rust = comparisonData.features.find(
        (f) => f.name === 'Written in Rust'
      );
      expect(rust?.mirdb).toBe(true);
    });

    it('should show Memcached and Redis are not written in Rust', () => {
      const rust = comparisonData.features.find(
        (f) => f.name === 'Written in Rust'
      );
      expect(rust?.memcached).toBe(false);
      expect(rust?.redis).toBe(false);
    });
  });

  describe('additional comparison features', () => {
    it('should include In-Memory Speed feature', () => {
      const speed = comparisonData.features.find(
        (f) => f.name === 'In-Memory Speed'
      );
      expect(speed).toBeDefined();
      // All three should have in-memory speed
      expect(speed?.mirdb).toBe(true);
      expect(speed?.memcached).toBe(true);
      expect(speed?.redis).toBe(true);
    });

    it('should include LSM-Tree Storage feature', () => {
      const lsm = comparisonData.features.find(
        (f) => f.name === 'LSM-Tree Storage'
      );
      expect(lsm).toBeDefined();
      // Only MirDB has LSM-Tree storage
      expect(lsm?.mirdb).toBe(true);
      expect(lsm?.memcached).toBe(false);
      expect(lsm?.redis).toBe(false);
    });
  });

  describe('feature coverage', () => {
    it('should have at least 5 comparison features', () => {
      expect(comparisonData.features.length).toBeGreaterThanOrEqual(5);
    });

    it('all features should have non-empty names', () => {
      comparisonData.features.forEach((feature) => {
        expect(feature.name.trim().length).toBeGreaterThan(0);
      });
    });
  });
});

/**
 * Unit tests for roadmap.json data file.
 * Owner: Scenario 12 - Project Status and Roadmap
 *
 * Tests:
 * - Data structure validation
 * - Required fields presence
 * - Completed features have checkmark indicator (via data structure)
 * - Roadmap items include Raft consensus
 */

import { describe, it, expect } from 'vitest';
import roadmapData from '../../../src/data/roadmap.json';

describe('roadmap.json', () => {
  describe('data structure', () => {
    it('should have completed array', () => {
      expect(roadmapData).toHaveProperty('completed');
      expect(Array.isArray(roadmapData.completed)).toBe(true);
    });

    it('should have planned array', () => {
      expect(roadmapData).toHaveProperty('planned');
      expect(Array.isArray(roadmapData.planned)).toBe(true);
    });

    it('should have non-empty completed features list', () => {
      expect(roadmapData.completed.length).toBeGreaterThan(0);
    });

    it('should have non-empty planned features list', () => {
      expect(roadmapData.planned.length).toBeGreaterThan(0);
    });
  });

  describe('completed features', () => {
    it('each completed feature should have feature and description fields', () => {
      roadmapData.completed.forEach((item) => {
        expect(item).toHaveProperty('feature');
        expect(item).toHaveProperty('description');
        expect(typeof item.feature).toBe('string');
        expect(typeof item.description).toBe('string');
        expect(item.feature.length).toBeGreaterThan(0);
        expect(item.description.length).toBeGreaterThan(0);
      });
    });

    it('should include Memcached Protocol Support as completed', () => {
      const memcachedFeature = roadmapData.completed.find(
        (item) => item.feature.toLowerCase().includes('memcached')
      );
      expect(memcachedFeature).toBeDefined();
    });

    it('should include Persistent Storage as completed', () => {
      const persistentFeature = roadmapData.completed.find(
        (item) => item.feature.toLowerCase().includes('persistent')
      );
      expect(persistentFeature).toBeDefined();
    });

    it('should include LSM-Tree Architecture as completed', () => {
      const lsmFeature = roadmapData.completed.find(
        (item) => item.feature.toLowerCase().includes('lsm')
      );
      expect(lsmFeature).toBeDefined();
    });

    it('should include Write-Ahead Logging as completed', () => {
      const walFeature = roadmapData.completed.find(
        (item) => item.feature.toLowerCase().includes('write-ahead') ||
                  item.feature.toLowerCase().includes('wal')
      );
      expect(walFeature).toBeDefined();
    });
  });

  describe('roadmap/planned features', () => {
    it('each planned feature should have feature and description fields', () => {
      roadmapData.planned.forEach((item) => {
        expect(item).toHaveProperty('feature');
        expect(item).toHaveProperty('description');
        expect(typeof item.feature).toBe('string');
        expect(typeof item.description).toBe('string');
        expect(item.feature.length).toBeGreaterThan(0);
        expect(item.description.length).toBeGreaterThan(0);
      });
    });

    it('should include Raft consensus in roadmap', () => {
      const raftFeature = roadmapData.planned.find(
        (item) => item.feature.toLowerCase().includes('raft')
      );
      expect(raftFeature).toBeDefined();
      expect(raftFeature?.feature).toContain('Raft');
    });
  });
});

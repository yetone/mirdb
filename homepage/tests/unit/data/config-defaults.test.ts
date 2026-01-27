/**
 * Unit tests for config-defaults.json data file.
 * Owner: Scenario 15 - Configuration Display
 *
 * Tests:
 * - Data structure validation
 * - Required configuration options presence
 * - Default values verification (REQ-7)
 */

import { describe, it, expect } from 'vitest';
import configData from '../../../src/data/config-defaults.json';

describe('config-defaults.json', () => {
  describe('data structure', () => {
    it('should have configOptions array', () => {
      expect(configData).toHaveProperty('configOptions');
      expect(Array.isArray(configData.configOptions)).toBe(true);
    });

    it('should have non-empty configOptions list', () => {
      expect(configData.configOptions.length).toBeGreaterThan(0);
    });

    it('each config option should have required fields', () => {
      configData.configOptions.forEach((option) => {
        expect(option).toHaveProperty('name');
        expect(option).toHaveProperty('key');
        expect(option).toHaveProperty('default');
        expect(option).toHaveProperty('description');
        expect(typeof option.name).toBe('string');
        expect(typeof option.key).toBe('string');
        expect(typeof option.default).toBe('string');
        expect(typeof option.description).toBe('string');
        expect(option.name.length).toBeGreaterThan(0);
        expect(option.key.length).toBeGreaterThan(0);
        expect(option.default.length).toBeGreaterThan(0);
        expect(option.description.length).toBeGreaterThan(0);
      });
    });
  });

  describe('default listen address (REQ-7)', () => {
    it('should include listen address configuration', () => {
      const listenAddr = configData.configOptions.find(
        (opt) => opt.key === 'addr'
      );
      expect(listenAddr).toBeDefined();
    });

    it('should have default listen address of 0.0.0.0:12333', () => {
      const listenAddr = configData.configOptions.find(
        (opt) => opt.key === 'addr'
      );
      expect(listenAddr?.default).toBe('0.0.0.0:12333');
    });
  });

  describe('memtable max size (REQ-7)', () => {
    it('should include memtable max size configuration', () => {
      const memtableSize = configData.configOptions.find(
        (opt) => opt.key === 'mem_table_max_size'
      );
      expect(memtableSize).toBeDefined();
    });

    it('should have default memtable max size of 4MB', () => {
      const memtableSize = configData.configOptions.find(
        (opt) => opt.key === 'mem_table_max_size'
      );
      expect(memtableSize?.default).toBe('4MB');
    });
  });

  describe('SSTable max size (REQ-7)', () => {
    it('should include SSTable max size configuration', () => {
      const sstableSize = configData.configOptions.find(
        (opt) => opt.key === 'sst_max_size'
      );
      expect(sstableSize).toBeDefined();
    });

    it('should have default SSTable max size of 100MB', () => {
      const sstableSize = configData.configOptions.find(
        (opt) => opt.key === 'sst_max_size'
      );
      expect(sstableSize?.default).toBe('100MB');
    });
  });

  describe('block size (REQ-7)', () => {
    it('should include block size configuration', () => {
      const blockSize = configData.configOptions.find(
        (opt) => opt.key === 'block_size'
      );
      expect(blockSize).toBeDefined();
    });

    it('should have default block size of 4KB', () => {
      const blockSize = configData.configOptions.find(
        (opt) => opt.key === 'block_size'
      );
      expect(blockSize?.default).toBe('4KB');
    });
  });

  describe('max LSM levels (REQ-7)', () => {
    it('should include max LSM levels configuration', () => {
      const maxLevel = configData.configOptions.find(
        (opt) => opt.key === 'max_level'
      );
      expect(maxLevel).toBeDefined();
    });

    it('should have default max LSM levels of 7', () => {
      const maxLevel = configData.configOptions.find(
        (opt) => opt.key === 'max_level'
      );
      expect(maxLevel?.default).toBe('7');
    });
  });

  describe('additional configuration options', () => {
    it('should include data directory configuration', () => {
      const workDir = configData.configOptions.find(
        (opt) => opt.key === 'work_dir'
      );
      expect(workDir).toBeDefined();
      expect(workDir?.default).toBe('/tmp/mirdb');
    });

    it('should include L0 compaction trigger configuration', () => {
      const l0Trigger = configData.configOptions.find(
        (opt) => opt.key === 'l0_compaction_trigger'
      );
      expect(l0Trigger).toBeDefined();
      expect(l0Trigger?.default).toBe('4');
    });
  });

  describe('configuration key format', () => {
    it('all config keys should use snake_case format', () => {
      configData.configOptions.forEach((option) => {
        // Keys should only contain lowercase letters, numbers, and underscores
        expect(option.key).toMatch(/^[a-z][a-z0-9_]*$/);
      });
    });
  });
});

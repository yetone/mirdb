/**
 * TechSpecs Component Unit Tests
 * Owner: Scenario 5 - Configuration Display
 *
 * Tests for:
 * - TechSpecs component rendering
 * - Configuration values display
 * - Component structure validation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { TECHSPECS_CONFIG } from '../fixtures/test-data';

// Load the TechSpecs HTML component from file system
const techspecsHtmlPath = resolve(__dirname, '../../src/components/TechSpecs/TechSpecs.html');
const techspecsHtml = readFileSync(techspecsHtmlPath, 'utf-8');

describe('TechSpecs Component', () => {
  beforeEach(() => {
    // Set up the DOM with TechSpecs HTML
    document.body.innerHTML = techspecsHtml;
  });

  it('renders techspecs section without errors', () => {
    const techspecsSection = document.querySelector('#techspecs');
    expect(techspecsSection).not.toBeNull();
    expect(techspecsSection?.classList.contains('techspecs')).toBe(true);
  });

  it('contains section title with Configuration text', () => {
    const title = document.querySelector('.techspecs__title');
    expect(title).not.toBeNull();
    expect(title?.textContent).toContain('Configuration');
  });

  it('displays default port 12333', () => {
    const values = document.querySelectorAll('.techspecs__value');
    const portValue = Array.from(values).find(v => v.textContent?.includes(TECHSPECS_CONFIG.PORT));
    expect(portValue).not.toBeNull();
    expect(portValue?.textContent).toBe(TECHSPECS_CONFIG.PORT);
  });

  it('displays work directory /tmp/mirdb', () => {
    const values = document.querySelectorAll('.techspecs__value');
    const workDirValue = Array.from(values).find(v => v.textContent?.includes(TECHSPECS_CONFIG.WORK_DIR));
    expect(workDirValue).not.toBeNull();
    expect(workDirValue?.textContent).toBe(TECHSPECS_CONFIG.WORK_DIR);
  });

  it('displays SSTable max size 100 MB', () => {
    const sectionText = document.querySelector('#techspecs')?.textContent || '';
    expect(sectionText).toMatch(/100\s*MB/);
  });

  it('displays memtable max size 4 MB', () => {
    const sectionText = document.querySelector('#techspecs')?.textContent || '';
    expect(sectionText).toMatch(/4\s*MB/);
  });

  it('displays block size 4 KB', () => {
    const sectionText = document.querySelector('#techspecs')?.textContent || '';
    expect(sectionText).toMatch(/4\s*KB/);
  });

  it('displays max LSM levels 7', () => {
    const sectionText = document.querySelector('#techspecs')?.textContent || '';
    expect(sectionText).toContain(TECHSPECS_CONFIG.MAX_LSM_LEVELS);
  });
});

describe('TechSpecs Component Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = techspecsHtml;
  });

  it('has all required structural elements', () => {
    expect(document.querySelector('.techspecs')).not.toBeNull();
    expect(document.querySelector('.techspecs__title')).not.toBeNull();
    expect(document.querySelector('.techspecs__grid')).not.toBeNull();
    expect(document.querySelector('.techspecs__item')).not.toBeNull();
  });

  it('has correct number of configuration items', () => {
    const items = document.querySelectorAll('.techspecs__item');
    expect(items.length).toBeGreaterThanOrEqual(4);
    expect(items.length).toBe(6); // Port, Work Dir, SSTable, Memtable, Block Size, LSM Levels
  });

  it('has proper definition list structure with dt/dd pairs', () => {
    const labels = document.querySelectorAll('.techspecs__label');
    const values = document.querySelectorAll('.techspecs__value');

    expect(labels.length).toBe(values.length);
    expect(labels.length).toBe(6);

    // Verify dt elements
    labels.forEach(label => {
      expect(label.tagName.toLowerCase()).toBe('dt');
    });

    // Verify dd elements
    values.forEach(value => {
      expect(value.tagName.toLowerCase()).toBe('dd');
    });
  });

  it('contains heading element h2', () => {
    const heading = document.querySelector('.techspecs h2');
    expect(heading).not.toBeNull();
    expect(heading?.tagName.toLowerCase()).toBe('h2');
  });

  it('has grid container for layout', () => {
    const grid = document.querySelector('.techspecs__grid');
    expect(grid).not.toBeNull();

    // Grid should contain all config items
    const items = grid?.querySelectorAll('.techspecs__item');
    expect(items?.length).toBe(6);
  });
});

describe('TechSpecs Configuration Values', () => {
  beforeEach(() => {
    document.body.innerHTML = techspecsHtml;
  });

  it('all required configuration labels are present', () => {
    const labels = document.querySelectorAll('.techspecs__label');
    const labelTexts = Array.from(labels).map(l => l.textContent?.toLowerCase() || '');

    expect(labelTexts.some(t => t.includes('port'))).toBe(true);
    expect(labelTexts.some(t => t.includes('directory') || t.includes('dir'))).toBe(true);
    expect(labelTexts.some(t => t.includes('sstable'))).toBe(true);
    expect(labelTexts.some(t => t.includes('memtable'))).toBe(true);
  });

  it('each config item has both label and value', () => {
    const items = document.querySelectorAll('.techspecs__item');

    items.forEach(item => {
      const label = item.querySelector('.techspecs__label');
      const value = item.querySelector('.techspecs__value');

      expect(label).not.toBeNull();
      expect(value).not.toBeNull();
      expect(label?.textContent?.trim().length).toBeGreaterThan(0);
      expect(value?.textContent?.trim().length).toBeGreaterThan(0);
    });
  });
});

/**
 * Unit tests for Features component.
 * Owner: Scenario 2 - Feature Overview Section
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderFeatures } from '../../../src/components/Features';
import { features } from '../../../src/data/features';

describe('Features Component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderFeatures();
  });

  it('renders a section element with proper id', () => {
    expect(section.tagName).toBe('SECTION');
    expect(section.id).toBe('features');
    expect(section.className).toContain('features-section');
  });

  it('contains at least 6 feature cards', () => {
    const cards = section.querySelectorAll('.feature-card');
    expect(cards.length).toBeGreaterThanOrEqual(6);
  });

  it('each feature card has an icon, title, and description', () => {
    const cards = section.querySelectorAll('.feature-card');

    cards.forEach((card) => {
      const icon = card.querySelector('.feature-icon');
      const title = card.querySelector('.feature-title');
      const description = card.querySelector('.feature-description');

      expect(icon).not.toBeNull();
      expect(title).not.toBeNull();
      expect(description).not.toBeNull();

      // Verify content is not empty
      expect(icon?.textContent?.trim().length).toBeGreaterThan(0);
      expect(title?.textContent?.trim().length).toBeGreaterThan(0);
      expect(description?.textContent?.trim().length).toBeGreaterThan(0);
    });
  });

  it('includes Persistent Storage feature', () => {
    const titles = Array.from(section.querySelectorAll('.feature-title'));
    const hasPersistentStorage = titles.some(
      (title) => title.textContent?.includes('Persistent Storage')
    );
    expect(hasPersistentStorage).toBe(true);
  });

  it('includes Memcached Protocol feature', () => {
    const titles = Array.from(section.querySelectorAll('.feature-title'));
    const hasMemcachedProtocol = titles.some(
      (title) => title.textContent?.includes('Memcached Protocol')
    );
    expect(hasMemcachedProtocol).toBe(true);
  });

  it('includes LSM Tree Architecture feature', () => {
    const titles = Array.from(section.querySelectorAll('.feature-title'));
    const hasLSMTree = titles.some(
      (title) => title.textContent?.includes('LSM Tree Architecture')
    );
    expect(hasLSMTree).toBe(true);
  });

  it('includes Skip Lists feature', () => {
    const titles = Array.from(section.querySelectorAll('.feature-title'));
    const hasSkipLists = titles.some(
      (title) => title.textContent?.includes('Skip Lists')
    );
    expect(hasSkipLists).toBe(true);
  });

  it('includes Compaction feature', () => {
    const titles = Array.from(section.querySelectorAll('.feature-title'));
    const hasCompaction = titles.some(
      (title) => title.textContent?.includes('Compaction')
    );
    expect(hasCompaction).toBe(true);
  });

  it('includes Async I/O feature', () => {
    const titles = Array.from(section.querySelectorAll('.feature-title'));
    const hasAsyncIO = titles.some(
      (title) => title.textContent?.includes('Async I/O')
    );
    expect(hasAsyncIO).toBe(true);
  });

  it('renders features in a grid container', () => {
    const grid = section.querySelector('.features-grid');
    expect(grid).not.toBeNull();
    expect(grid?.children.length).toBe(features.length);
  });

  it('has proper heading structure', () => {
    const heading = section.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading?.id).toBe('features-heading');
    expect(heading?.textContent).toBe('Key Features');
  });

  it('has accessible aria attributes', () => {
    expect(section.getAttribute('aria-labelledby')).toBe('features-heading');

    const icons = section.querySelectorAll('.feature-icon');
    icons.forEach((icon) => {
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    });
  });

  it('uses semantic article elements for feature cards', () => {
    const cards = section.querySelectorAll('.feature-card');
    cards.forEach((card) => {
      expect(card.tagName).toBe('ARTICLE');
    });
  });
});

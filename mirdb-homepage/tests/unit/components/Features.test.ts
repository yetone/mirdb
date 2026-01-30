/**
 * Features Component Unit Tests.
 * Owner: Scenario 2 - Features Section Grid Display
 *
 * Tests:
 * - Features section renders with 6 cards
 * - Each feature card has icon, title, description
 * - All required features are present
 * - Grid layout classes are applied
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderFeatures, FEATURES } from '../../../src/components/Features';

describe('Features Section', () => {
  let featuresSection: HTMLElement;

  beforeEach(() => {
    featuresSection = renderFeatures();
  });

  // Test Case 1: Features section renders with 6 feature cards
  it('should render features section with 6 feature cards', () => {
    const featureCards = featuresSection.querySelectorAll('.feature-card-item');
    expect(featureCards.length).toBe(6);
  });

  // Test Case 2: Check Persistent Storage feature card
  it('should display Persistent Storage feature card with icon, title, and description', () => {
    const card = featuresSection.querySelector('[data-testid="feature-persistent-storage"]');
    expect(card).not.toBeNull();

    const title = card?.querySelector('[data-testid="feature-title"]');
    expect(title?.textContent).toBe('Persistent Storage');

    const description = card?.querySelector('[data-testid="feature-description"]');
    expect(description?.textContent).toContain('disk');

    const icon = card?.querySelector('[data-testid="feature-icon"]');
    expect(icon).not.toBeNull();
  });

  // Test Case 3: Check Memcached Protocol feature card
  it('should display Memcached Protocol feature card with icon, title, and description', () => {
    const card = featuresSection.querySelector('[data-testid="feature-memcached-protocol"]');
    expect(card).not.toBeNull();

    const title = card?.querySelector('[data-testid="feature-title"]');
    expect(title?.textContent).toBe('Memcached Protocol');

    const description = card?.querySelector('[data-testid="feature-description"]');
    expect(description?.textContent?.toLowerCase()).toContain('protocol');

    const icon = card?.querySelector('[data-testid="feature-icon"]');
    expect(icon).not.toBeNull();
  });

  // Test Case 4: Check LSM-Tree Architecture feature card
  it('should display LSM-Tree Architecture feature card with icon, title, and description', () => {
    const card = featuresSection.querySelector('[data-testid="feature-lsm-tree-architecture"]');
    expect(card).not.toBeNull();

    const title = card?.querySelector('[data-testid="feature-title"]');
    expect(title?.textContent).toBe('LSM-Tree Architecture');

    const description = card?.querySelector('[data-testid="feature-description"]');
    expect(description?.textContent?.toLowerCase()).toMatch(/storage|engine|lsm/i);

    const icon = card?.querySelector('[data-testid="feature-icon"]');
    expect(icon).not.toBeNull();
  });

  // Test Case 5: Check Compaction feature card
  it('should display Compaction feature card with icon, title, and description about minor/major compaction', () => {
    const card = featuresSection.querySelector('[data-testid="feature-compaction"]');
    expect(card).not.toBeNull();

    const title = card?.querySelector('[data-testid="feature-title"]');
    expect(title?.textContent).toBe('Compaction');

    const description = card?.querySelector('[data-testid="feature-description"]');
    expect(description?.textContent?.toLowerCase()).toMatch(/minor|major|compaction/i);

    const icon = card?.querySelector('[data-testid="feature-icon"]');
    expect(icon).not.toBeNull();
  });

  // Test Case 6: Check Tokio Async feature card
  it('should display Tokio Async feature card with icon, title, and description about async networking', () => {
    const card = featuresSection.querySelector('[data-testid="feature-tokio-async"]');
    expect(card).not.toBeNull();

    const title = card?.querySelector('[data-testid="feature-title"]');
    expect(title?.textContent).toBe('Tokio Async');

    const description = card?.querySelector('[data-testid="feature-description"]');
    expect(description?.textContent?.toLowerCase()).toMatch(/async|network/i);

    const icon = card?.querySelector('[data-testid="feature-icon"]');
    expect(icon).not.toBeNull();
  });

  // Test Case 7: Check Rust Performance feature card
  it('should display Rust Performance feature card with icon, title, and description about memory safety and speed', () => {
    const card = featuresSection.querySelector('[data-testid="feature-rust-performance"]');
    expect(card).not.toBeNull();

    const title = card?.querySelector('[data-testid="feature-title"]');
    expect(title?.textContent).toBe('Rust Performance');

    const description = card?.querySelector('[data-testid="feature-description"]');
    expect(description?.textContent?.toLowerCase()).toMatch(/memory|safe|speed|fast/i);

    const icon = card?.querySelector('[data-testid="feature-icon"]');
    expect(icon).not.toBeNull();
  });

  // Test: Grid layout classes are applied
  it('should have grid layout classes applied', () => {
    const grid = featuresSection.querySelector('[data-testid="features-grid"]');
    expect(grid).not.toBeNull();
    expect(grid?.classList.contains('grid')).toBe(true);
  });

  // Test: FEATURES constant exports 6 features
  it('should export FEATURES array with 6 items', () => {
    expect(FEATURES).toBeDefined();
    expect(FEATURES.length).toBe(6);
  });

  // Test: Each feature in FEATURES has required properties
  it('should have all required properties in each feature', () => {
    FEATURES.forEach((feature) => {
      expect(feature.id).toBeDefined();
      expect(feature.title).toBeDefined();
      expect(feature.description).toBeDefined();
      expect(feature.icon).toBeDefined();
    });
  });

  // Test: Section has proper semantic structure
  it('should have proper semantic structure', () => {
    expect(featuresSection.tagName.toLowerCase()).toBe('section');
    expect(featuresSection.getAttribute('id')).toBe('features');
  });
});

// Test Case 8: Integration test for grid responsiveness
describe('Features Section Grid Responsiveness', () => {
  it('should have responsive grid classes for tablet breakpoint (768px)', () => {
    const featuresSection = renderFeatures();
    const grid = featuresSection.querySelector('[data-testid="features-grid"]');

    expect(grid).not.toBeNull();
    // Check for responsive classes that handle 768px breakpoint
    // md: breakpoint in Tailwind is 768px
    // lg: breakpoint in Tailwind is 1024px
    // Grid should be 3-column on desktop (lg:grid-cols-3) and 2-column on tablet (md:grid-cols-2)
    expect(grid?.classList.contains('md:grid-cols-2')).toBe(true);
    expect(grid?.classList.contains('lg:grid-cols-3')).toBe(true);
  });
});

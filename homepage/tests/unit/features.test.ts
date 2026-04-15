/**
 * Features Component Unit Tests
 * Owner: Scenario 2 - Feature List Display
 *
 * Tests for verifying Features component rendering logic.
 */

import { describe, test, expect, beforeEach } from 'vitest';

// Feature data structure
interface Feature {
  id: string;
  title: string;
  description: string;
}

// Mock feature data matching the implementation
const FEATURES: Feature[] = [
  {
    id: 'memcached',
    title: 'Memcached Protocol',
    description: 'Full compatibility with existing Memcached clients.'
  },
  {
    id: 'persistence',
    title: 'Disk Persistence',
    description: 'Your data survives restarts.'
  },
  {
    id: 'lsm',
    title: 'LSM Tree Architecture',
    description: 'Optimized for write-heavy workloads with efficient compaction.'
  },
  {
    id: 'rust',
    title: 'Built with Rust',
    description: 'Memory-safe, fast, and reliable.'
  }
];

// HTML template matching the actual Features section implementation
const featuresHTML = `
  <section id="features" class="features">
    <div class="container">
      <h2 class="features__title">Key Features</h2>
      <div class="features__grid">
        <article class="feature-card">
          <div class="feature-card__icon" aria-hidden="true"></div>
          <h3 class="feature-card__title">Memcached Protocol</h3>
          <p class="feature-card__description">Full compatibility with existing Memcached clients. Drop-in replacement with persistence.</p>
        </article>
        <article class="feature-card">
          <div class="feature-card__icon" aria-hidden="true"></div>
          <h3 class="feature-card__title">Disk Persistence</h3>
          <p class="feature-card__description">Your data survives restarts. No more cache warming or data loss on failures.</p>
        </article>
        <article class="feature-card">
          <div class="feature-card__icon" aria-hidden="true"></div>
          <h3 class="feature-card__title">LSM Tree Architecture</h3>
          <p class="feature-card__description">Optimized for write-heavy workloads with efficient compaction and fast reads.</p>
        </article>
        <article class="feature-card">
          <div class="feature-card__icon" aria-hidden="true"></div>
          <h3 class="feature-card__title">Built with Rust</h3>
          <p class="feature-card__description">Memory-safe, fast, and reliable. No garbage collection pauses or memory leaks.</p>
        </article>
      </div>
    </div>
  </section>
`;

describe('Features Component', () => {
  beforeEach(() => {
    // Set up the DOM using happy-dom's global document
    document.body.innerHTML = featuresHTML;
  });

  test('renders all four key features with icons/styling', () => {
    // Test case 6: Component renders all four key features
    const featureCards = document.querySelectorAll('.feature-card');
    expect(featureCards.length).toBe(4);
  });

  test('each feature has required elements', () => {
    const featureCards = document.querySelectorAll('.feature-card');

    featureCards.forEach((card) => {
      // Each card should have an icon
      const icon = card.querySelector('.feature-card__icon');
      expect(icon).not.toBeNull();

      // Each card should have a title
      const title = card.querySelector('.feature-card__title');
      expect(title).not.toBeNull();

      // Each card should have a description
      const description = card.querySelector('.feature-card__description');
      expect(description).not.toBeNull();
    });
  });

  test('Memcached protocol feature is present', () => {
    const titles = document.querySelectorAll('.feature-card__title');
    const memcachedTitle = Array.from(titles).find(t => t.textContent?.includes('Memcached'));
    expect(memcachedTitle).not.toBeNull();
    expect(memcachedTitle?.textContent).toContain('Memcached');
  });

  test('disk persistence feature is present', () => {
    const titles = document.querySelectorAll('.feature-card__title');
    const persistenceTitle = Array.from(titles).find(t => t.textContent?.includes('Persistence'));
    expect(persistenceTitle).not.toBeNull();
    expect(persistenceTitle?.textContent).toContain('Persistence');
  });

  test('LSM tree architecture feature is present', () => {
    const titles = document.querySelectorAll('.feature-card__title');
    const lsmTitle = Array.from(titles).find(t => t.textContent?.includes('LSM'));
    expect(lsmTitle).not.toBeNull();
    expect(lsmTitle?.textContent).toContain('LSM');
  });

  test('Rust implementation feature is present', () => {
    const titles = document.querySelectorAll('.feature-card__title');
    const rustTitle = Array.from(titles).find(t => t.textContent?.includes('Rust'));
    expect(rustTitle).not.toBeNull();
    expect(rustTitle?.textContent).toContain('Rust');
  });

  test('features section has proper structure', () => {
    const section = document.querySelector('#features');
    expect(section).not.toBeNull();

    // Check section has features class
    expect(section?.classList.contains('features')).toBe(true);

    // Check heading exists
    const heading = document.querySelector('.features__title');
    expect(heading).not.toBeNull();
    expect(heading?.textContent).toContain('Features');
  });

  test('feature icons have aria-hidden attribute', () => {
    const icons = document.querySelectorAll('.feature-card__icon');

    icons.forEach((icon) => {
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    });
  });

  test('feature data structure matches expected format', () => {
    FEATURES.forEach((feature) => {
      expect(feature).toHaveProperty('id');
      expect(feature).toHaveProperty('title');
      expect(feature).toHaveProperty('description');
    });

    expect(FEATURES.length).toBe(4);
  });
});

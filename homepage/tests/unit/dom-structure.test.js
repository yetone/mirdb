/**
 * DOM Structure Validation Tests
 *
 * Validates HTML structure for:
 * - Hero section (Scenario 1)
 * - Features section (Scenario 2)
 * - Configuration section (Scenario 5)
 * - Footer section (Scenario 7)
 * - SEO meta tags (Scenario 13)
 *
 * Each scenario adds tests for their assigned section.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { loadHTML } from '../setup.js';

let document;

beforeAll(() => {
  const dom = loadHTML();
  document = dom.window.document;
});

// ===== Scenario 1: Hero Section Tests =====

describe('Hero Section - Value Proposition Display', () => {
  describe('TC2: Hero section HTML structure', () => {
    it('should have a hero section with correct id', () => {
      const heroSection = document.getElementById('hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection.classList.contains('hero')).toBe(true);
    });

    it('should have a semantic h1 heading with project name', () => {
      const heroTitle = document.querySelector('.hero h1');
      expect(heroTitle).not.toBeNull();
      expect(heroTitle.classList.contains('hero-title')).toBe(true);
      expect(heroTitle.textContent).toContain('MirDB');
    });

    it('should have a tagline paragraph explaining the value proposition', () => {
      const tagline = document.querySelector('.hero-tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.tagName.toLowerCase()).toBe('p');
      // Should mention persistent key-value store
      const text = tagline.textContent.toLowerCase();
      expect(text).toContain('persistent');
      expect(text).toContain('key-value');
    });

    it('should mention Memcached compatibility in tagline', () => {
      const tagline = document.querySelector('.hero-tagline');
      const text = tagline.textContent.toLowerCase();
      expect(text).toContain('memcached');
    });

    it('should have 3-5 key feature highlights', () => {
      const featuresList = document.querySelector('.hero-features');
      expect(featuresList).not.toBeNull();

      const features = featuresList.querySelectorAll('li');
      expect(features.length).toBeGreaterThanOrEqual(3);
      expect(features.length).toBeLessThanOrEqual(5);
    });
  });

  describe('Hero section CTA buttons', () => {
    it('should have a Get Started CTA button', () => {
      const getStartedBtn = document.querySelector('.hero-cta .btn-primary');
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.textContent.trim()).toContain('Get Started');
    });

    it('should have Get Started button link to getting-started section', () => {
      const getStartedBtn = document.querySelector('.hero-cta .btn-primary');
      expect(getStartedBtn.getAttribute('href')).toBe('#getting-started');
    });

    it('should have a GitHub CTA button', () => {
      const githubBtn = document.querySelector('.hero-cta .btn-secondary');
      expect(githubBtn).not.toBeNull();
      expect(githubBtn.textContent.trim()).toContain('GitHub');
    });

    it('should have GitHub button link to external repository', () => {
      const githubBtn = document.querySelector('.hero-cta .btn-secondary');
      const href = githubBtn.getAttribute('href');
      expect(href).toContain('github.com');
      expect(href).toContain('mirdb');
    });

    it('should open GitHub link in new tab', () => {
      const githubBtn = document.querySelector('.hero-cta .btn-secondary');
      expect(githubBtn.getAttribute('target')).toBe('_blank');
      expect(githubBtn.getAttribute('rel')).toContain('noopener');
    });

    it('should have aria-hidden on decorative GitHub icon', () => {
      const icon = document.querySelector('.hero-cta .btn-secondary svg');
      expect(icon).not.toBeNull();
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    });
  });
});

// ===== Scenario 2: Features Section Tests =====

describe('Features Section - Key Capabilities', () => {
  describe('Test Case 1: Features section contains all required feature cards', () => {
    it('should have a features section with id="features"', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
    });

    it('should contain 6 feature cards', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBe(6);
    });

    it('should have feature card for Memcached protocol compatibility', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const memcachedFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('memcached')
      );
      expect(memcachedFeature).not.toBeUndefined();
    });

    it('should have feature card for Persistent storage', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const persistentFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('persistent')
      );
      expect(persistentFeature).not.toBeUndefined();
    });

    it('should have feature card for LSM tree architecture', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const lsmFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('lsm')
      );
      expect(lsmFeature).not.toBeUndefined();
    });

    it('should have feature card for Rust performance/safety', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const rustFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('rust')
      );
      expect(rustFeature).not.toBeUndefined();
    });

    it('should have feature card for Compaction support', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const compactionFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('compaction')
      );
      expect(compactionFeature).not.toBeUndefined();
    });

    it('should have feature card for WAL durability', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const walFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('wal')
      );
      expect(walFeature).not.toBeUndefined();
    });
  });

  describe('Test Case 2: Feature card structure validation', () => {
    it('each feature card should have an icon/visual element', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        const icon = card.querySelector('.feature-icon');
        expect(icon, `Feature card ${index + 1} should have an icon`).not.toBeNull();
        // Verify icon has visual content (SVG)
        const svg = icon.querySelector('svg');
        expect(svg, `Feature card ${index + 1} icon should contain SVG`).not.toBeNull();
      });
    });

    it('each feature card should have a title', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        const title = card.querySelector('.feature-title');
        expect(title, `Feature card ${index + 1} should have a title`).not.toBeNull();
        expect(title.textContent.trim().length, `Feature card ${index + 1} title should not be empty`).toBeGreaterThan(0);
      });
    });

    it('each feature card should have a description', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        const description = card.querySelector('.feature-description');
        expect(description, `Feature card ${index + 1} should have a description`).not.toBeNull();
        expect(description.textContent.trim().length, `Feature card ${index + 1} description should not be empty`).toBeGreaterThan(0);
      });
    });

    it('feature cards should use semantic article elements', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        expect(card.tagName.toLowerCase(), `Feature card ${index + 1} should be an article element`).toBe('article');
      });
    });

    it('feature titles should use h3 elements for proper heading hierarchy', () => {
      const featuresSection = document.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-title');

      titles.forEach((title, index) => {
        expect(title.tagName.toLowerCase(), `Feature title ${index + 1} should be an h3 element`).toBe('h3');
      });
    });
  });
});

describe('Features Section - Grid Layout Structure', () => {
  it('should have a features-grid container', () => {
    const featuresSection = document.getElementById('features');
    const grid = featuresSection.querySelector('.features-grid');
    expect(grid).not.toBeNull();
  });

  it('all feature cards should be inside the grid container', () => {
    const featuresSection = document.getElementById('features');
    const grid = featuresSection.querySelector('.features-grid');
    const featureCards = grid.querySelectorAll('.feature-card');
    expect(featureCards.length).toBe(6);
  });
});

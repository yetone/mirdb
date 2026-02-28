/**
 * Features Component Unit Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests:
 * - 3-5 feature cards render
 * - Each card has title and description
 * - Icons present on cards
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { loadDocument, getDocument } from '../../setup.js';

describe('Features Section Display', () => {
  let document;
  let dom;

  beforeEach(() => {
    dom = loadDocument();
    document = getDocument(dom);
  });

  describe('Test Case 1: Features section contains between 3 and 5 feature cards', () => {
    it('should render the features section', () => {
      const featuresSection = document.querySelector('.features');
      expect(featuresSection).not.toBeNull();
    });

    it('should have a features title', () => {
      const title = document.querySelector('.features__title');
      expect(title).not.toBeNull();
      expect(title.textContent).toBe('Features');
    });

    it('should contain between 3 and 5 feature cards', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
      expect(featureCards.length).toBeLessThanOrEqual(5);
    });

    it('should have exactly 4 feature cards for MirDB', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBe(4);
    });
  });

  describe('Test Case 2: Each feature card has a title and description', () => {
    it('should have a title (h3 heading) in each feature card', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const title = card.querySelector('.feature-card__title');
        expect(title).not.toBeNull();
        expect(title.tagName.toLowerCase()).toBe('h3');
        expect(title.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have a description (paragraph) in each feature card', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const description = card.querySelector('.feature-card__description');
        expect(description).not.toBeNull();
        expect(description.tagName.toLowerCase()).toBe('p');
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have unique titles for each feature card', () => {
      const titles = Array.from(document.querySelectorAll('.feature-card__title'))
        .map(el => el.textContent.trim());
      const uniqueTitles = new Set(titles);
      expect(uniqueTitles.size).toBe(titles.length);
    });
  });

  describe('Test Case 3: Each feature card displays an icon', () => {
    it('should have an icon container in each feature card', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const iconContainer = card.querySelector('.feature-card__icon');
        expect(iconContainer).not.toBeNull();
      });
    });

    it('should have an SVG icon in each feature card', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const icon = card.querySelector('.feature-card__icon svg');
        expect(icon).not.toBeNull();
        expect(icon.tagName.toLowerCase()).toBe('svg');
      });
    });

    it('should have aria-hidden on icon containers for accessibility', () => {
      const iconContainers = document.querySelectorAll('.feature-card__icon');
      iconContainers.forEach((container) => {
        expect(container.getAttribute('aria-hidden')).toBe('true');
      });
    });
  });

  describe('Features section accessibility', () => {
    it('should have an aria-labelledby attribute pointing to the section title', () => {
      const featuresSection = document.querySelector('.features');
      expect(featuresSection.getAttribute('aria-labelledby')).toBe('features-title');
    });

    it('should have proper id on features section title', () => {
      const title = document.getElementById('features-title');
      expect(title).not.toBeNull();
      expect(title.textContent).toBe('Features');
    });

    it('should use article elements for feature cards for semantic structure', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        expect(card.tagName.toLowerCase()).toBe('article');
      });
    });
  });

  describe('Features grid structure', () => {
    it('should have a features grid container', () => {
      const grid = document.querySelector('.features__grid');
      expect(grid).not.toBeNull();
    });

    it('should contain all feature cards within the grid', () => {
      const grid = document.querySelector('.features__grid');
      const cardsInGrid = grid.querySelectorAll('.feature-card');
      const allCards = document.querySelectorAll('.feature-card');
      expect(cardsInGrid.length).toBe(allCards.length);
    });
  });
});

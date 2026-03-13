/**
 * Lazy Loading Unit Tests
 * Owner: Scenario 11 - Performance Requirements
 *
 * Test case 5: Below-fold images use lazy loading
 * Verifies that images that appear below the fold have the
 * loading="lazy" attribute for better performance.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { StatusBadges } from '../../../src/components/sections/StatusBadges';

describe('Lazy Loading Performance', () => {
  describe('StatusBadges Component', () => {
    it('should have lazy loading on badge images', () => {
      render(<StatusBadges />);

      // Find all images in the status badges section
      const images = screen.getAllByRole('img');

      // All images in StatusBadges are below the fold, so they should have lazy loading
      images.forEach((img) => {
        expect(img).toHaveAttribute('loading', 'lazy');
      });
    });

    it('should render CircleCI badge with lazy loading', () => {
      render(<StatusBadges />);

      const badge = screen.getByAltText('CircleCI Build Status');
      expect(badge).toBeInTheDocument();
      expect(badge).toHaveAttribute('loading', 'lazy');
    });
  });

  describe('Below-Fold Image Requirements', () => {
    it('images below viewport should use native lazy loading', () => {
      // This test verifies the concept that below-fold images
      // should use the native loading="lazy" attribute
      // for better Core Web Vitals (LCP, FCP) scores

      // Render the StatusBadges component
      render(<StatusBadges />);

      // Get all images
      const images = screen.getAllByRole('img');

      // Verify each image has the lazy loading attribute
      expect(images.length).toBeGreaterThan(0);
      images.forEach((img) => {
        const loadingAttr = img.getAttribute('loading');
        expect(loadingAttr).toBe('lazy');
      });
    });
  });
});

/**
 * FeaturesSection component tests.
 * Owner: Scenario 3 - Features Section Display
 *
 * Test coverage:
 * - 4 feature cards are displayed
 * - Each card has icon, heading, and description
 * - Cards use GlassMorphismCard wrapper
 * - Feature content matches PRD specifications
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { FeaturesSection } from '@/components/home/FeaturesSection';
import { renderWithProviders } from '../../utils/renderWithProviders';

describe('FeaturesSection', () => {
  describe('Test Case 1: Render FeaturesSection component', () => {
    it('should display 4 feature cards (URL Shortening, Click Analytics, Secure Management, Custom Short Codes)', () => {
      renderWithProviders(<FeaturesSection />);

      // Verify the features section is rendered
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Verify all 4 feature cards are displayed
      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
      const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics');
      const secureManagementCard = screen.getByTestId('feature-card-secure-management');
      const customShortCodesCard = screen.getByTestId('feature-card-custom-short-codes');

      expect(urlShorteningCard).toBeInTheDocument();
      expect(clickAnalyticsCard).toBeInTheDocument();
      expect(secureManagementCard).toBeInTheDocument();
      expect(customShortCodesCard).toBeInTheDocument();

      // Count total feature cards
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards).toHaveLength(4);
    });
  });

  describe('Test Case 2: Check feature card structure', () => {
    it('should have each card contain an icon SVG, heading, and description text', () => {
      renderWithProviders(<FeaturesSection />);

      const featureIds = ['url-shortening', 'click-analytics', 'secure-management', 'custom-short-codes'];

      featureIds.forEach((featureId) => {
        // Check icon exists
        const icon = screen.getByTestId(`feature-icon-${featureId}`);
        expect(icon).toBeInTheDocument();

        // Check icon contains SVG
        const svg = icon.querySelector('svg');
        expect(svg).toBeInTheDocument();

        // Check heading exists
        const title = screen.getByTestId(`feature-title-${featureId}`);
        expect(title).toBeInTheDocument();
        expect(title.tagName.toLowerCase()).toBe('h3');

        // Check description exists
        const description = screen.getByTestId(`feature-description-${featureId}`);
        expect(description).toBeInTheDocument();
        expect(description.tagName.toLowerCase()).toBe('p');
      });
    });
  });

  describe('Test Case 3: Verify feature card styling', () => {
    it('should use GlassMorphismCard component with backdrop blur effect', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        // GlassMorphismCard applies these classes
        expect(card).toHaveClass('backdrop-blur-md');
        expect(card).toHaveClass('bg-base-100/50');
        expect(card).toHaveClass('border');
        expect(card).toHaveClass('rounded-xl');
      });
    });
  });

  describe('Test Case 4: Check URL Shortening feature card', () => {
    it('should display URL Shortening with description about transforming long URLs', () => {
      renderWithProviders(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-url-shortening');
      expect(title).toHaveTextContent('URL Shortening');

      const description = screen.getByTestId('feature-description-url-shortening');
      expect(description.textContent).toContain('Transform');
      expect(description.textContent?.toLowerCase()).toContain('long');
      expect(description.textContent?.toLowerCase()).toContain('url');
    });
  });

  describe('Test Case 5: Check Click Analytics feature card', () => {
    it('should display Click Analytics with description about detailed analytics tracking', () => {
      renderWithProviders(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-click-analytics');
      expect(title).toHaveTextContent('Click Analytics');

      const description = screen.getByTestId('feature-description-click-analytics');
      expect(description.textContent?.toLowerCase()).toContain('detailed');
      expect(description.textContent?.toLowerCase()).toContain('analytics');
    });
  });
});

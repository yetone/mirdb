/**
 * FeaturesSection Component Unit Tests
 *
 * Tests for Scenario 2: Features Section Display
 * Verifies that the features section showcases key capabilities
 * (URL shortening, analytics, tracking) as specified in REQ-3.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeaturesSection } from '@/components/homepage/FeaturesSection';

describe('FeaturesSection', () => {
  describe('Test Case 1: Render FeaturesSection component', () => {
    it('renders four feature cards with distinct content', () => {
      render(<FeaturesSection />);

      // Check that all four feature cards are rendered
      const urlShorteningCard = screen.getByText('URL Shortening');
      const clickAnalyticsCard = screen.getByText('Click Analytics');
      const geographicInsightsCard = screen.getByText('Geographic Insights');
      const shareStatisticsCard = screen.getByText('Share Statistics');

      expect(urlShorteningCard).toBeInTheDocument();
      expect(clickAnalyticsCard).toBeInTheDocument();
      expect(geographicInsightsCard).toBeInTheDocument();
      expect(shareStatisticsCard).toBeInTheDocument();
    });

    it('renders exactly four feature cards', () => {
      render(<FeaturesSection />);

      // Each card has a heading role (h3) for the title
      const featureTitles = screen.getAllByRole('heading', { level: 3 });
      expect(featureTitles).toHaveLength(4);
    });
  });

  describe('Test Case 2: Query for URL Shortening feature card', () => {
    it('contains icon, title, and description about creating short links', () => {
      render(<FeaturesSection />);

      // Check for title
      const title = screen.getByText('URL Shortening');
      expect(title).toBeInTheDocument();

      // Check for description about creating short links
      const description = screen.getByText(/create short.*memorable links/i);
      expect(description).toBeInTheDocument();

      // Check for icon presence (the card should have an icon element)
      const card = title.closest('[data-testid="feature-card-url-shortening"]');
      expect(card).toBeInTheDocument();

      const icon = card?.querySelector('[data-testid="feature-icon"]');
      expect(icon).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Query for Click Analytics feature card', () => {
    it('contains icon, title, and description about tracking clicks', () => {
      render(<FeaturesSection />);

      // Check for title
      const title = screen.getByText('Click Analytics');
      expect(title).toBeInTheDocument();

      // Check for description about tracking clicks
      const description = screen.getByText(/track.*clicks.*metrics/i);
      expect(description).toBeInTheDocument();

      // Check for icon presence
      const card = title.closest('[data-testid="feature-card-click-analytics"]');
      expect(card).toBeInTheDocument();

      const icon = card?.querySelector('[data-testid="feature-icon"]');
      expect(icon).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Query for Geographic Insights feature card', () => {
    it('contains icon, title, and description about audience location', () => {
      render(<FeaturesSection />);

      // Check for title
      const title = screen.getByText('Geographic Insights');
      expect(title).toBeInTheDocument();

      // Check for description about audience location
      const description = screen.getByText(/where.*audience.*located/i);
      expect(description).toBeInTheDocument();

      // Check for icon presence
      const card = title.closest('[data-testid="feature-card-geographic-insights"]');
      expect(card).toBeInTheDocument();

      const icon = card?.querySelector('[data-testid="feature-icon"]');
      expect(icon).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Query for Share Statistics feature card', () => {
    it('contains icon, title, and description about public share links', () => {
      render(<FeaturesSection />);

      // Check for title
      const title = screen.getByText('Share Statistics');
      expect(title).toBeInTheDocument();

      // Check for description about public share links
      const description = screen.getByText(/public.*share.*links.*stats/i);
      expect(description).toBeInTheDocument();

      // Check for icon presence
      const card = title.closest('[data-testid="feature-card-share-statistics"]');
      expect(card).toBeInTheDocument();

      const icon = card?.querySelector('[data-testid="feature-icon"]');
      expect(icon).toBeInTheDocument();
    });
  });

  describe('Test Case 6: Inspect feature card DOM structure', () => {
    it('uses GlassMorphismCard component or equivalent glass-effect styling classes', () => {
      render(<FeaturesSection />);

      // Check that cards have glass-effect styling
      const urlCard = screen.getByTestId('feature-card-url-shortening');
      const clickCard = screen.getByTestId('feature-card-click-analytics');
      const geoCard = screen.getByTestId('feature-card-geographic-insights');
      const shareCard = screen.getByTestId('feature-card-share-statistics');

      // Verify glass-effect styling classes are present
      [urlCard, clickCard, geoCard, shareCard].forEach((card) => {
        // Check for backdrop-blur which is characteristic of glass morphism
        expect(card.className).toMatch(/backdrop-blur/);
        // Check for semi-transparent background
        expect(card.className).toMatch(/bg-base-100/);
        // Check for rounded corners
        expect(card.className).toMatch(/rounded/);
      });
    });

    it('renders cards in a grid layout', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toBeInTheDocument();

      // Check for grid layout
      const grid = section.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();
    });

    it('includes section heading', () => {
      render(<FeaturesSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveTextContent(/features/i);
    });
  });
});

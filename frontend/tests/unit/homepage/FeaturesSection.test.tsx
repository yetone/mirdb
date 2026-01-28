/**
 * Unit tests for FeaturesSection component
 * Owner: Scenario 3 - Features Section Display
 *
 * Tests cover:
 * - Component rendering without errors
 * - Correct number of feature cards
 * - Presence of URL Shortening, Analytics, and Link Management features
 * - Glass morphism styling
 * - Icon presence in each feature card
 *
 * Requirements: REQ-2, REQ-4, US-4
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection';

describe('FeaturesSection', () => {
  // Test Case 1: Component renders without errors
  it('renders without errors', () => {
    expect(() => render(<FeaturesSection />)).not.toThrow();
  });

  // Test Case 1 continued: Verifies section element exists
  it('renders the features section container', () => {
    render(<FeaturesSection />);
    const section = screen.getByTestId('features-section');
    expect(section).toBeInTheDocument();
  });

  // Test Case 2: At least 3 FeatureCard components are rendered
  it('renders at least 3 feature cards', () => {
    render(<FeaturesSection />);
    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards.length).toBeGreaterThanOrEqual(3);
  });

  // Test Case 2 continued: Verify the grid container exists
  it('renders the feature cards grid', () => {
    render(<FeaturesSection />);
    const grid = screen.getByTestId('feature-cards-grid');
    expect(grid).toBeInTheDocument();
  });

  // Test Case 3: Feature card with title containing 'shorten' or 'URL' exists
  it('renders URL Shortening feature card', () => {
    render(<FeaturesSection />);
    // Check that at least one element contains 'url' or 'shorten' (case insensitive)
    const urlElements = screen.getAllByText(/url|shorten/i);
    expect(urlElements.length).toBeGreaterThan(0);
  });

  // Test Case 3 continued: More specific check for URL Shortening feature
  it('has a feature card with URL Shortening title', () => {
    render(<FeaturesSection />);
    const featureTitle = screen.getByText('URL Shortening');
    expect(featureTitle).toBeInTheDocument();
  });

  // Test Case 4: Feature card with title containing 'analytics' or 'track' exists
  it('renders Analytics feature card', () => {
    render(<FeaturesSection />);
    // Check that at least one element contains 'analytics' or 'track' (case insensitive)
    const analyticsElements = screen.getAllByText(/analytics|track/i);
    expect(analyticsElements.length).toBeGreaterThan(0);
  });

  // Test Case 4 continued: More specific check for Analytics feature
  it('has a feature card with Click Analytics title', () => {
    render(<FeaturesSection />);
    const featureTitle = screen.getByText('Click Analytics');
    expect(featureTitle).toBeInTheDocument();
  });

  // Test Case 5: Feature card with title containing 'manage' or 'organize' exists
  it('renders Link Management feature card', () => {
    render(<FeaturesSection />);
    // Check that at least one element contains 'manage' or 'organize' (case insensitive)
    const managementElements = screen.getAllByText(/manage|organize/i);
    expect(managementElements.length).toBeGreaterThan(0);
  });

  // Test Case 5 continued: More specific check for Link Management feature
  it('has a feature card with Link Management title', () => {
    render(<FeaturesSection />);
    const featureTitle = screen.getByText('Link Management');
    expect(featureTitle).toBeInTheDocument();
  });

  // Test Case 7: Cards have glass morphism visual style
  it('feature cards use GlassMorphismCard styling', () => {
    render(<FeaturesSection />);
    const glassCards = screen.getAllByTestId('glass-morphism-card');
    expect(glassCards.length).toBeGreaterThanOrEqual(3);
  });

  // Test Case 8: Each feature card has a visual icon element
  it('each feature card has an icon element', () => {
    render(<FeaturesSection />);
    const icons = screen.getAllByTestId('feature-icon');
    const featureCards = screen.getAllByTestId('feature-card');
    expect(icons.length).toBe(featureCards.length);
  });

  // Additional tests for completeness

  it('renders section heading', () => {
    render(<FeaturesSection />);
    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Powerful Features');
  });

  it('has correct ARIA labeling', () => {
    render(<FeaturesSection />);
    const section = screen.getByRole('region', { name: /features/i });
    expect(section).toBeInTheDocument();
  });

  it('renders 4 default features', () => {
    render(<FeaturesSection />);
    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(4);
  });

  it('accepts custom features prop', () => {
    const customFeatures = [
      {
        id: 'custom-1',
        title: 'Custom Feature',
        description: 'Custom description',
        icon: <span>Custom Icon</span>,
      },
    ];
    render(<FeaturesSection features={customFeatures} />);
    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(1);
    expect(screen.getByText('Custom Feature')).toBeInTheDocument();
  });

  it('renders Share Statistics feature', () => {
    render(<FeaturesSection />);
    const shareFeature = screen.getByText('Share Statistics');
    expect(shareFeature).toBeInTheDocument();
  });

  it('each feature card has a description', () => {
    render(<FeaturesSection />);
    const descriptions = screen.getAllByTestId('feature-description');
    expect(descriptions.length).toBeGreaterThanOrEqual(3);
    descriptions.forEach((desc) => {
      expect(desc.textContent?.length).toBeGreaterThan(0);
    });
  });

  it('each feature card has a title', () => {
    render(<FeaturesSection />);
    const titles = screen.getAllByTestId('feature-title');
    expect(titles.length).toBeGreaterThanOrEqual(3);
    titles.forEach((title) => {
      expect(title.textContent?.length).toBeGreaterThan(0);
    });
  });
});

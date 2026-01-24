/**
 * Unit Tests for FeaturesSection Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests verify:
 * 1. Features section exists with at least 3 feature cards
 * 2. URL Shortening feature card exists with proper content
 * 3. Click Analytics feature card exists with proper content
 * 4. Share Stats feature card exists with proper content
 * 5. Each feature card displays an icon
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { FeaturesSection } from '../../../src/components/landing/FeaturesSection';
import { renderWithProviders } from './test-utils';

describe('FeaturesSection', () => {
  // Test Case 1: Features section exists with at least 3 feature cards
  it('renders features section with at least 3 feature cards', () => {
    renderWithProviders(<FeaturesSection />);

    // Check that features section exists
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toBeInTheDocument();

    // Check that there are at least 3 feature cards
    const featureCards = screen.getAllByTestId(/^feature-card-/);
    expect(featureCards.length).toBeGreaterThanOrEqual(3);
  });

  // Test Case 2: URL Shortening feature card exists
  it('displays URL Shortening feature card with proper content', () => {
    renderWithProviders(<FeaturesSection />);

    // Check for URL Shortening feature card
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
    expect(urlShorteningCard).toBeInTheDocument();

    // Check for title
    const title = screen.getByText('URL Shortening');
    expect(title).toBeInTheDocument();

    // Check for description about creating short links
    const description = screen.getByText(/create short.*links/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 3: Click Analytics feature card exists
  it('displays Click Analytics feature card with proper content', () => {
    renderWithProviders(<FeaturesSection />);

    // Check for Click Analytics feature card
    const analyticsCard = screen.getByTestId('feature-card-click-analytics');
    expect(analyticsCard).toBeInTheDocument();

    // Check for title
    const title = screen.getByText('Click Analytics');
    expect(title).toBeInTheDocument();

    // Check for description about tracking clicks
    const description = screen.getByText(/track.*clicks/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 4: Share Stats feature card exists
  it('displays Share Stats feature card with proper content', () => {
    renderWithProviders(<FeaturesSection />);

    // Check for Share Stats feature card
    const shareStatsCard = screen.getByTestId('feature-card-share-stats');
    expect(shareStatsCard).toBeInTheDocument();

    // Check for title
    const title = screen.getByText('Share Stats');
    expect(title).toBeInTheDocument();

    // Check for description about public share links
    const description = screen.getByText(/public.*share.*links/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 5: Each feature card has an icon
  it('displays an icon for each feature card', () => {
    renderWithProviders(<FeaturesSection />);

    // Check that all feature icons are present
    const linkIcon = screen.getByTestId('feature-icon-link');
    expect(linkIcon).toBeInTheDocument();

    const chartIcon = screen.getByTestId('feature-icon-chart');
    expect(chartIcon).toBeInTheDocument();

    const shareIcon = screen.getByTestId('feature-icon-share');
    expect(shareIcon).toBeInTheDocument();

    const dashboardIcon = screen.getByTestId('feature-icon-dashboard');
    expect(dashboardIcon).toBeInTheDocument();

    // Verify icons contain SVG elements
    const svgElements = document.querySelectorAll('[data-testid^="feature-icon-"] svg');
    expect(svgElements.length).toBeGreaterThanOrEqual(3);
  });

  // Additional test: Verify section has proper accessibility
  it('has proper accessibility attributes', () => {
    renderWithProviders(<FeaturesSection />);

    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toHaveAttribute('id', 'features-heading');
    expect(heading).toHaveTextContent('Powerful Features');
  });

  // Test with custom features
  it('renders custom features when provided', () => {
    const customFeatures = [
      { icon: 'link', title: 'Custom Feature 1', description: 'Custom description 1' },
      { icon: 'chart', title: 'Custom Feature 2', description: 'Custom description 2' },
      { icon: 'share', title: 'Custom Feature 3', description: 'Custom description 3' },
    ];

    renderWithProviders(<FeaturesSection features={customFeatures} />);

    expect(screen.getByText('Custom Feature 1')).toBeInTheDocument();
    expect(screen.getByText('Custom Feature 2')).toBeInTheDocument();
    expect(screen.getByText('Custom Feature 3')).toBeInTheDocument();
  });
});

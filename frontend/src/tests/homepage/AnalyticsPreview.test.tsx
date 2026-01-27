/**
 * AnalyticsPreview Component Tests
 * Owner: Scenario 3 - Analytics Preview Implementation
 *
 * Tests for:
 * - Component rendering
 * - Section heading visibility
 * - Chart visualizations
 * - Statistics display
 * - GlassMorphismCard usage
 * - Referrer information
 * - Geographic data
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from './test-utils';
import { AnalyticsPreview } from '../../components/homepage/AnalyticsPreview';

describe('AnalyticsPreview Component', () => {
  // Test Case 1: Component renders with analytics content visible
  it('renders with analytics content visible', () => {
    render(<AnalyticsPreview />);

    // Check that the section is rendered
    const section = screen.getByTestId('analytics-preview-section');
    expect(section).toBeInTheDocument();
    expect(section).toBeVisible();
  });

  // Test Case 2: Heading with 'Analytics' text is visible
  it('displays section heading with Analytics text', () => {
    render(<AnalyticsPreview />);

    const heading = screen.getByTestId('analytics-heading');
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent(/analytics/i);
    expect(heading).toHaveClass('text-3xl', 'md:text-4xl', 'font-bold');
  });

  // Test Case 3: At least one Recharts chart component is rendered
  it('renders Recharts chart visualizations', () => {
    render(<AnalyticsPreview />);

    // Check for the clicks over time chart
    const clicksChart = screen.getByTestId('clicks-chart');
    expect(clicksChart).toBeInTheDocument();

    // Check for the referrer chart
    const referrerChart = screen.getByTestId('referrer-chart');
    expect(referrerChart).toBeInTheDocument();

    // Check for location chart
    const locationChart = screen.getByTestId('location-chart');
    expect(locationChart).toBeInTheDocument();

    // Check for browser chart
    const browserChart = screen.getByTestId('browser-chart');
    expect(browserChart).toBeInTheDocument();
  });

  // Test Case 4: Sample statistics (clicks, locations, etc.) are displayed
  it('displays sample statistics', () => {
    render(<AnalyticsPreview />);

    // Check statistics display container
    const statsDisplay = screen.getByTestId('statistics-display');
    expect(statsDisplay).toBeInTheDocument();

    // Check for specific statistics
    expect(screen.getByText('Total Clicks')).toBeInTheDocument();
    expect(screen.getByText('12,847')).toBeInTheDocument();

    expect(screen.getByText('Unique Visitors')).toBeInTheDocument();
    expect(screen.getByText('8,392')).toBeInTheDocument();

    expect(screen.getByText('Countries')).toBeInTheDocument();
    expect(screen.getByText('42')).toBeInTheDocument();
  });

  // Test Case 5: Content is wrapped in GlassMorphismCard component
  it('wraps content in GlassMorphismCard component', () => {
    render(<AnalyticsPreview />);

    // Check for the glass morphism card
    const glassCard = screen.getByTestId('glass-morphism-card');
    expect(glassCard).toBeInTheDocument();

    // Verify it has the backdrop-blur class which is part of glass morphism styling
    expect(glassCard).toHaveClass('backdrop-blur-md');
  });

  // Test Case 6: Referrer or traffic source information is displayed
  it('displays referrer/traffic source information', () => {
    render(<AnalyticsPreview />);

    // Check for the traffic sources section
    expect(screen.getByText('Traffic Sources')).toBeInTheDocument();

    // Check for referral sources stat
    expect(screen.getByText('Referral Sources')).toBeInTheDocument();
    expect(screen.getByText('15')).toBeInTheDocument();

    // The referrer chart should be present
    const referrerChart = screen.getByTestId('referrer-chart');
    expect(referrerChart).toBeInTheDocument();
  });

  // Test Case 7: Location or geographic analytics data is shown
  it('displays geographic/location analytics data', () => {
    render(<AnalyticsPreview />);

    // Check for the Top Locations section
    expect(screen.getByText('Top Locations')).toBeInTheDocument();

    // Check for Countries stat
    expect(screen.getByText('Countries')).toBeInTheDocument();

    // The location chart should be present
    const locationChart = screen.getByTestId('location-chart');
    expect(locationChart).toBeInTheDocument();
  });

  // Additional tests for comprehensive coverage
  it('has proper accessibility attributes', () => {
    render(<AnalyticsPreview />);

    const section = screen.getByTestId('analytics-preview-section');
    expect(section).toHaveAttribute('aria-labelledby', 'analytics-heading');

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toHaveAttribute('id', 'analytics-heading');
  });

  it('displays browser breakdown information', () => {
    render(<AnalyticsPreview />);

    expect(screen.getByText('Browser Breakdown')).toBeInTheDocument();
    const browserChart = screen.getByTestId('browser-chart');
    expect(browserChart).toBeInTheDocument();
  });

  it('displays clicks over time chart', () => {
    render(<AnalyticsPreview />);

    expect(screen.getByText('Clicks Over Time')).toBeInTheDocument();
    const clicksChart = screen.getByTestId('clicks-chart');
    expect(clicksChart).toBeInTheDocument();
  });

  it('renders descriptive text about analytics capabilities', () => {
    render(<AnalyticsPreview />);

    expect(
      screen.getByText(/detailed insights into your link performance/i)
    ).toBeInTheDocument();
  });
});

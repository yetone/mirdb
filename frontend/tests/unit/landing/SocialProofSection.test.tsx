/**
 * Unit Tests for SocialProofSection Component
 * Owner: Scenario 5 - Social Proof Section
 *
 * Tests verify:
 * 1. Social proof section exists with statistics or trust indicators
 * 2. Usage statistics are displayed (URLs shortened, clicks tracked)
 * 3. Trust indicators (security badge, uptime commitment) are present
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { SocialProofSection } from '../../../src/components/landing/SocialProofSection';
import { renderWithProviders } from './test-utils';

describe('SocialProofSection', () => {
  // Test Case 1: Social proof section exists with statistics or trust indicators
  it('renders social proof section with statistics and trust indicators', () => {
    renderWithProviders(<SocialProofSection />);

    // Check that social proof section exists
    const socialProofSection = screen.getByTestId('social-proof-section');
    expect(socialProofSection).toBeInTheDocument();

    // Check for section heading
    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toHaveTextContent('Trusted by Thousands');

    // Check that statistics grid exists
    const statisticsGrid = screen.getByTestId('statistics-grid');
    expect(statisticsGrid).toBeInTheDocument();

    // Check that trust indicators section exists
    const trustIndicators = screen.getByTestId('trust-indicators');
    expect(trustIndicators).toBeInTheDocument();
  });

  // Test Case 2: Usage statistics are displayed
  it('displays usage statistics like URLs shortened and clicks tracked', () => {
    renderWithProviders(<SocialProofSection />);

    // Check for URLs shortened statistic
    const urlsShortened = screen.getByTestId('statistic-urls-shortened');
    expect(urlsShortened).toBeInTheDocument();
    expect(urlsShortened).toHaveTextContent('URLs Shortened');
    expect(urlsShortened).toHaveTextContent('10K+');

    // Check for clicks tracked statistic
    const clicksTracked = screen.getByTestId('statistic-clicks-tracked');
    expect(clicksTracked).toBeInTheDocument();
    expect(clicksTracked).toHaveTextContent('Clicks Tracked');
    expect(clicksTracked).toHaveTextContent('500K+');

    // Check for active users statistic
    const activeUsers = screen.getByTestId('statistic-active-users');
    expect(activeUsers).toBeInTheDocument();
    expect(activeUsers).toHaveTextContent('Active Users');
    expect(activeUsers).toHaveTextContent('1K+');
  });

  // Test Case 3: Trust indicators are present
  it('displays trust indicators such as security badge and uptime commitment', () => {
    renderWithProviders(<SocialProofSection />);

    // Check for security trust indicator
    const securityIndicator = screen.getByTestId('trust-indicator-security');
    expect(securityIndicator).toBeInTheDocument();
    expect(securityIndicator).toHaveTextContent('Secure & Private');
    expect(securityIndicator).toHaveTextContent(/encryption/i);

    // Check for uptime trust indicator
    const uptimeIndicator = screen.getByTestId('trust-indicator-uptime');
    expect(uptimeIndicator).toBeInTheDocument();
    expect(uptimeIndicator).toHaveTextContent('99.9% Uptime');
    expect(uptimeIndicator).toHaveTextContent(/reliable/i);
  });

  // Additional test: Section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    renderWithProviders(<SocialProofSection />);

    const socialProofSection = screen.getByTestId('social-proof-section');
    expect(socialProofSection).toHaveAttribute('aria-labelledby', 'social-proof-heading');

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toHaveAttribute('id', 'social-proof-heading');
  });

  // Test with custom statistics
  it('renders custom statistics when provided', () => {
    const customStats = [
      { value: '100K+', label: 'Custom Stat 1' },
      { value: '1M+', label: 'Custom Stat 2' },
    ];

    renderWithProviders(<SocialProofSection statistics={customStats} />);

    expect(screen.getByText('100K+')).toBeInTheDocument();
    expect(screen.getByText('Custom Stat 1')).toBeInTheDocument();
    expect(screen.getByText('1M+')).toBeInTheDocument();
    expect(screen.getByText('Custom Stat 2')).toBeInTheDocument();
  });

  // Test testimonials section when enabled
  it('renders testimonials section when showTestimonials is true', () => {
    renderWithProviders(<SocialProofSection showTestimonials={true} />);

    const testimonialsSection = screen.getByTestId('testimonials-section');
    expect(testimonialsSection).toBeInTheDocument();

    // Check for testimonial heading
    expect(screen.getByText('What Our Users Say')).toBeInTheDocument();
  });

  // Test testimonials section is hidden by default
  it('does not render testimonials section by default', () => {
    renderWithProviders(<SocialProofSection />);

    const testimonialsSection = screen.queryByTestId('testimonials-section');
    expect(testimonialsSection).not.toBeInTheDocument();
  });

  // Test that statistics have proper structure
  it('renders statistics with proper value and label structure', () => {
    renderWithProviders(<SocialProofSection />);

    const statisticValues = screen.getAllByTestId('statistic-value');
    const statisticLabels = screen.getAllByTestId('statistic-label');

    expect(statisticValues.length).toBe(3);
    expect(statisticLabels.length).toBe(3);

    // Verify first statistic structure
    expect(statisticValues[0]).toHaveTextContent('10K+');
    expect(statisticLabels[0]).toHaveTextContent('URLs Shortened');
  });
});

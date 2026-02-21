/**
 * TrustSection component tests.
 * Owner: Scenario 12 - Social Proof and Trust Indicators
 *
 * Test coverage:
 * - Trust section displays if implemented
 * - Metrics are properly formatted
 * - Trust indicators are visually distinct
 */

import React from 'react';
import { screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { TrustSection } from '@/components/home/TrustSection';
import { renderWithProviders } from '../../utils/renderWithProviders';

describe('TrustSection', () => {
  describe('Trust Section Display', () => {
    it('renders the trust section with correct testid', () => {
      renderWithProviders(<TrustSection />);

      const section = screen.getByTestId('trust-section');
      expect(section).toBeInTheDocument();
    });

    it('displays the trust section heading', () => {
      renderWithProviders(<TrustSection />);

      const heading = screen.getByTestId('trust-heading');
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent('Trusted by Thousands');
    });

    it('displays the trust section subheading', () => {
      renderWithProviders(<TrustSection />);

      const subheading = screen.getByTestId('trust-subheading');
      expect(subheading).toBeInTheDocument();
      expect(subheading).toHaveTextContent('Join thousands of users');
    });

    it('displays all four trust metrics', () => {
      renderWithProviders(<TrustSection />);

      expect(screen.getByTestId('trust-metric-active-users')).toBeInTheDocument();
      expect(screen.getByTestId('trust-metric-links-created')).toBeInTheDocument();
      expect(screen.getByTestId('trust-metric-clicks-tracked')).toBeInTheDocument();
      expect(screen.getByTestId('trust-metric-uptime')).toBeInTheDocument();
    });
  });

  describe('Metrics Display and Formatting', () => {
    it('displays user count metric with properly formatted number', () => {
      renderWithProviders(<TrustSection />);

      const value = screen.getByTestId('metric-value-active-users');
      expect(value).toBeInTheDocument();
      expect(value).toHaveTextContent('10,000+');
    });

    it('displays links created metric with properly formatted number', () => {
      renderWithProviders(<TrustSection />);

      const value = screen.getByTestId('metric-value-links-created');
      expect(value).toBeInTheDocument();
      expect(value).toHaveTextContent('1,000,000+');
    });

    it('displays clicks tracked metric with properly formatted number', () => {
      renderWithProviders(<TrustSection />);

      const value = screen.getByTestId('metric-value-clicks-tracked');
      expect(value).toBeInTheDocument();
      expect(value).toHaveTextContent('50,000,000+');
    });

    it('displays uptime percentage metric', () => {
      renderWithProviders(<TrustSection />);

      const value = screen.getByTestId('metric-value-uptime');
      expect(value).toBeInTheDocument();
      expect(value).toHaveTextContent('99.9%');
    });

    it('displays metric labels correctly', () => {
      renderWithProviders(<TrustSection />);

      expect(screen.getByTestId('metric-label-active-users')).toHaveTextContent('Active Users');
      expect(screen.getByTestId('metric-label-links-created')).toHaveTextContent('Links Created');
      expect(screen.getByTestId('metric-label-clicks-tracked')).toHaveTextContent('Clicks Tracked');
      expect(screen.getByTestId('metric-label-uptime')).toHaveTextContent('Uptime');
    });
  });

  describe('Visual Styling and Prominence', () => {
    it('metric values have prominent styling with primary color and bold font', () => {
      renderWithProviders(<TrustSection />);

      const metricValue = screen.getByTestId('metric-value-active-users');
      expect(metricValue).toHaveClass('text-3xl');
      expect(metricValue).toHaveClass('font-bold');
      expect(metricValue).toHaveClass('text-primary');
    });

    it('trust section has background styling for visual distinction', () => {
      renderWithProviders(<TrustSection />);

      const section = screen.getByTestId('trust-section');
      expect(section).toHaveClass('bg-base-200/30');
    });

    it('metrics are displayed in a grid layout', () => {
      renderWithProviders(<TrustSection />);

      const section = screen.getByTestId('trust-section');
      const grid = section.querySelector('.grid');
      expect(grid).toBeInTheDocument();
      expect(grid).toHaveClass('grid-cols-2');
      expect(grid).toHaveClass('md:grid-cols-4');
    });

    it('each metric card uses GlassMorphismCard wrapper', () => {
      renderWithProviders(<TrustSection />);

      const metricCard = screen.getByTestId('trust-metric-active-users');
      // GlassMorphismCard applies these classes
      expect(metricCard).toHaveClass('backdrop-blur-md');
      expect(metricCard).toHaveClass('bg-base-100/50');
    });

    it('displays icons for each metric', () => {
      renderWithProviders(<TrustSection />);

      expect(screen.getByTestId('metric-icon-active-users')).toBeInTheDocument();
      expect(screen.getByTestId('metric-icon-links-created')).toBeInTheDocument();
      expect(screen.getByTestId('metric-icon-clicks-tracked')).toBeInTheDocument();
      expect(screen.getByTestId('metric-icon-uptime')).toBeInTheDocument();
    });

    it('icons have primary color styling', () => {
      renderWithProviders(<TrustSection />);

      const iconContainer = screen.getByTestId('metric-icon-active-users');
      expect(iconContainer).toHaveClass('text-primary');
    });

    it('icons contain SVG elements', () => {
      renderWithProviders(<TrustSection />);

      const icons = screen.getAllByTestId('metric-icon-svg');
      expect(icons.length).toBe(4);
    });
  });

  describe('Semantic Structure', () => {
    it('uses semantic section element', () => {
      renderWithProviders(<TrustSection />);

      const section = screen.getByTestId('trust-section');
      expect(section.tagName).toBe('SECTION');
    });

    it('has proper heading hierarchy with h2', () => {
      renderWithProviders(<TrustSection />);

      const heading = screen.getByTestId('trust-heading');
      expect(heading.tagName).toBe('H2');
    });
  });
});

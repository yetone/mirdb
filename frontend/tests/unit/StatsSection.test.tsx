/**
 * Stats Section Unit Tests
 * Owner: Scenario 6
 *
 * Test coverage:
 * - Statistics display
 * - Metric formatting
 * - Trust indicators
 *
 * Test suites:
 * - describe('Stats Section')
 * - describe('Individual Statistics')
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '../utils/renderWithProviders';
import StatsSection from '../../src/components/homepage/StatsSection';

describe('Stats Section', () => {
  describe('Section Structure', () => {
    it('renders the statistics section', () => {
      renderWithProviders(<StatsSection />);

      // Check that section exists with proper structure
      const section = document.querySelector('section');
      expect(section).toBeInTheDocument();
    });

    it('displays the trust headline', () => {
      renderWithProviders(<StatsSection />);

      // Check for trust-building headline
      const headline = screen.getByRole('heading', { level: 2 });
      expect(headline).toBeInTheDocument();
      expect(headline.textContent).toContain('Trusted');
    });

    it('contains statistical metrics container', () => {
      renderWithProviders(<StatsSection />);

      // Section should have metrics displayed
      const section = document.querySelector('section');
      expect(section).toBeInTheDocument();

      // Should have multiple stat items
      const statValues = section?.querySelectorAll('.text-primary');
      expect(statValues?.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Individual Statistics', () => {
    it('displays URLs shortened statistic', () => {
      renderWithProviders(<StatsSection />);

      // Check for URLs shortened metric
      expect(screen.getByText(/URLs Shortened/i)).toBeInTheDocument();
      expect(screen.getByText(/10M\+/)).toBeInTheDocument();
    });

    it('displays clicks tracked statistic', () => {
      renderWithProviders(<StatsSection />);

      // Check for clicks tracked metric
      expect(screen.getByText(/Clicks Tracked/i)).toBeInTheDocument();
      expect(screen.getByText(/500M\+/)).toBeInTheDocument();
    });

    it('displays countries reached statistic', () => {
      renderWithProviders(<StatsSection />);

      // Check for geographic reach metric
      expect(screen.getByText(/Countries Reached/i)).toBeInTheDocument();
      expect(screen.getByText(/150\+/)).toBeInTheDocument();
    });
  });

  describe('Statistics Formatting', () => {
    it('displays statistics with prominent styling', () => {
      renderWithProviders(<StatsSection />);

      // Statistics should have primary color and large text
      const statValues = document.querySelectorAll('.text-primary');
      expect(statValues.length).toBeGreaterThanOrEqual(3);

      // Each stat value should have bold formatting
      statValues.forEach((stat) => {
        expect(stat.classList.contains('font-bold')).toBe(true);
      });
    });

    it('displays statistics with supporting labels', () => {
      renderWithProviders(<StatsSection />);

      // Check that labels are present with appropriate styling
      expect(screen.getByText('URLs Shortened')).toBeInTheDocument();
      expect(screen.getByText('Clicks Tracked')).toBeInTheDocument();
      expect(screen.getByText('Countries Reached')).toBeInTheDocument();
    });

    it('formats numbers in human-readable format', () => {
      renderWithProviders(<StatsSection />);

      // Numbers should be formatted with M+ and + suffixes
      const urlsStat = screen.getByText(/10M\+/);
      const clicksStat = screen.getByText(/500M\+/);
      const countriesStat = screen.getByText(/150\+/);

      expect(urlsStat).toBeInTheDocument();
      expect(clicksStat).toBeInTheDocument();
      expect(countriesStat).toBeInTheDocument();
    });
  });

  describe('Responsive Layout', () => {
    it('has responsive container classes', () => {
      renderWithProviders(<StatsSection />);

      const section = document.querySelector('section');
      expect(section).toBeInTheDocument();

      // Check for responsive flex layout
      const flexContainer = section?.querySelector('.flex');
      expect(flexContainer).toBeInTheDocument();
      expect(flexContainer?.classList.contains('flex-col')).toBe(true);
      expect(flexContainer?.classList.contains('md:flex-row')).toBe(true);
    });
  });
});

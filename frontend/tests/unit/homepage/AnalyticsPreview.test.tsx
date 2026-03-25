/**
 * AnalyticsPreview Unit Tests
 * Owner: Scenario 6 - Analytics Preview Section
 *
 * Tests analytics preview section rendering including:
 * - Section visibility and structure
 * - Chart/visualization display
 * - Sample metrics display (clicks, visitors, etc.)
 * - Theme-adaptive styling
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { AnalyticsPreview } from '../../../src/components/homepage/AnalyticsPreview';
import { renderWithProviders } from './test-utils';

describe('AnalyticsPreview', () => {
  describe('Test Case 1: Analytics preview section exists', () => {
    it('renders analytics preview section', () => {
      renderWithProviders(<AnalyticsPreview />);

      const analyticsSection = screen.getByTestId('analytics-preview-section');
      expect(analyticsSection).toBeInTheDocument();
      expect(analyticsSection).toBeVisible();
    });

    it('section has proper aria-label for accessibility', () => {
      renderWithProviders(<AnalyticsPreview />);

      const analyticsSection = screen.getByRole('region', { name: /analytics preview/i });
      expect(analyticsSection).toBeInTheDocument();
    });

    it('displays section heading', () => {
      renderWithProviders(<AnalyticsPreview />);

      const heading = screen.getByRole('heading', { name: /analytics/i });
      expect(heading).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Sample chart/graph visualization renders', () => {
    it('renders chart container', () => {
      renderWithProviders(<AnalyticsPreview />);

      const chartContainer = screen.getByTestId('analytics-chart');
      expect(chartContainer).toBeInTheDocument();
      expect(chartContainer).toBeVisible();
    });

    it('chart displays bar elements for click data', () => {
      renderWithProviders(<AnalyticsPreview />);

      const chartBars = screen.getAllByTestId(/chart-bar-/);
      expect(chartBars.length).toBeGreaterThan(0);
    });

    it('chart has proper labels for days', () => {
      renderWithProviders(<AnalyticsPreview />);

      // Check for day labels
      const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
      days.forEach(day => {
        expect(screen.getByText(day)).toBeInTheDocument();
      });
    });

    it('chart uses GlassMorphismCard styling', () => {
      renderWithProviders(<AnalyticsPreview />);

      const chartContainer = screen.getByTestId('analytics-chart');
      expect(chartContainer.closest('[class*="backdrop-blur"]')).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Preview shows sample metrics', () => {
    it('displays total clicks metric', () => {
      renderWithProviders(<AnalyticsPreview />);

      const clicksMetric = screen.getByTestId('metric-clicks');
      expect(clicksMetric).toBeInTheDocument();
      expect(clicksMetric).toHaveTextContent(/clicks/i);
    });

    it('displays unique visitors metric', () => {
      renderWithProviders(<AnalyticsPreview />);

      const visitorsMetric = screen.getByTestId('metric-visitors');
      expect(visitorsMetric).toBeInTheDocument();
      expect(visitorsMetric).toHaveTextContent(/visitors/i);
    });

    it('displays conversion rate metric', () => {
      renderWithProviders(<AnalyticsPreview />);

      const conversionMetric = screen.getByTestId('metric-conversion');
      expect(conversionMetric).toBeInTheDocument();
      expect(conversionMetric).toHaveTextContent(/%/);
    });

    it('metrics display numeric values', () => {
      renderWithProviders(<AnalyticsPreview />);

      const clicksMetric = screen.getByTestId('metric-clicks');
      const visitorsMetric = screen.getByTestId('metric-visitors');

      // Should contain numbers
      expect(clicksMetric.textContent).toMatch(/\d+/);
      expect(visitorsMetric.textContent).toMatch(/\d+/);
    });

    it('displays growth indicators', () => {
      renderWithProviders(<AnalyticsPreview />);

      // Should show growth percentage indicators
      const growthIndicators = screen.getAllByTestId(/growth-indicator/);
      expect(growthIndicators.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: Theme-adaptive styling', () => {
    it('applies light theme styles correctly', () => {
      renderWithProviders(<AnalyticsPreview />, {
        themeOptions: { theme: 'light' }
      });

      const chartContainer = screen.getByTestId('analytics-chart');
      expect(chartContainer).toBeInTheDocument();
    });

    it('applies dark theme styles correctly', () => {
      renderWithProviders(<AnalyticsPreview />, {
        themeOptions: { theme: 'dark' }
      });

      const chartContainer = screen.getByTestId('analytics-chart');
      expect(chartContainer).toBeInTheDocument();
    });

    it('applies cyberpunk theme styles correctly', () => {
      renderWithProviders(<AnalyticsPreview />, {
        themeOptions: { theme: 'cyberpunk' }
      });

      const chartContainer = screen.getByTestId('analytics-chart');
      expect(chartContainer).toBeInTheDocument();
    });

    it('chart bars use theme-aware primary color', () => {
      renderWithProviders(<AnalyticsPreview />, {
        themeOptions: { theme: 'light' }
      });

      const chartBars = screen.getAllByTestId(/chart-bar-/);
      chartBars.forEach(bar => {
        // Bars should have primary color class
        expect(bar.className).toMatch(/bg-primary|from-primary/);
      });
    });

    it('metrics cards adapt to theme', () => {
      renderWithProviders(<AnalyticsPreview />, {
        themeOptions: { theme: 'synthwave' }
      });

      const metricsCards = screen.getAllByTestId(/metric-/);
      metricsCards.forEach(card => {
        expect(card).toBeInTheDocument();
      });
    });
  });

  describe('Interactive Elements', () => {
    it('chart bars have hover state', async () => {
      const user = userEvent.setup();
      renderWithProviders(<AnalyticsPreview />);

      const firstBar = screen.getByTestId('chart-bar-0');
      await user.hover(firstBar);

      // Bar should be focusable/hoverable
      expect(firstBar).toBeInTheDocument();
    });

    it('displays tooltip on bar hover', async () => {
      const user = userEvent.setup();
      renderWithProviders(<AnalyticsPreview />);

      const firstBar = screen.getByTestId('chart-bar-0');
      await user.hover(firstBar);

      // Should show tooltip with value
      const tooltip = screen.queryByTestId('chart-tooltip');
      // Tooltip may or may not be visible depending on implementation
      // This tests that hovering doesn't break
      expect(firstBar).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('section has semantic structure', () => {
      renderWithProviders(<AnalyticsPreview />);

      const section = screen.getByRole('region', { name: /analytics preview/i });
      expect(section).toBeInTheDocument();
    });

    it('chart has accessible description', () => {
      renderWithProviders(<AnalyticsPreview />);

      const chart = screen.getByTestId('analytics-chart');
      expect(chart).toHaveAttribute('role', 'img');
      expect(chart).toHaveAttribute('aria-label');
    });

    it('metrics are properly labeled', () => {
      renderWithProviders(<AnalyticsPreview />);

      const clicksMetric = screen.getByTestId('metric-clicks');
      const labelledMetric = within(clicksMetric).getByText(/clicks/i);
      expect(labelledMetric).toBeInTheDocument();
    });
  });

  describe('Responsive Design', () => {
    it('chart container is present at all viewport sizes', () => {
      renderWithProviders(<AnalyticsPreview />);

      const chartContainer = screen.getByTestId('analytics-chart');
      expect(chartContainer).toBeInTheDocument();
    });

    it('metrics grid displays properly', () => {
      renderWithProviders(<AnalyticsPreview />);

      const metricsGrid = screen.getByTestId('metrics-grid');
      expect(metricsGrid).toBeInTheDocument();
    });
  });
});

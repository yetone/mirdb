/**
 * AnalyticsPreview Component Tests
 * Owner: Scenario 5 - Analytics Preview Section
 *
 * Test cases:
 * 1. Analytics preview section renders with chart visualizations
 * 2. At least one time-series chart showing 'clicks over time' is displayed
 * 3. Pie or donut chart showing 'browser distribution' or similar is displayed
 * 4. Charts have clear labels, axes, and legends
 * 5. Charts display realistic sample data values
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { AnalyticsPreview } from '@/components/homepage/AnalyticsPreview';
import {
  clicksOverTimeData,
  browserDistributionData,
} from '@/data/sampleAnalytics';

// Mock ResizeObserver for Recharts ResponsiveContainer
class MockResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}

global.ResizeObserver = MockResizeObserver;

// Mock IntersectionObserver for Framer Motion
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | null = null;
  readonly rootMargin: string = '';
  readonly thresholds: ReadonlyArray<number> = [];

  constructor(
    private callback: IntersectionObserverCallback,
    _options?: IntersectionObserverInit
  ) {}

  observe(_target: Element): void {
    this.callback(
      [
        {
          isIntersecting: true,
          boundingClientRect: {} as DOMRectReadOnly,
          intersectionRatio: 1,
          intersectionRect: {} as DOMRectReadOnly,
          rootBounds: null,
          target: {} as Element,
          time: Date.now(),
        },
      ],
      this
    );
  }

  unobserve(_target: Element): void {}
  disconnect(): void {}
  takeRecords(): IntersectionObserverEntry[] {
    return [];
  }
}

global.IntersectionObserver = MockIntersectionObserver;

describe('AnalyticsPreview', () => {
  // Test Case 1: Analytics preview section renders with chart visualizations
  describe('renders with chart visualizations', () => {
    it('should render the analytics preview section', () => {
      render(<AnalyticsPreview />);

      const section = screen.getByTestId('analytics-preview-section');
      expect(section).toBeInTheDocument();
    });

    it('should have a heading for the section', () => {
      render(<AnalyticsPreview />);

      const heading = screen.getByRole('heading', { name: /powerful analytics/i });
      expect(heading).toBeInTheDocument();
    });

    it('should have a description explaining the analytics', () => {
      render(<AnalyticsPreview />);

      const description = screen.getByTestId('analytics-preview-description');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toContain('Track every click');
    });

    it('should render both chart containers', () => {
      render(<AnalyticsPreview />);

      const clicksChart = screen.getByTestId('clicks-over-time-chart');
      const browserChart = screen.getByTestId('browser-distribution-chart');

      expect(clicksChart).toBeInTheDocument();
      expect(browserChart).toBeInTheDocument();
    });
  });

  // Test Case 2: Time-series chart showing 'clicks over time' is displayed
  describe('clicks over time chart', () => {
    it('should render the clicks over time chart container', () => {
      render(<AnalyticsPreview />);

      const chartContainer = screen.getByTestId('clicks-over-time-chart');
      expect(chartContainer).toBeInTheDocument();
    });

    it('should display the clicks over time label', () => {
      render(<AnalyticsPreview />);

      const label = screen.getByTestId('clicks-over-time-label');
      expect(label).toHaveTextContent('Clicks Over Time');
    });

    it('should have an accessible chart container', () => {
      render(<AnalyticsPreview />);

      const chartArea = screen.getByTestId('clicks-over-time-container');
      expect(chartArea).toHaveAttribute('role', 'img');
      expect(chartArea).toHaveAttribute(
        'aria-label',
        'Area chart showing clicks over time for the past week'
      );
    });

    it('should render inside a GlassMorphismCard with proper styling', () => {
      render(<AnalyticsPreview />);

      const chartCard = screen.getByTestId('clicks-over-time-chart');
      expect(chartCard.className).toContain('backdrop-blur');
      expect(chartCard.className).toContain('bg-base-100');
    });
  });

  // Test Case 3: Pie or donut chart showing 'browser distribution' is displayed
  describe('browser distribution chart', () => {
    it('should render the browser distribution chart container', () => {
      render(<AnalyticsPreview />);

      const chartContainer = screen.getByTestId('browser-distribution-chart');
      expect(chartContainer).toBeInTheDocument();
    });

    it('should display the browser distribution label', () => {
      render(<AnalyticsPreview />);

      const label = screen.getByTestId('browser-distribution-label');
      expect(label).toHaveTextContent('Browser Distribution');
    });

    it('should have an accessible chart container', () => {
      render(<AnalyticsPreview />);

      const chartArea = screen.getByTestId('browser-distribution-container');
      expect(chartArea).toHaveAttribute('role', 'img');
      expect(chartArea).toHaveAttribute(
        'aria-label',
        'Pie chart showing browser distribution of visitors'
      );
    });

    it('should render inside a GlassMorphismCard with proper styling', () => {
      render(<AnalyticsPreview />);

      const chartCard = screen.getByTestId('browser-distribution-chart');
      expect(chartCard.className).toContain('backdrop-blur');
      expect(chartCard.className).toContain('bg-base-100');
    });
  });

  // Test Case 4: Charts have clear labels, axes, and legends
  describe('chart labels and legends', () => {
    it('should have a label for the clicks over time chart', () => {
      render(<AnalyticsPreview />);

      const label = screen.getByTestId('clicks-over-time-label');
      expect(label).toBeInTheDocument();
      expect(label.tagName.toLowerCase()).toBe('h3');
    });

    it('should have a label for the browser distribution chart', () => {
      render(<AnalyticsPreview />);

      const label = screen.getByTestId('browser-distribution-label');
      expect(label).toBeInTheDocument();
      expect(label.tagName.toLowerCase()).toBe('h3');
    });

    it('should have sample note indicating data is sample', () => {
      render(<AnalyticsPreview />);

      const clicksNote = screen.getByTestId('clicks-chart-sample-note');
      const browserNote = screen.getByTestId('browser-chart-sample-note');

      expect(clicksNote).toHaveTextContent(/sample/i);
      expect(browserNote).toHaveTextContent(/sample/i);
    });

    it('should have proper section labeling for accessibility', () => {
      render(<AnalyticsPreview />);

      const section = screen.getByTestId('analytics-preview-section');
      expect(section).toHaveAttribute('aria-labelledby', 'analytics-preview-heading');
    });
  });

  // Test Case 5: Charts display realistic sample data values
  describe('sample data display', () => {
    it('should use clicksOverTimeData with realistic values', () => {
      // Verify the sample data has realistic click counts
      expect(clicksOverTimeData).toHaveLength(7); // 7 days

      clicksOverTimeData.forEach((dataPoint) => {
        expect(dataPoint.date).toBeTruthy();
        expect(dataPoint.clicks).toBeGreaterThan(0);
        expect(dataPoint.clicks).toBeLessThan(1000); // Realistic range
      });
    });

    it('should use browserDistributionData with realistic percentages', () => {
      // Verify the sample data adds up to 100%
      const totalPercentage = browserDistributionData.reduce(
        (sum, item) => sum + item.value,
        0
      );
      expect(totalPercentage).toBe(100);
    });

    it('should include major browsers in distribution data', () => {
      const browsers = browserDistributionData.map((d) => d.browser.toLowerCase());

      expect(browsers).toContain('chrome');
      expect(browsers).toContain('safari');
      expect(browsers).toContain('firefox');
    });

    it('should have colors defined for each browser', () => {
      browserDistributionData.forEach((dataPoint) => {
        expect(dataPoint.color).toBeTruthy();
        expect(dataPoint.color).toMatch(/^#[0-9A-Fa-f]{6}$/); // Valid hex color
      });
    });

    it('should have day labels in clicks data', () => {
      const days = clicksOverTimeData.map((d) => d.date);

      expect(days).toContain('Mon');
      expect(days).toContain('Tue');
      expect(days).toContain('Wed');
      expect(days).toContain('Thu');
      expect(days).toContain('Fri');
      expect(days).toContain('Sat');
      expect(days).toContain('Sun');
    });
  });

  // Additional styling and accessibility tests
  describe('styling consistency', () => {
    it('should use GlassMorphismCard for both charts', () => {
      render(<AnalyticsPreview />);

      const charts = [
        screen.getByTestId('clicks-over-time-chart'),
        screen.getByTestId('browser-distribution-chart'),
      ];

      charts.forEach((chart) => {
        expect(chart.className).toContain('backdrop-blur');
        expect(chart.className).toContain('rounded-xl');
        expect(chart.className).toContain('shadow-xl');
      });
    });

    it('should have responsive grid layout', () => {
      render(<AnalyticsPreview />);

      const section = screen.getByTestId('analytics-preview-section');
      const gridContainer = section.querySelector('.grid');

      expect(gridContainer).toBeInTheDocument();
      expect(gridContainer?.className).toContain('grid-cols-1');
      expect(gridContainer?.className).toContain('lg:grid-cols-2');
    });
  });
});

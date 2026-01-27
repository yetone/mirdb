/**
 * Stats Section Tests
 * Owner: Scenario 4 - Trust Indicators and Statistics
 *
 * Test Cases:
 * 1. Stats section exists (if implemented as 'Could' priority feature)
 * 2. Statistic showing count of URLs shortened is displayed
 * 3. Statistic showing total clicks tracked is displayed
 * 4. Large numbers are formatted with commas or abbreviated (e.g., 1.2M)
 * 5. Each statistic has a descriptive label explaining what it represents
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from './test-utils';
import { StatsSection, formatStatNumber } from '../../src/components/homepage/StatsSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({
      children,
      ...props
    }: {
      children: React.ReactNode;
      [key: string]: unknown;
    }) => {
      const {
        variants,
        initial,
        whileInView,
        viewport,
        animate,
        exit,
        ...domProps
      } = props;
      return <div {...domProps}>{children}</div>;
    },
  },
  useInView: () => true,
}));

// Mock matchMedia for reduced motion detection
const mockMatchMedia = vi.fn().mockImplementation((query) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: vi.fn(),
  removeListener: vi.fn(),
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  dispatchEvent: vi.fn(),
}));

beforeEach(() => {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: mockMatchMedia,
  });
});

afterEach(() => {
  vi.clearAllMocks();
});

describe('StatsSection', () => {
  // Test Case 1: Stats section exists (if implemented as 'Could' priority feature)
  describe('TC-1: Stats Section Container', () => {
    it('should have a stats section with id="stats"', () => {
      renderWithProviders(<StatsSection />);

      const statsSection = document.getElementById('stats');
      expect(statsSection).toBeInTheDocument();
    });

    it('should have data-testid="stats-section" for querying', () => {
      renderWithProviders(<StatsSection />);

      const statsSection = screen.getByTestId('stats-section');
      expect(statsSection).toBeInTheDocument();
    });

    it('should have accessible landmark with aria-labelledby', () => {
      renderWithProviders(<StatsSection />);

      const section = document.getElementById('stats');
      expect(section).toHaveAttribute('aria-labelledby', 'stats-heading');
    });

    it('should be a semantic section element', () => {
      renderWithProviders(<StatsSection />);

      const section = document.getElementById('stats');
      expect(section?.tagName).toBe('SECTION');
    });
  });

  // Test Case 2: Statistic showing count of URLs shortened is displayed
  describe('TC-2: URLs Created Statistic', () => {
    it('should display a statistic for URLs created', () => {
      renderWithProviders(<StatsSection />);

      const labels = screen.getAllByTestId('stat-label');
      const hasUrlsLabel = labels.some((label) => {
        const text = label.textContent?.toLowerCase() || '';
        return text.includes('url') || text.includes('link') || text.includes('created');
      });

      expect(hasUrlsLabel).toBe(true);
    });

    it('should display the URLs created count', () => {
      renderWithProviders(<StatsSection />);

      const statValues = screen.getAllByTestId('stat-number');
      expect(statValues.length).toBeGreaterThanOrEqual(1);
    });
  });

  // Test Case 3: Statistic showing total clicks tracked is displayed
  describe('TC-3: Clicks Tracked Statistic', () => {
    it('should display a statistic for clicks tracked', () => {
      renderWithProviders(<StatsSection />);

      const labels = screen.getAllByTestId('stat-label');
      const hasClicksLabel = labels.some((label) => {
        const text = label.textContent?.toLowerCase() || '';
        return text.includes('click') || text.includes('track');
      });

      expect(hasClicksLabel).toBe(true);
    });
  });

  // Test Case 4: Large numbers are formatted with commas or abbreviated (e.g., 1.2M)
  describe('TC-4: Number Formatting', () => {
    it('should format millions with M suffix (e.g., 1.5M)', () => {
      const formatted = formatStatNumber(1500000);
      expect(formatted).toBe('1.5M');
    });

    it('should format clean millions without decimal (e.g., 1M)', () => {
      const formatted = formatStatNumber(1000000);
      expect(formatted).toBe('1M');
    });

    it('should format large millions correctly (e.g., 25M)', () => {
      const formatted = formatStatNumber(25000000);
      expect(formatted).toBe('25M');
    });

    it('should format thousands with comma separators', () => {
      const formatted = formatStatNumber(50000);
      expect(formatted).toBe('50,000');
    });

    it('should format numbers under 1000 without separators', () => {
      const formatted = formatStatNumber(500);
      expect(formatted).toBe('500');
    });

    it('should format large thousands correctly', () => {
      const formatted = formatStatNumber(999999);
      expect(formatted).toBe('999,999');
    });

    it('stats displayed on the page should show formatted numbers', () => {
      renderWithProviders(<StatsSection />);

      const statValues = screen.getAllByTestId('stat-value');
      statValues.forEach((value) => {
        const text = value.textContent || '';
        // Should either have M suffix or comma separators or be a raw number
        const hasValidFormat =
          text.includes('M') ||
          text.includes(',') ||
          /^\d+\+?$/.test(text);
        expect(hasValidFormat).toBe(true);
      });
    });
  });

  // Test Case 5: Each statistic has a descriptive label explaining what it represents
  describe('TC-5: Statistics Have Labels', () => {
    it('should render at least 2 statistic cards', () => {
      renderWithProviders(<StatsSection />);

      const statCards = screen.getAllByTestId('stat-card');
      expect(statCards.length).toBeGreaterThanOrEqual(2);
    });

    it('each statistic should have a descriptive label', () => {
      renderWithProviders(<StatsSection />);

      const statCards = screen.getAllByTestId('stat-card');
      statCards.forEach((card) => {
        const label = within(card).getByTestId('stat-label');
        expect(label).toBeInTheDocument();
        expect(label.textContent?.length).toBeGreaterThan(0);
      });
    });

    it('labels should be non-empty and descriptive', () => {
      renderWithProviders(<StatsSection />);

      const labels = screen.getAllByTestId('stat-label');
      labels.forEach((label) => {
        const text = label.textContent || '';
        // Labels should be at least 3 characters (meaningful)
        expect(text.length).toBeGreaterThanOrEqual(3);
      });
    });
  });

  // Additional tests for visual presentation
  describe('Visual Presentation', () => {
    it('each statistic should have an icon', () => {
      renderWithProviders(<StatsSection />);

      const statCards = screen.getAllByTestId('stat-card');
      statCards.forEach((card) => {
        const icon = within(card).getByTestId('stat-icon');
        expect(icon).toBeInTheDocument();
      });
    });

    it('stat cards should use GlassMorphismCard styling (backdrop-blur)', () => {
      renderWithProviders(<StatsSection />);

      const statCards = screen.getAllByTestId('stat-card');
      statCards.forEach((card) => {
        expect(card).toHaveClass('backdrop-blur-md');
      });
    });

    it('stat cards should have rounded corners', () => {
      renderWithProviders(<StatsSection />);

      const statCards = screen.getAllByTestId('stat-card');
      statCards.forEach((card) => {
        expect(card).toHaveClass('rounded-xl');
      });
    });

    it('should display stats in 3-column grid on desktop', () => {
      renderWithProviders(<StatsSection />);

      const section = document.getElementById('stats');
      const grid = section?.querySelector('.grid');
      expect(grid).toHaveClass('md:grid-cols-3');
    });

    it('should display stats in single column on mobile', () => {
      renderWithProviders(<StatsSection />);

      const section = document.getElementById('stats');
      const grid = section?.querySelector('.grid');
      expect(grid).toHaveClass('grid-cols-1');
    });
  });

  // Accessibility tests
  describe('Accessibility', () => {
    it('should have a section heading', () => {
      renderWithProviders(<StatsSection />);

      const heading = screen.getByRole('heading', { name: /trusted by thousands/i });
      expect(heading).toBeInTheDocument();
    });

    it('icons should have aria-hidden attribute', () => {
      renderWithProviders(<StatsSection />);

      const icons = screen.getAllByTestId('stat-icon');
      icons.forEach((iconContainer) => {
        const svg = iconContainer.querySelector('svg');
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });

  // Custom stats props test
  describe('Custom Stats Props', () => {
    it('should accept custom stats via props', () => {
      const customStats = [
        {
          id: 1,
          icon: <span data-testid="custom-icon">Custom</span>,
          value: 123456,
          label: 'Custom Metric',
        },
      ];

      renderWithProviders(<StatsSection stats={customStats} />);

      const label = screen.getByTestId('stat-label');
      expect(label).toHaveTextContent('Custom Metric');
    });
  });
});

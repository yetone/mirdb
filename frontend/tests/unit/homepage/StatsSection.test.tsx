/**
 * StatsSection Unit Tests
 * Owner: Scenario 11 - Social Proof Elements
 *
 * Tests social proof statistics section including:
 * - Statistics section visibility
 * - User count display
 * - Links created display
 * - API data fetching and display
 * - Graceful error fallback
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import { StatsSection } from '../../../src/components/homepage/StatsSection';
import { renderWithProviders } from './test-utils';

// Mock stats data
const mockStats = {
  userCount: 5420,
  linksCreated: 128750,
};

// Mock stats result for controlled testing
const createMockStatsResult = (overrides = {}) => ({
  stats: mockStats,
  isLoading: false,
  error: null,
  ...overrides,
});

describe('StatsSection', () => {
  describe('Test Case 1: Statistics/social proof section exists', () => {
    it('renders the stats section', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const statsSection = screen.getByTestId('stats-section');
      expect(statsSection).toBeInTheDocument();
    });

    it('stats section is visible', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const statsSection = screen.getByTestId('stats-section');
      expect(statsSection).toBeVisible();
    });

    it('stats section uses semantic section element', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const statsSection = screen.getByTestId('stats-section');
      expect(statsSection.tagName.toLowerCase()).toBe('section');
    });

    it('stats section has proper aria-labelledby for accessibility', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const statsSection = screen.getByTestId('stats-section');
      expect(statsSection).toHaveAttribute('aria-labelledby', 'stats-heading');
    });

    it('displays section heading', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveAttribute('id', 'stats-heading');
    });

    it('renders stats card container', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const statsCard = screen.getByTestId('stats-card');
      expect(statsCard).toBeInTheDocument();
    });
  });

  describe('Test Case 2: User count number is displayed', () => {
    it('renders user count stat item', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const usersStat = screen.getByTestId('stat-users');
      expect(usersStat).toBeInTheDocument();
    });

    it('displays user count value', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const usersValue = screen.getByTestId('stat-users-value');
      expect(usersValue).toBeInTheDocument();
      // 5420 should be formatted as "5.4K+"
      expect(usersValue).toHaveTextContent('5.4K+');
    });

    it('displays user count label', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const usersLabel = screen.getByTestId('stat-users-label');
      expect(usersLabel).toBeInTheDocument();
      expect(usersLabel).toHaveTextContent(/users/i);
    });

    it('displays user icon', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const usersIcon = screen.getByTestId('stat-users-icon');
      expect(usersIcon).toBeInTheDocument();
      expect(usersIcon.querySelector('svg')).toBeInTheDocument();
    });

    it('displays small user counts without K formatting', () => {
      renderWithProviders(
        <StatsSection
          statsOverride={createMockStatsResult({
            stats: { userCount: 500, linksCreated: 1000 },
          })}
        />
      );

      const usersValue = screen.getByTestId('stat-users-value');
      expect(usersValue).toHaveTextContent('500');
    });
  });

  describe('Test Case 3: Links created count is displayed', () => {
    it('renders links created stat item', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const linksStat = screen.getByTestId('stat-links');
      expect(linksStat).toBeInTheDocument();
    });

    it('displays links created value', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const linksValue = screen.getByTestId('stat-links-value');
      expect(linksValue).toBeInTheDocument();
      // 128750 should be formatted as "128.8K+"
      expect(linksValue).toHaveTextContent('128.8K+');
    });

    it('displays links created label', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const linksLabel = screen.getByTestId('stat-links-label');
      expect(linksLabel).toBeInTheDocument();
      expect(linksLabel).toHaveTextContent(/links/i);
    });

    it('displays links icon', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const linksIcon = screen.getByTestId('stat-links-icon');
      expect(linksIcon).toBeInTheDocument();
      expect(linksIcon.querySelector('svg')).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Real statistics fetch and display correctly (Integration)', () => {
    let originalFetch: typeof global.fetch;

    beforeEach(() => {
      originalFetch = global.fetch;
    });

    afterEach(() => {
      global.fetch = originalFetch;
      vi.restoreAllMocks();
    });

    it('fetches and displays stats from API', async () => {
      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            userCount: 10000,
            linksCreated: 50000,
          }),
      });

      renderWithProviders(<StatsSection />);

      // Wait for loading to finish
      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument();
      });

      const usersValue = screen.getByTestId('stat-users-value');
      expect(usersValue).toHaveTextContent('10.0K+');

      const linksValue = screen.getByTestId('stat-links-value');
      expect(linksValue).toHaveTextContent('50.0K+');
    });

    it('calls the correct API endpoint', async () => {
      const mockFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ userCount: 100, linksCreated: 200 }),
      });
      global.fetch = mockFetch;

      renderWithProviders(<StatsSection />);

      await waitFor(() => {
        expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('/stats'));
      });
    });

    it('shows loading state while fetching', () => {
      global.fetch = vi.fn().mockImplementation(
        () =>
          new Promise(() => {
            /* never resolves */
          })
      );

      renderWithProviders(<StatsSection />);

      const loadingElement = screen.getByTestId('stats-loading');
      expect(loadingElement).toBeInTheDocument();
    });

    it('handles alternative API response format', async () => {
      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            users: 7500,
            links: 30000,
          }),
      });

      renderWithProviders(<StatsSection />);

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument();
      });

      const usersValue = screen.getByTestId('stat-users-value');
      expect(usersValue).toHaveTextContent('7.5K+');
    });
  });

  describe('Test Case 5: Graceful fallback on API error (Integration)', () => {
    let originalFetch: typeof global.fetch;

    beforeEach(() => {
      originalFetch = global.fetch;
    });

    afterEach(() => {
      global.fetch = originalFetch;
      vi.restoreAllMocks();
    });

    it('displays error message when API fails', () => {
      renderWithProviders(
        <StatsSection
          statsOverride={createMockStatsResult({
            stats: null,
            error: new Error('Failed to fetch'),
          })}
        />
      );

      const errorMessage = screen.getByTestId('stats-error');
      expect(errorMessage).toBeInTheDocument();
      expect(errorMessage).toHaveTextContent(/unavailable/i);
    });

    it('section still renders on error (graceful degradation)', () => {
      renderWithProviders(
        <StatsSection
          statsOverride={createMockStatsResult({
            stats: null,
            error: new Error('Network error'),
          })}
        />
      );

      const statsSection = screen.getByTestId('stats-section');
      expect(statsSection).toBeInTheDocument();
      expect(statsSection).toHaveAttribute('data-error', 'true');
    });

    it('handles network errors gracefully', async () => {
      global.fetch = vi.fn().mockRejectedValueOnce(new Error('Network error'));

      renderWithProviders(<StatsSection />);

      await waitFor(() => {
        const errorMessage = screen.getByTestId('stats-error');
        expect(errorMessage).toBeInTheDocument();
      });
    });

    it('handles non-ok HTTP responses', async () => {
      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: false,
        status: 500,
      });

      renderWithProviders(<StatsSection />);

      await waitFor(() => {
        const errorMessage = screen.getByTestId('stats-error');
        expect(errorMessage).toBeInTheDocument();
      });
    });
  });

  describe('Accessibility', () => {
    it('icons have aria-hidden attribute', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const usersIcon = screen.getByTestId('stat-users-icon');
      const svg = usersIcon.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });

    it('heading has correct id for aria-labelledby', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveAttribute('id', 'stats-heading');
    });
  });

  describe('Styling', () => {
    it('stats card uses GlassMorphismCard styling', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const statsCard = screen.getByTestId('stats-card');
      expect(statsCard).toHaveClass('backdrop-blur-md');
    });

    it('section has proper padding', () => {
      renderWithProviders(
        <StatsSection statsOverride={createMockStatsResult()} />
      );

      const section = screen.getByTestId('stats-section');
      expect(section).toHaveClass('py-16');
      expect(section).toHaveClass('px-4');
    });
  });
});

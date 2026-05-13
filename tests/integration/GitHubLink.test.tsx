import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import GitHubLink from '../../src/components/GitHubLink';
import { GITHUB_API_URL } from '../../src/utils/constants';

describe('GitHubLink Integration', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  describe('Star count display — dynamic fetch', () => {
    it('fetches star count from GitHub API and displays it', async () => {
      const mockFetch = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
        ok: true,
        json: async () => ({ stargazers_count: 1337 }),
      } as Response);

      render(<GitHubLink />);

      await waitFor(() => {
        expect(mockFetch).toHaveBeenCalledWith(GITHUB_API_URL);
      });

      await waitFor(() => {
        expect(screen.getByText('1,337')).toBeInTheDocument();
      });
    });

    it('displays star count with a star icon', async () => {
      vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
        ok: true,
        json: async () => ({ stargazers_count: 500 }),
      } as Response);

      render(<GitHubLink />);

      await waitFor(() => {
        expect(screen.getByText('500')).toBeInTheDocument();
      });

      const link = screen.getByRole('link');
      const starIcon = link.querySelector('svg[fill="currentColor"]');
      expect(starIcon).toBeInTheDocument();
    });
  });

  describe('Graceful error handling', () => {
    it('still renders the GitHub link when API fetch fails', async () => {
      vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(new Error('Network error'));

      render(<GitHubLink />);

      const link = await screen.findByRole('link');
      expect(link).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      expect(link).toHaveAttribute('target', '_blank');
    });

    it('does not display star count when API fetch fails', async () => {
      vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(new Error('Network error'));

      render(<GitHubLink />);

      await waitFor(() => {
        const link = screen.getByRole('link');
        expect(link.textContent).toContain('View on GitHub');
      });

      const starCount = screen.queryByText(/\d{1,3}(,\d{3})*/);
      expect(starCount).toBeNull();
    });

    it('handles non-OK HTTP response gracefully', async () => {
      vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
        ok: false,
        status: 403,
        json: async () => ({}),
      } as Response);

      render(<GitHubLink />);

      const link = await screen.findByRole('link');
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      await waitFor(() => {
        const starCount = screen.queryByText(/\d{1,3}(,\d{3})*/);
        expect(starCount).toBeNull();
      });
    });

    it('page layout is not broken when API fails', async () => {
      vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(new Error('Network error'));

      render(<GitHubLink />);

      const link = await screen.findByRole('link');
      expect(link).toBeInTheDocument();
      expect(link.tagName).toBe('A');
      // The link should still have proper styling classes
      expect(link.className).toMatch(/inline-flex/);
      expect(link.className).toMatch(/rounded-lg/);
      expect(link.className).toMatch(/transition-colors/);
    });
  });

  describe('Loading state', () => {
    it('shows loading indicator for star count while fetching', async () => {
      // Never resolve the fetch to keep it in loading state
      vi.spyOn(globalThis, 'fetch').mockImplementationOnce(
        () => new Promise(() => {})
      );

      render(<GitHubLink />);

      const link = screen.getByRole('link');
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('aria-label');

      // The loading pulse animation should be present
      const pulseElement = link.querySelector('.animate-pulse');
      expect(pulseElement).toBeInTheDocument();
    });
  });
});

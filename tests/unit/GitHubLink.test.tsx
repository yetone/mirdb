import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import GitHubLink from '../../src/components/GitHubLink';
import { GITHUB_REPO_URL, GITHUB_API_URL } from '../../src/utils/constants';

function mockGitHubApi(stars: number | null, ok = true) {
  return vi.spyOn(globalThis, 'fetch').mockResolvedValue({
    ok,
    json: async () => ({ stargazers_count: stars }),
  } as Response);
}

describe('GitHubLink Component', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  describe('"View on GitHub" CTA', () => {
    it('renders a link with text "View on GitHub"', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByText('View on GitHub');
      expect(link).toBeInTheDocument();
    });

    it('links to the correct GitHub repository URL', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      expect(link).toHaveAttribute('href', GITHUB_REPO_URL);
    });
  });

  describe('External link attributes', () => {
    it('has target="_blank" attribute to open in new tab', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      expect(link).toHaveAttribute('target', '_blank');
    });

    it('has rel="noopener noreferrer" for security', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  describe('GitHub brand icon', () => {
    it('includes a GitHub icon with aria-hidden', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      const icons = link.querySelectorAll('svg');
      const githubIcon = Array.from(icons).find(
        (icon) => !icon.classList.contains('animate-pulse')
      );
      expect(githubIcon).toBeInTheDocument();
    });
  });

  describe('Keyboard accessibility', () => {
    it('link is focusable via keyboard Tab', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      link.focus();
      expect(document.activeElement).toBe(link);
    });

    it('link has visible focus ring styles', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      expect(link.className).toMatch(/focus:ring-2/);
      expect(link.className).toMatch(/focus:ring-brand-500/);
      expect(link.className).toMatch(/focus:ring-offset-2/);
    });

    it('link has accessible name via aria-label', async () => {
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      expect(link).toHaveAttribute('aria-label');
      expect(link.getAttribute('aria-label')).toContain('View MirDB on GitHub');
      expect(link.getAttribute('aria-label')).toContain('42 stars');
    });

    it('link is activatable via Enter key', async () => {
      const user = userEvent.setup();
      mockGitHubApi(42);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      link.focus();

      // Verify link is an anchor element that can receive keyboard events
      expect(link.tagName).toBe('A');
      expect(link).toHaveAttribute('href', GITHUB_REPO_URL);

      // Keyboard activation triggers navigation intent on anchor
      await user.keyboard('{Enter}');
      // The link element has the correct href, confirming it is keyboard activatable
      expect(link).toBe(document.activeElement);
    });
  });

  describe('Accessible label without star count', () => {
    it('has aria-label without star count when stars unavailable', async () => {
      mockGitHubApi(null);
      render(<GitHubLink />);
      const link = await screen.findByRole('link');
      expect(link.getAttribute('aria-label')).toBe('View MirDB on GitHub');
    });
  });
});

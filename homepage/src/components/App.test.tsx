import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import App from '../App';

describe('GitHub Repository Links - Integration Tests', () => {
  const githubUrl = 'https://github.com/mirdb/mirdb';

  describe('Test Case 1: Navigation contains link to GitHub repository', () => {
    it('navigation contains link to GitHub repository', () => {
      render(<App />);

      // Get sticky navigation element
      const nav = screen.getByTestId('sticky-nav');

      // Find GitHub link within the navigation
      const navGitHubLink = nav.querySelector('a[href*="github.com"]');
      expect(navGitHubLink).toBeInTheDocument();
      expect(navGitHubLink).toHaveAttribute('href', githubUrl);
    });
  });

  describe('Test Case 2: Footer contains link to GitHub repository', () => {
    it('footer contains link to GitHub repository', () => {
      render(<App />);

      // Get footer element
      const footer = screen.getByRole('contentinfo');

      // Find GitHub link within the footer
      const footerGitHubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGitHubLink).toBeInTheDocument();
      expect(footerGitHubLink).toHaveAttribute('href', githubUrl);
    });
  });

  describe('Test Case 3: GitHub link opens in new tab (target="_blank")', () => {
    it('navigation GitHub link has target="_blank"', () => {
      render(<App />);

      const nav = screen.getByTestId('sticky-nav');
      const navGitHubLink = nav.querySelector('a[href*="github.com"]');
      expect(navGitHubLink).toHaveAttribute('target', '_blank');
    });

    it('footer GitHub link has target="_blank"', () => {
      render(<App />);

      const footer = screen.getByRole('contentinfo');
      const footerGitHubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGitHubLink).toHaveAttribute('target', '_blank');
    });
  });
});

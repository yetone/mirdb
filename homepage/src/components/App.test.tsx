import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import App from '../App';

describe('GitHub Repository Links - Integration Tests', () => {
  const githubUrl = 'https://github.com/mirdb/mirdb';

  describe('Test Case 1: Header contains link to GitHub repository', () => {
    it('header contains link to GitHub repository', () => {
      render(<App />);

      // Get header element
      const header = screen.getByRole('banner');

      // Find GitHub link within the header
      const headerGitHubLink = header.querySelector('a[href*="github.com"]');
      expect(headerGitHubLink).toBeInTheDocument();
      expect(headerGitHubLink).toHaveAttribute('href', githubUrl);
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
    it('header GitHub link has target="_blank"', () => {
      render(<App />);

      const header = screen.getByRole('banner');
      const headerGitHubLink = header.querySelector('a[href*="github.com"]');
      expect(headerGitHubLink).toHaveAttribute('target', '_blank');
    });

    it('footer GitHub link has target="_blank"', () => {
      render(<App />);

      const footer = screen.getByRole('contentinfo');
      const footerGitHubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGitHubLink).toHaveAttribute('target', '_blank');
    });
  });
});

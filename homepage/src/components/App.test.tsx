import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import App from '../App';

// Wrapper component that provides routing context at the root path
const renderApp = (initialRoute = '/') => {
  // Since App includes BrowserRouter, we render it directly
  // For tests, we need to mock window.location or use MemoryRouter
  window.history.pushState({}, '', initialRoute);
  return render(<App />);
};

describe('GitHub Repository Links - Integration Tests', () => {
  const githubUrl = 'https://github.com/mirdb/mirdb';

  describe('Test Case 1: Navigation contains link to GitHub repository', () => {
    it('navigation contains link to GitHub repository', () => {
      renderApp('/');

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
      renderApp('/');

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
      renderApp('/');

      const nav = screen.getByTestId('sticky-nav');
      const navGitHubLink = nav.querySelector('a[href*="github.com"]');
      expect(navGitHubLink).toHaveAttribute('target', '_blank');
    });

    it('footer GitHub link has target="_blank"', () => {
      renderApp('/');

      const footer = screen.getByRole('contentinfo');
      const footerGitHubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGitHubLink).toHaveAttribute('target', '_blank');
    });
  });
});

describe('404 Error Page - Integration Tests', () => {
  it('renders 404 page for invalid routes', () => {
    renderApp('/nonexistent-page');

    // Verify 404 page is displayed
    const notFoundPage = screen.getByTestId('not-found-page');
    expect(notFoundPage).toBeInTheDocument();

    // Verify 404 title
    const title = screen.getByRole('heading', { level: 1 });
    expect(title).toHaveTextContent('404');
  });

  it('404 page has link back to homepage', () => {
    renderApp('/nonexistent-page');

    const homeLink = screen.getByTestId('home-link');
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');
  });
});

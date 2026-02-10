/**
 * Navigation Integration Tests
 * Owner: Scenario 5 - Navigation and GitHub Links
 *
 * Integration tests for:
 * - Sticky header behavior when scrolling
 * - Navigation within full page context
 * - Anchor link navigation
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import App from '@/App';
import { Header } from '@/components/layout/Header';
import { ThemeProvider } from '@/context/ThemeContext';
import { GITHUB_URL } from '@/utils/constants';

// Helper to render App with required providers
const renderApp = () => {
  return render(
    <ThemeProvider defaultTheme="light">
      <App />
    </ThemeProvider>
  );
};

const getSiteHeader = () => {
  return screen.getByRole('banner', { name: /site header/i });
};

describe('Navigation Integration Tests', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 4: Sticky header on scroll', () => {
    beforeEach(() => {
      Object.defineProperty(window, 'scrollY', {
        value: 0,
        writable: true,
      });
    });

    it('header remains visible when page is scrolled', () => {
      renderApp();
      const header = getSiteHeader();

      expect(header).toBeInTheDocument();
      expect(header).toHaveStyle({ position: 'sticky' });
      expect(header).toHaveStyle({ top: '0' });
    });

    it('header maintains sticky position with high z-index', () => {
      renderApp();
      const header = getSiteHeader();

      expect(header).toHaveStyle({ zIndex: '1000' });
    });

    it('header stays at top of viewport', () => {
      renderApp();
      const header = getSiteHeader();

      expect(header).toHaveStyle({ top: '0' });
      expect(header).toHaveStyle({ position: 'sticky' });
    });
  });

  describe('Navigation within full app context', () => {
    it('renders navigation within Layout', () => {
      renderApp();
      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();
    });

    it('logo is accessible within app', () => {
      renderApp();
      const logos = screen.getAllByText('MirDB');
      expect(logos.length).toBeGreaterThan(0);
      const navLogo = screen.getByRole('link', { name: /MirDB home/i });
      expect(navLogo).toBeInTheDocument();
    });

    it('all navigation sections are rendered on page', () => {
      renderApp();

      const heroSection = document.getElementById('hero');
      const featuresSection = document.getElementById('features');
      const usageSection = document.getElementById('usage');
      const gettingStartedSection = document.getElementById('getting-started');

      expect(heroSection).toBeInTheDocument();
      expect(featuresSection).toBeInTheDocument();
      expect(usageSection).toBeInTheDocument();
      expect(gettingStartedSection).toBeInTheDocument();
    });

    it('navigation links point to existing sections', () => {
      renderApp();

      const featuresLink = screen.getByRole('link', { name: 'Features' });
      const usageLink = screen.getByRole('link', { name: 'Usage' });
      const gettingStartedLink = screen.getByRole('link', { name: 'Getting Started' });

      expect(featuresLink).toHaveAttribute('href', '#features');
      expect(usageLink).toHaveAttribute('href', '#usage');
      expect(gettingStartedLink).toHaveAttribute('href', '#getting-started');
    });
  });

  describe('GitHub link integration', () => {
    it('GitHub link is accessible from header', () => {
      renderApp();
      const header = getSiteHeader();
      const githubLinks = screen.getAllByRole('link', { name: /github/i });
      const headerGithubLink = githubLinks.find(link => header.contains(link));

      expect(headerGithubLink).toBeDefined();
      expect(header).toContainElement(headerGithubLink!);
    });

    it('GitHub link has correct URL', () => {
      renderApp();
      const header = getSiteHeader();
      const githubLinks = screen.getAllByRole('link', { name: /github/i });
      const headerGithubLink = githubLinks.find(link => header.contains(link));

      expect(headerGithubLink).toHaveAttribute('href', GITHUB_URL);
      expect(headerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    it('GitHub link opens in new tab', () => {
      renderApp();
      const header = getSiteHeader();
      const githubLinks = screen.getAllByRole('link', { name: /github/i });
      const headerGithubLink = githubLinks.find(link => header.contains(link));

      expect(headerGithubLink).toHaveAttribute('target', '_blank');
      expect(headerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  describe('Smooth scroll behavior', () => {
    it('clicking navigation link triggers smooth scroll', () => {
      const scrollIntoViewMock = vi.fn();

      renderApp();

      const featuresSection = document.getElementById('features');
      if (featuresSection) {
        featuresSection.scrollIntoView = scrollIntoViewMock;
      }

      const featuresLink = screen.getAllByRole('link', { name: 'Features' })[0];
      fireEvent.click(featuresLink);

      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
    });
  });

  describe('Header within Layout context', () => {
    it('header is a child of Layout', () => {
      renderApp();
      const header = getSiteHeader();
      const main = screen.getByRole('main');

      expect(header.parentElement).toBe(main.parentElement);
    });

    it('header appears before main content', () => {
      renderApp();
      const header = getSiteHeader();
      const main = screen.getByRole('main');

      const parent = header.parentElement;
      if (parent) {
        const children = Array.from(parent.children);
        const headerIndex = children.indexOf(header);
        const mainIndex = children.indexOf(main);
        expect(headerIndex).toBeLessThan(mainIndex);
      }
    });
  });
});

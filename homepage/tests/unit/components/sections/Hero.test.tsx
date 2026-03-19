/**
 * Unit tests for Hero component.
 * Owner: Scenario 1 - Hero Section Display
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { Hero } from '@/components/sections/Hero';

// Mock next/image
jest.mock('next/image', () => ({
  __esModule: true,
  default: ({ priority, ...props }: { alt: string; src: string; priority?: boolean; [key: string]: unknown }) => {
    // eslint-disable-next-line @next/next/no-img-element, jsx-a11y/alt-text
    return <img data-priority={priority} {...props} />;
  },
}));

describe('Hero Component', () => {
  // Test Case 1: Logo image renders with alt text 'MirDB'
  describe('TC1: Logo renders with correct alt text', () => {
    it('should render the MirDB logo with alt text "MirDB"', () => {
      render(<Hero />);

      const logo = screen.getByAltText('MirDB');
      expect(logo).toBeInTheDocument();
      expect(logo).toHaveAttribute('src', '/images/logo.svg');
    });

    it('should render logo in an image element', () => {
      render(<Hero />);

      const logo = screen.getByAltText('MirDB');
      expect(logo.tagName.toLowerCase()).toBe('img');
    });
  });

  // Test Case 2: Tagline text is displayed
  describe('TC2: Tagline is displayed correctly', () => {
    it('should display the tagline "Persistent Key-Value Store with Memcached Compatibility"', () => {
      render(<Hero />);

      const tagline = screen.getByText(
        'Persistent Key-Value Store with Memcached Compatibility'
      );
      expect(tagline).toBeInTheDocument();
    });

    it('should display tagline in a paragraph element', () => {
      render(<Hero />);

      const tagline = screen.getByText(
        'Persistent Key-Value Store with Memcached Compatibility'
      );
      expect(tagline.tagName.toLowerCase()).toBe('p');
    });
  });

  // Test for project name heading
  describe('Project name heading', () => {
    it('should display MirDB as the main heading', () => {
      render(<Hero />);

      const heading = screen.getByRole('heading', { name: 'MirDB', level: 1 });
      expect(heading).toBeInTheDocument();
    });
  });

  // Test for CTA buttons
  describe('CTA buttons', () => {
    it('should render "Get Started" button', () => {
      render(<Hero />);

      const getStartedButton = screen.getByRole('link', { name: 'Get Started' });
      expect(getStartedButton).toBeInTheDocument();
    });

    it('should render "View on GitHub" button', () => {
      render(<Hero />);

      const githubButton = screen.getByRole('link', { name: 'View on GitHub' });
      expect(githubButton).toBeInTheDocument();
    });

    it('should have "Get Started" button linked to #quick-start', () => {
      render(<Hero />);

      const getStartedButton = screen.getByRole('link', { name: 'Get Started' });
      expect(getStartedButton).toHaveAttribute('href', '#quick-start');
    });

    it('should have "View on GitHub" button linked to GitHub URL', () => {
      render(<Hero />);

      const githubButton = screen.getByRole('link', { name: 'View on GitHub' });
      expect(githubButton).toHaveAttribute(
        'href',
        'https://github.com/yetone/mirdb'
      );
    });

    it('should have "View on GitHub" button open in new tab', () => {
      render(<Hero />);

      const githubButton = screen.getByRole('link', { name: 'View on GitHub' });
      expect(githubButton).toHaveAttribute('target', '_blank');
      expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  // Test for section structure and accessibility
  describe('Section structure and accessibility', () => {
    it('should render a section with id="hero"', () => {
      render(<Hero />);

      const section = document.getElementById('hero');
      expect(section).toBeInTheDocument();
    });

    it('should have proper aria-labelledby on the section', () => {
      render(<Hero />);

      const section = document.getElementById('hero');
      expect(section).toHaveAttribute('aria-labelledby', 'hero-heading');
    });

    it('should have proper heading structure with id', () => {
      render(<Hero />);

      const heading = document.getElementById('hero-heading');
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent('MirDB');
    });
  });
});

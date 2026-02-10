/**
 * Hero Component Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for:
 * - Product name display
 * - Tagline visibility
 * - CTA buttons with correct hrefs
 * - Accessibility attributes
 */

import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { Hero } from '@/components/sections/Hero';
import {
  GITHUB_URL,
  DOCS_URL,
  SITE_TITLE,
  SITE_TAGLINE,
  SITE_DESCRIPTION,
} from '@/utils/constants';

describe('Hero Component', () => {
  describe('Test Case 1: Product name display', () => {
    it('displays the product name "MirDB" prominently', () => {
      render(<Hero />);
      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toHaveTextContent(SITE_TITLE);
      expect(heading).toHaveTextContent('MirDB');
    });

    it('renders the product name in a visible heading element', () => {
      render(<Hero />);
      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toBeInTheDocument();
      expect(heading).toBeVisible();
    });
  });

  describe('Test Case 2: Tagline visibility', () => {
    it('displays the tagline "A Persistent Key-Value Store with Memcached Protocol"', () => {
      render(<Hero />);
      const tagline = screen.getByText(SITE_TAGLINE);
      expect(tagline).toBeInTheDocument();
      expect(tagline).toHaveTextContent(
        'A Persistent Key-Value Store with Memcached Protocol'
      );
    });

    it('renders the tagline in a visible element', () => {
      render(<Hero />);
      const tagline = screen.getByText(SITE_TAGLINE);
      expect(tagline).toBeVisible();
    });
  });

  describe('Test Case 3: CTA buttons', () => {
    it('renders primary CTA button linking to GitHub repository', () => {
      render(<Hero />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toBeInTheDocument();
      expect(githubLink).toHaveAttribute('href', GITHUB_URL);
      expect(githubLink).toHaveAttribute(
        'href',
        'https://github.com/yetone/mirdb'
      );
    });

    it('renders secondary CTA button linking to documentation', () => {
      render(<Hero />);
      const docsLink = screen.getByRole('link', { name: /get started/i });
      expect(docsLink).toBeInTheDocument();
      expect(docsLink).toHaveAttribute('href', DOCS_URL);
    });

    it('opens GitHub link in new tab with security attributes', () => {
      render(<Hero />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('target', '_blank');
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  describe('Value proposition', () => {
    it('displays the value proposition description', () => {
      render(<Hero />);
      const description = screen.getByText(SITE_DESCRIPTION);
      expect(description).toBeInTheDocument();
      expect(description).toBeVisible();
    });
  });

  describe('Accessibility', () => {
    it('has proper aria-label on the hero section', () => {
      render(<Hero />);
      const section = screen.getByRole('region', { name: /hero section/i });
      expect(section).toBeInTheDocument();
    });

    it('has accessible names on CTA buttons', () => {
      render(<Hero />);
      // Button accessible names are computed from text content
      const githubButton = screen.getByRole('link', {
        name: /view on github/i,
      });
      const docsButton = screen.getByRole('link', {
        name: /get started/i,
      });
      expect(githubButton).toBeInTheDocument();
      expect(docsButton).toBeInTheDocument();
    });

    it('has decorative elements marked as aria-hidden', () => {
      const { container } = render(<Hero />);
      const decorativeElements = container.querySelectorAll('[aria-hidden="true"]');
      expect(decorativeElements.length).toBeGreaterThan(0);
    });
  });

  describe('Layout structure', () => {
    it('renders as a section element with id "hero"', () => {
      render(<Hero />);
      const section = document.getElementById('hero');
      expect(section).toBeInTheDocument();
      expect(section?.tagName.toLowerCase()).toBe('section');
    });

    it('contains both CTA buttons in the same container', () => {
      render(<Hero />);
      const githubLink = screen.getByRole('link', { name: /github/i });
      const docsLink = screen.getByRole('link', { name: /get started/i });

      expect(githubLink.parentElement).toBe(docsLink.parentElement);
    });
  });
});

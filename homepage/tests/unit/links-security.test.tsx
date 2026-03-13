/**
 * External Links Security Tests
 * Owner: Scenario 14 - External Links Security
 *
 * Verifies that all external links have proper security attributes
 * to prevent reverse tabnapping attacks:
 * - target="_blank" for opening in new tab
 * - rel="noopener noreferrer" for preventing window.opener access
 */

import { render, screen, within } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { Footer } from '../../src/components/layout/Footer';
import { Navbar } from '../../src/components/layout/Navbar';
import { Hero } from '../../src/components/sections/Hero';
import { StatusBadges } from '../../src/components/sections/StatusBadges';

describe('External Links Security', () => {
  describe('GitHub link security attributes', () => {
    it('Footer GitHub link has rel="noopener noreferrer"', () => {
      render(<Footer />);

      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('Footer GitHub link has target="_blank"', () => {
      render(<Footer />);

      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('target', '_blank');
    });

    it('Hero GitHub link has rel="noopener noreferrer"', () => {
      render(<Hero />);

      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('Hero GitHub link has target="_blank"', () => {
      render(<Hero />);

      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('target', '_blank');
    });

    it('Navbar GitHub link has rel="noopener noreferrer"', () => {
      render(<Navbar />);

      const githubLink = screen.getByTestId('nav-link-github');
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('Navbar GitHub link has target="_blank"', () => {
      render(<Navbar />);

      const githubLink = screen.getByTestId('nav-link-github');
      expect(githubLink).toHaveAttribute('target', '_blank');
    });
  });

  describe('Documentation link security attributes', () => {
    it('Navbar Documentation link does not have target="_blank" for internal anchor', () => {
      render(<Navbar />);

      const docsLink = screen.getByTestId('nav-link-documentation');
      // Internal links (anchors) should NOT have target="_blank"
      expect(docsLink).not.toHaveAttribute('target', '_blank');
    });

    it('Navbar Documentation link does not have rel attribute for internal anchor', () => {
      render(<Navbar />);

      const docsLink = screen.getByTestId('nav-link-documentation');
      // Internal links should NOT have rel="noopener noreferrer"
      expect(docsLink).not.toHaveAttribute('rel');
    });

    it('Hero Get Started link is internal and does not have security attributes', () => {
      render(<Hero />);

      const getStartedLink = screen.getByRole('link', { name: /get started/i });
      // Internal links should NOT have target="_blank" or rel
      expect(getStartedLink).not.toHaveAttribute('target', '_blank');
    });
  });

  describe('CircleCI badge link security attributes', () => {
    it('CircleCI badge link has rel="noopener noreferrer"', () => {
      render(<StatusBadges />);

      const circleCiLink = screen.getByRole('link');
      expect(circleCiLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('CircleCI badge link has target="_blank"', () => {
      render(<StatusBadges />);

      const circleCiLink = screen.getByRole('link');
      expect(circleCiLink).toHaveAttribute('target', '_blank');
    });
  });

  describe('All external links have target="_blank"', () => {
    it('Footer external links have target="_blank"', () => {
      render(<Footer />);

      const footer = screen.getByTestId('footer');
      const externalLinks = within(footer).getAllByRole('link');

      externalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // Check if it's an external link (starts with http)
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('target', '_blank');
        }
      });
    });

    it('Hero external links have target="_blank"', () => {
      render(<Hero />);

      const hero = screen.getByTestId('hero');
      const links = within(hero).getAllByRole('link');

      links.forEach((link) => {
        const href = link.getAttribute('href');
        // Check if it's an external link (starts with http)
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('target', '_blank');
        }
      });
    });

    it('StatusBadges external links have target="_blank"', () => {
      render(<StatusBadges />);

      const section = screen.getByTestId('status-badges');
      const links = within(section).getAllByRole('link');

      links.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('target', '_blank');
        }
      });
    });

    it('Navbar external links have target="_blank"', () => {
      render(<Navbar />);

      const navbar = screen.getByTestId('navbar');
      const links = within(navbar).getAllByRole('link');

      links.forEach((link) => {
        const href = link.getAttribute('href');
        // External links should have target="_blank"
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('target', '_blank');
        }
      });
    });
  });

  describe('All external links have rel="noopener noreferrer"', () => {
    it('All Footer external links have rel="noopener noreferrer"', () => {
      render(<Footer />);

      const footer = screen.getByTestId('footer');
      const links = within(footer).getAllByRole('link');

      links.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        }
      });
    });

    it('All Hero external links have rel="noopener noreferrer"', () => {
      render(<Hero />);

      const hero = screen.getByTestId('hero');
      const links = within(hero).getAllByRole('link');

      links.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        }
      });
    });

    it('All StatusBadges external links have rel="noopener noreferrer"', () => {
      render(<StatusBadges />);

      const section = screen.getByTestId('status-badges');
      const links = within(section).getAllByRole('link');

      links.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        }
      });
    });

    it('All Navbar external links have rel="noopener noreferrer"', () => {
      render(<Navbar />);

      const navbar = screen.getByTestId('navbar');
      const links = within(navbar).getAllByRole('link');

      links.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('http')) {
          expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        }
      });
    });
  });

  describe('Link URL validation', () => {
    it('GitHub link points to correct URL', () => {
      render(<Footer />);

      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    it('Hero GitHub link points to correct URL', () => {
      render(<Hero />);

      const githubLink = screen.getByRole('link', { name: /github/i });
      expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    it('CircleCI badge link points to correct URL', () => {
      render(<StatusBadges />);

      const circleCiLink = screen.getByRole('link');
      expect(circleCiLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    });

    it('Navbar GitHub link points to correct URL', () => {
      render(<Navbar />);

      const githubLink = screen.getByTestId('nav-link-github');
      expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });
  });
});

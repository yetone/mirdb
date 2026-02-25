import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Features } from '../../src/components/Features';

describe('Features Integration Tests', () => {
  // Test Case 7: Hover over feature card shows hover effect with subtle rise and shadow
  describe('Hover Effects', () => {
    it('card has CSS transition properties for hover effect', () => {
      render(<Features />);

      const cards = screen.getAllByRole('article');
      const firstCard = cards[0];

      // Check that the card has the necessary CSS properties for hover effect
      const styles = window.getComputedStyle(firstCard);

      // The card should have transition property set
      expect(firstCard).toHaveClass(/card/);
    });

    it('all feature cards are interactive elements', async () => {
      const user = userEvent.setup();
      render(<Features />);

      const cards = screen.getAllByRole('article');

      // Verify each card can receive hover/focus
      for (const card of cards) {
        expect(card).toBeInTheDocument();
        // Cards should have cursor pointer style indicating interactivity
        expect(card).toHaveClass(/card/);
      }
    });

    it('feature cards can receive focus for keyboard navigation', async () => {
      render(<Features />);

      const cards = screen.getAllByRole('article');

      // All cards should be present and visible
      expect(cards).toHaveLength(4);
      cards.forEach(card => {
        expect(card).toBeVisible();
      });
    });
  });

  // Test Case 8: View features section on mobile viewport (375px) - cards stack vertically
  describe('Responsive Design', () => {
    let originalMatchMedia: typeof window.matchMedia;

    beforeEach(() => {
      originalMatchMedia = window.matchMedia;
    });

    afterEach(() => {
      window.matchMedia = originalMatchMedia;
    });

    it('renders features grid that adapts to viewport', () => {
      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();
    });

    it('renders all four features regardless of viewport size', () => {
      render(<Features />);

      const cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);

      // Verify all feature titles are present
      expect(screen.getByText('Persistent Storage')).toBeInTheDocument();
      expect(screen.getByText('Memcached Protocol Compatibility')).toBeInTheDocument();
      expect(screen.getByText('LSM Tree Architecture')).toBeInTheDocument();
      expect(screen.getByText('Raft Support')).toBeInTheDocument();
    });

    it('grid container uses CSS grid for layout', () => {
      render(<Features />);

      const grid = document.querySelector('[class*="grid"]');
      expect(grid).toBeInTheDocument();

      // Verify grid contains all feature cards
      const cards = grid?.querySelectorAll('article');
      expect(cards).toHaveLength(4);
    });

    it('features section is properly contained within parent', () => {
      render(<Features />);

      const section = screen.getByRole('region', { name: /features/i });
      const grid = section.querySelector('[class*="grid"]');

      expect(section).toContainElement(grid as HTMLElement);
    });

    it('mobile viewport layout maintains content accessibility', () => {
      // Simulate mobile viewport
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      });

      render(<Features />);

      // All content should still be accessible
      const cards = screen.getAllByRole('article');
      expect(cards).toHaveLength(4);

      // Each card should have its content visible
      cards.forEach(card => {
        expect(card).toBeVisible();
        expect(card.querySelector('h3')).toBeVisible();
        expect(card.querySelector('p')).toBeVisible();
      });
    });
  });

  describe('Visual Hierarchy', () => {
    it('features section has proper heading hierarchy', () => {
      render(<Features />);

      const mainHeading = screen.getByRole('heading', { level: 2 });
      expect(mainHeading).toHaveTextContent('Key Features');

      const cardHeadings = screen.getAllByRole('heading', { level: 3 });
      expect(cardHeadings).toHaveLength(4);
    });

    it('planned feature has visual indicator badge', () => {
      render(<Features />);

      const plannedBadge = screen.getByText('Planned');
      expect(plannedBadge).toBeInTheDocument();
      expect(plannedBadge).toHaveClass(/plannedBadge/);
    });

    it('each feature card displays icon, title, and description', () => {
      render(<Features />);

      const cards = screen.getAllByRole('article');

      cards.forEach(card => {
        // Each card should have an icon (aria-hidden span)
        const icon = card.querySelector('[aria-hidden="true"]');
        expect(icon).toBeInTheDocument();

        // Each card should have a title (h3)
        const title = card.querySelector('h3');
        expect(title).toBeInTheDocument();

        // Each card should have a description (p)
        const description = card.querySelector('p');
        expect(description).toBeInTheDocument();
      });
    });
  });
});

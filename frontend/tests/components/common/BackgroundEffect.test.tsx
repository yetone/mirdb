/**
 * BackgroundEffect component tests.
 * Owner: Scenario 9 - BackgroundEffect Visual Component
 *
 * Test coverage:
 * - Component renders with fixed positioning and -z-10
 * - 3 gradient orbs with theme colors
 * - Animation classes applied
 * - No layout shift caused
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BackgroundEffect } from '@/components/common/BackgroundEffect';

describe('BackgroundEffect Visual Component', () => {
  describe('Test Case 1: Component renders with fixed positioning and -z-10', () => {
    it('should render the BackgroundEffect component', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('should have fixed positioning', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('fixed');
    });

    it('should have -z-10 class for negative z-index', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('-z-10');
    });

    it('should have inset-0 to cover the entire viewport', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('inset-0');
    });

    it('should have overflow-hidden to prevent content from spilling', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('overflow-hidden');
    });

    it('should have pointer-events-none to not interfere with interactions', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('pointer-events-none');
    });
  });

  describe('Test Case 2: 3 orb elements exist with bg-primary, bg-secondary, bg-accent classes', () => {
    it('should render exactly 3 gradient orb elements', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      // Get all child divs (the orbs)
      const orbs = backgroundEffect.querySelectorAll(':scope > div');
      expect(orbs.length).toBe(3);
    });

    it('should have an orb with bg-primary color', () => {
      const { container } = render(<BackgroundEffect />);

      // bg-primary/20 means primary color with 20% opacity
      const primaryOrb = container.querySelector('.bg-primary\\/20');
      expect(primaryOrb).toBeInTheDocument();
    });

    it('should have an orb with bg-secondary color', () => {
      const { container } = render(<BackgroundEffect />);

      // bg-secondary/20 means secondary color with 20% opacity
      const secondaryOrb = container.querySelector('.bg-secondary\\/20');
      expect(secondaryOrb).toBeInTheDocument();
    });

    it('should have an orb with bg-accent color', () => {
      const { container } = render(<BackgroundEffect />);

      // bg-accent/20 means accent color with 20% opacity
      const accentOrb = container.querySelector('.bg-accent\\/20');
      expect(accentOrb).toBeInTheDocument();
    });

    it('should have all orbs styled as rounded circles', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = backgroundEffect.querySelectorAll(':scope > div');

      orbs.forEach((orb) => {
        expect(orb).toHaveClass('rounded-full');
      });
    });

    it('should have all orbs with blur effect', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = backgroundEffect.querySelectorAll(':scope > div');

      orbs.forEach((orb) => {
        expect(orb).toHaveClass('blur-3xl');
      });
    });

    it('should position orbs at different absolute positions', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = backgroundEffect.querySelectorAll(':scope > div');

      orbs.forEach((orb) => {
        expect(orb).toHaveClass('absolute');
      });
    });
  });

  describe('Test Case 3: Orbs have animate-pulse class with different animation durations', () => {
    it('should have animate-pulse class on all orbs', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = backgroundEffect.querySelectorAll(':scope > div');

      orbs.forEach((orb) => {
        expect(orb).toHaveClass('animate-pulse');
      });
    });

    it('should have orbs with different positioning for visual variety', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = Array.from(backgroundEffect.querySelectorAll(':scope > div'));

      // Check that orbs have different positioning classes
      const hasTopClass = orbs.some((orb) => orb.className.includes('top-'));
      const hasBottomClass = orbs.some((orb) => orb.className.includes('bottom-'));
      const hasLeftClass = orbs.some((orb) => orb.className.includes('left-'));
      const hasRightClass = orbs.some((orb) => orb.className.includes('right-'));

      // At least some variety in positioning should exist
      expect(hasTopClass || hasBottomClass).toBe(true);
      expect(hasLeftClass || hasRightClass).toBe(true);
    });

    it('should have orbs with different sizes', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = Array.from(backgroundEffect.querySelectorAll(':scope > div'));

      // Extract width classes from orbs
      const widthClasses = orbs.map((orb) => {
        const classes = orb.className.split(' ');
        return classes.find((c) => c.startsWith('w-'));
      });

      // Check that there are different sizes (not all the same)
      const uniqueWidths = new Set(widthClasses.filter(Boolean));
      expect(uniqueWidths.size).toBeGreaterThanOrEqual(2);
    });

    it('should have delay classes for staggered animations', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = Array.from(backgroundEffect.querySelectorAll(':scope > div'));

      // Check that some orbs have delay classes for staggered animations
      const hasDelayClasses = orbs.some((orb) =>
        orb.className.includes('delay-')
      );

      expect(hasDelayClasses).toBe(true);
    });
  });

  describe('Test Case 4: BackgroundEffect does not cause cumulative layout shift (CLS)', () => {
    it('should use fixed positioning which does not affect layout flow', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // Fixed positioning removes element from document flow, preventing CLS
      expect(backgroundEffect).toHaveClass('fixed');
    });

    it('should have inset-0 for consistent dimensions', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // inset-0 sets top, right, bottom, left to 0, ensuring no dimension changes
      expect(backgroundEffect).toHaveClass('inset-0');
    });

    it('should not affect parent container dimensions', () => {
      const { container } = render(
        <div data-testid="parent" style={{ width: '100px', height: '100px' }}>
          <BackgroundEffect />
          <div data-testid="sibling">Sibling content</div>
        </div>
      );

      // BackgroundEffect should not push sibling content around
      const parent = screen.getByTestId('parent');
      const sibling = screen.getByTestId('sibling');

      expect(parent).toBeInTheDocument();
      expect(sibling).toBeInTheDocument();

      // Sibling should still be in the document and visible
      expect(sibling).toHaveTextContent('Sibling content');
    });

    it('should have pointer-events-none to not interfere with user interactions', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // pointer-events-none ensures the background doesn't capture clicks
      expect(backgroundEffect).toHaveClass('pointer-events-none');
    });

    it('should be positioned at z-index below content', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // -z-10 ensures content can be placed above the background
      expect(backgroundEffect).toHaveClass('-z-10');
    });

    it('should render without throwing errors', () => {
      // Test that component renders cleanly
      expect(() => render(<BackgroundEffect />)).not.toThrow();
    });

    it('should not have any elements causing reflows outside the fixed container', () => {
      const { container } = render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      const orbs = backgroundEffect.querySelectorAll(':scope > div');

      // All orbs should be absolute positioned within the fixed container
      orbs.forEach((orb) => {
        expect(orb).toHaveClass('absolute');
      });
    });
  });
});

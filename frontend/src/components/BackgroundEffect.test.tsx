import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import BackgroundEffect from './BackgroundEffect';

describe('BackgroundEffect Component', () => {
  describe('Rendering', () => {
    it('renders the background effect container', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('renders with custom data-testid', () => {
      render(<BackgroundEffect data-testid="custom-background" />);

      const backgroundEffect = screen.getByTestId('custom-background');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('renders three animated orbs', () => {
      render(<BackgroundEffect />);

      expect(screen.getByTestId('background-effect-orb-1')).toBeInTheDocument();
      expect(screen.getByTestId('background-effect-orb-2')).toBeInTheDocument();
      expect(screen.getByTestId('background-effect-orb-3')).toBeInTheDocument();
    });

    it('applies custom className', () => {
      render(<BackgroundEffect className="custom-class" />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('custom-class');
    });
  });

  describe('Accessibility', () => {
    it('has aria-hidden="true" to hide from screen readers', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });

    it('has pointer-events-none to prevent interaction', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('pointer-events-none');
    });
  });

  describe('Positioning and z-index', () => {
    it('has absolute positioning for overlay effect', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('absolute');
    });

    it('covers the full container with inset-0', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('inset-0');
    });

    it('has z-index 0 to stay behind content', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveStyle({ zIndex: '0' });
    });

    it('has overflow hidden to contain animations', () => {
      render(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveClass('overflow-hidden');
    });
  });

  describe('Animation elements', () => {
    it('orbs have blur effect classes', () => {
      render(<BackgroundEffect />);

      const orb1 = screen.getByTestId('background-effect-orb-1');
      const orb2 = screen.getByTestId('background-effect-orb-2');
      const orb3 = screen.getByTestId('background-effect-orb-3');

      expect(orb1).toHaveClass('blur-3xl');
      expect(orb2).toHaveClass('blur-3xl');
      expect(orb3).toHaveClass('blur-3xl');
    });

    it('orbs have rounded-full for circular shape', () => {
      render(<BackgroundEffect />);

      const orb1 = screen.getByTestId('background-effect-orb-1');
      const orb2 = screen.getByTestId('background-effect-orb-2');
      const orb3 = screen.getByTestId('background-effect-orb-3');

      expect(orb1).toHaveClass('rounded-full');
      expect(orb2).toHaveClass('rounded-full');
      expect(orb3).toHaveClass('rounded-full');
    });

    it('orbs use theme colors', () => {
      render(<BackgroundEffect />);

      const orb1 = screen.getByTestId('background-effect-orb-1');
      const orb2 = screen.getByTestId('background-effect-orb-2');
      const orb3 = screen.getByTestId('background-effect-orb-3');

      expect(orb1).toHaveClass('bg-primary/20');
      expect(orb2).toHaveClass('bg-secondary/20');
      expect(orb3).toHaveClass('bg-accent/15');
    });

    it('orbs have absolute positioning', () => {
      render(<BackgroundEffect />);

      const orb1 = screen.getByTestId('background-effect-orb-1');
      const orb2 = screen.getByTestId('background-effect-orb-2');
      const orb3 = screen.getByTestId('background-effect-orb-3');

      expect(orb1).toHaveClass('absolute');
      expect(orb2).toHaveClass('absolute');
      expect(orb3).toHaveClass('absolute');
    });
  });
});

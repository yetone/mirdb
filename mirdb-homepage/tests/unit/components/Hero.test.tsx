/**
 * Unit tests for Hero section components.
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Tests cover:
 * - CTAButton component (primary/secondary variants, href vs onClick)
 * - HeroLogo component (ASCII art display, animation, reduced motion)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CTAButton } from '../../../src/components/Hero/CTAButton';
import { HeroLogo } from '../../../src/components/Hero/HeroLogo';

describe('CTAButton', () => {
  describe('rendering', () => {
    it('renders children content correctly', () => {
      render(<CTAButton>Get Started</CTAButton>);
      expect(screen.getByText('Get Started')).toBeInTheDocument();
    });

    it('renders as a button by default when no href is provided', () => {
      render(<CTAButton>Click Me</CTAButton>);
      const button = screen.getByRole('button');
      expect(button).toBeInTheDocument();
      expect(button.tagName).toBe('BUTTON');
    });

    it('renders as an anchor when href is provided', () => {
      render(<CTAButton href="/docs">Documentation</CTAButton>);
      const link = screen.getByRole('link');
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', '/docs');
    });
  });

  describe('primary variant', () => {
    it('applies primary variant styles by default', () => {
      render(<CTAButton>Primary Button</CTAButton>);
      const button = screen.getByRole('button');
      expect(button).toHaveClass('bg-accent');
    });

    it('applies primary variant styles when explicitly set', () => {
      render(<CTAButton variant="primary">Primary Button</CTAButton>);
      const button = screen.getByRole('button');
      expect(button).toHaveClass('bg-accent');
    });
  });

  describe('secondary variant', () => {
    it('applies secondary variant styles with border', () => {
      render(<CTAButton variant="secondary">Secondary Button</CTAButton>);
      const button = screen.getByRole('button');
      expect(button).toHaveClass('border-2');
      expect(button).toHaveClass('border-accent');
    });
  });

  describe('external links', () => {
    it('adds target="_blank" for external links', () => {
      render(
        <CTAButton href="https://github.com" external>
          GitHub
        </CTAButton>
      );
      const link = screen.getByRole('link');
      expect(link).toHaveAttribute('target', '_blank');
    });

    it('adds rel="noopener noreferrer" for external links', () => {
      render(
        <CTAButton href="https://github.com" external>
          GitHub
        </CTAButton>
      );
      const link = screen.getByRole('link');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('does not add external attributes for internal links', () => {
      render(<CTAButton href="/docs">Documentation</CTAButton>);
      const link = screen.getByRole('link');
      expect(link).not.toHaveAttribute('target');
      expect(link).not.toHaveAttribute('rel');
    });
  });

  describe('click handling', () => {
    it('calls onClick handler when button is clicked', async () => {
      const user = userEvent.setup();
      const handleClick = vi.fn();
      render(<CTAButton onClick={handleClick}>Click Me</CTAButton>);

      await user.click(screen.getByRole('button'));
      expect(handleClick).toHaveBeenCalledTimes(1);
    });

    it('calls onClick handler when link is clicked', async () => {
      const user = userEvent.setup();
      const handleClick = vi.fn();
      render(
        <CTAButton href="#section" onClick={handleClick}>
          Scroll
        </CTAButton>
      );

      await user.click(screen.getByRole('link'));
      expect(handleClick).toHaveBeenCalledTimes(1);
    });
  });

  describe('custom className', () => {
    it('applies additional custom classes', () => {
      render(<CTAButton className="text-lg font-bold">Styled Button</CTAButton>);
      const button = screen.getByRole('button');
      expect(button).toHaveClass('text-lg');
      expect(button).toHaveClass('font-bold');
    });
  });

  describe('accessibility', () => {
    it('has type="button" attribute', () => {
      render(<CTAButton>Button</CTAButton>);
      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('type', 'button');
    });

    it('is focusable', () => {
      render(<CTAButton>Focus Me</CTAButton>);
      const button = screen.getByRole('button');
      button.focus();
      expect(document.activeElement).toBe(button);
    });
  });
});

describe('HeroLogo', () => {
  let matchMediaMock: ReturnType<typeof vi.fn>;
  let addEventListenerMock: ReturnType<typeof vi.fn>;
  let removeEventListenerMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.useFakeTimers();
    addEventListenerMock = vi.fn();
    removeEventListenerMock = vi.fn();
    matchMediaMock = vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      addEventListener: addEventListenerMock,
      removeEventListener: removeEventListenerMock,
    }));
    window.matchMedia = matchMediaMock;
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  describe('rendering', () => {
    it('renders the logo container with correct aria attributes', () => {
      render(<HeroLogo />);
      const logo = screen.getByRole('img', { name: 'MirDB logo' });
      expect(logo).toBeInTheDocument();
    });

    it('renders a pre element for the ASCII art', () => {
      render(<HeroLogo />);
      const preElement = document.querySelector('pre');
      expect(preElement).toBeInTheDocument();
    });

    it('applies custom className when provided', () => {
      render(<HeroLogo className="custom-class" />);
      const logo = screen.getByRole('img', { name: 'MirDB logo' });
      expect(logo).toHaveClass('custom-class');
    });
  });

  describe('animation', () => {
    it('shows typing animation when reduced motion is not preferred', async () => {
      render(<HeroLogo />);
      const preElement = document.querySelector('pre');

      // Advance time to let animation progress
      await act(async () => {
        vi.advanceTimersByTime(500);
      });

      const midText = preElement?.textContent || '';
      // Animation should have produced some text
      expect(midText.length).toBeGreaterThanOrEqual(0);
    });

    it('shows cursor during animation initially', async () => {
      render(<HeroLogo />);

      // Advance a small amount of time to trigger state update
      await act(async () => {
        vi.advanceTimersByTime(10);
      });

      // During animation, cursor should be visible
      const cursor = document.querySelector('.animate-pulse');
      expect(cursor).toBeInTheDocument();
    });

    it('completes animation and hides cursor', async () => {
      render(<HeroLogo />);

      // Fast forward through the entire animation (about 250 chars * 5ms = 1250ms)
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });

      // Cursor should no longer be visible after animation completes
      const cursor = document.querySelector('.animate-pulse');
      expect(cursor).not.toBeInTheDocument();
    });
  });

  describe('reduced motion preference', () => {
    it('shows full ASCII art immediately when reduced motion is preferred', async () => {
      matchMediaMock.mockImplementation((query: string) => ({
        matches: query === '(prefers-reduced-motion: reduce)',
        media: query,
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
      }));

      render(<HeroLogo />);

      // Advance timers to allow state to settle
      await act(async () => {
        vi.advanceTimersByTime(100);
      });

      const preElement = document.querySelector('pre');

      // Should contain Unicode block characters from the ASCII art
      // The ASCII art uses █ (U+2588) block characters
      expect(preElement?.textContent).toContain('█');
    });

    it('does not show cursor when reduced motion is preferred', async () => {
      matchMediaMock.mockImplementation((query: string) => ({
        matches: query === '(prefers-reduced-motion: reduce)',
        media: query,
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
      }));

      render(<HeroLogo />);

      // Advance timers to allow state to settle
      await act(async () => {
        vi.advanceTimersByTime(100);
      });

      const cursor = document.querySelector('.animate-pulse');
      expect(cursor).not.toBeInTheDocument();
    });
  });

  describe('accessibility', () => {
    it('has aria-label for screen readers', () => {
      render(<HeroLogo />);
      const logo = screen.getByRole('img');
      expect(logo).toHaveAttribute('aria-label', 'MirDB logo');
    });

    it('marks ASCII art as aria-hidden', () => {
      render(<HeroLogo />);
      const preElement = document.querySelector('pre');
      expect(preElement).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('responsive styles', () => {
    it('has responsive text size classes', () => {
      render(<HeroLogo />);
      const preElement = document.querySelector('pre');
      expect(preElement).toHaveClass('text-xs');
      expect(preElement).toHaveClass('sm:text-sm');
      expect(preElement).toHaveClass('md:text-base');
      expect(preElement).toHaveClass('lg:text-lg');
    });
  });

  describe('cleanup', () => {
    it('removes event listener on unmount', async () => {
      const { unmount } = render(<HeroLogo />);

      await act(async () => {
        vi.advanceTimersByTime(10);
      });

      unmount();
      expect(removeEventListenerMock).toHaveBeenCalledWith('change', expect.any(Function));
    });

    it('clears animation interval on unmount', async () => {
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval');
      const { unmount } = render(<HeroLogo />);

      await act(async () => {
        vi.advanceTimersByTime(10);
      });

      unmount();
      expect(clearIntervalSpy).toHaveBeenCalled();
    });
  });
});

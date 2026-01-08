import { describe, it, expect, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';

// Mock useNavigate
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => vi.fn(),
  };
});

const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Scenario: Background Effect Integration
// Verify BackgroundEffect component is properly integrated in hero section
describe('Home - Background Effect Integration', () => {
  // Test Case 1: BackgroundEffect component is rendered in hero section
  describe('Test Case 1: Render Home component hero section - BackgroundEffect component is rendered', () => {
    it('renders BackgroundEffect component within hero section', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('BackgroundEffect is a child of hero section', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const backgroundEffect = within(heroSection).getByTestId('hero-background-effect');

      expect(backgroundEffect).toBeInTheDocument();
    });

    it('renders animated orbs within BackgroundEffect', () => {
      renderHome();

      expect(screen.getByTestId('background-effect-orb-1')).toBeInTheDocument();
      expect(screen.getByTestId('background-effect-orb-2')).toBeInTheDocument();
      expect(screen.getByTestId('background-effect-orb-3')).toBeInTheDocument();
    });
  });

  // Test Case 2: Background animation does not block content visibility
  describe('Test Case 2: Render hero with BackgroundEffect - Background animation does not block content visibility', () => {
    it('hero content is still rendered and accessible', () => {
      renderHome();

      // Verify hero headline is rendered (animation initial state has opacity 0 but content is in DOM)
      const headline = screen.getByRole('heading', { level: 1, name: /shorten, share, track/i });
      expect(headline).toBeInTheDocument();
    });

    it('hero subheadline is rendered', () => {
      renderHome();

      const subheadline = screen.getByText(/Create short, memorable links/i);
      expect(subheadline).toBeInTheDocument();
    });

    it('hero CTA buttons are rendered and enabled (not blocked by background)', () => {
      renderHome();

      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i });
      const secondaryCTA = screen.getByRole('button', { name: /Learn More/i });

      // Content is rendered (animation may start with opacity 0, but it's in the DOM)
      expect(primaryCTA).toBeInTheDocument();
      expect(primaryCTA).not.toBeDisabled();

      expect(secondaryCTA).toBeInTheDocument();
      expect(secondaryCTA).not.toBeDisabled();
    });

    it('BackgroundEffect has pointer-events-none to not block interactions', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveClass('pointer-events-none');
    });

    it('BackgroundEffect is hidden from screen readers', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });
  });

  // Test Case 3: Background is behind hero content (lower z-index)
  describe('Test Case 3: Check BackgroundEffect z-index - Background is behind hero content (lower z-index)', () => {
    it('BackgroundEffect has z-index 0', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveStyle({ zIndex: '0' });
    });

    it('hero animated container has z-index higher than BackgroundEffect', () => {
      renderHome();

      const heroAnimatedContainer = screen.getByTestId('hero-animated-container');
      expect(heroAnimatedContainer).toHaveStyle({ zIndex: '1' });
    });

    it('hero section has relative positioning for proper z-index stacking', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('relative');
    });

    it('hero content container has relative positioning', () => {
      renderHome();

      const heroAnimatedContainer = screen.getByTestId('hero-animated-container');
      expect(heroAnimatedContainer).toHaveClass('relative');
    });

    it('BackgroundEffect has absolute positioning', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveClass('absolute');
    });

    it('BackgroundEffect covers full hero section with inset-0', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveClass('inset-0');
    });
  });

  // Additional visual enhancement tests
  describe('Visual enhancement tests', () => {
    it('BackgroundEffect uses theme colors for visual consistency', () => {
      renderHome();

      const orb1 = screen.getByTestId('background-effect-orb-1');
      const orb2 = screen.getByTestId('background-effect-orb-2');
      const orb3 = screen.getByTestId('background-effect-orb-3');

      expect(orb1).toHaveClass('bg-primary/20');
      expect(orb2).toHaveClass('bg-secondary/20');
      expect(orb3).toHaveClass('bg-accent/15');
    });

    it('BackgroundEffect orbs have blur effect for soft appearance', () => {
      renderHome();

      const orb1 = screen.getByTestId('background-effect-orb-1');
      const orb2 = screen.getByTestId('background-effect-orb-2');
      const orb3 = screen.getByTestId('background-effect-orb-3');

      expect(orb1).toHaveClass('blur-3xl');
      expect(orb2).toHaveClass('blur-3xl');
      expect(orb3).toHaveClass('blur-3xl');
    });

    it('BackgroundEffect has overflow hidden to contain animations', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveClass('overflow-hidden');
    });
  });
});

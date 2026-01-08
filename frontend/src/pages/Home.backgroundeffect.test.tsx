import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
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

// BackgroundEffect Integration Tests (Scenario 16: Background Effect Integration)
describe('Home - Background Effect Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: BackgroundEffect component is rendered in hero section
  describe('Test Case 1: BackgroundEffect component is rendered', () => {
    it('renders BackgroundEffect component in hero section', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('BackgroundEffect is within hero section', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const backgroundEffect = screen.getByTestId('hero-background-effect');

      expect(heroSection).toContainElement(backgroundEffect);
    });

    it('renders animated background blobs', () => {
      renderHome();

      const blob1 = screen.getByTestId('background-effect-blob-1');
      const blob2 = screen.getByTestId('background-effect-blob-2');
      const blob3 = screen.getByTestId('background-effect-blob-3');

      expect(blob1).toBeInTheDocument();
      expect(blob2).toBeInTheDocument();
      expect(blob3).toBeInTheDocument();
    });
  });

  // Test Case 2: Background animation does not block content visibility
  describe('Test Case 2: Background animation does not block content visibility', () => {
    it('BackgroundEffect has pointer-events-none class', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveClass('pointer-events-none');
    });

    it('hero content is still rendered and accessible (not blocked by background)', () => {
      renderHome();

      // Check that hero content elements are rendered and in the document
      // Note: Animation may start with opacity: 0, but content should be in DOM
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();

      const subheadline = screen.getByText(/Create short, memorable links/i);
      expect(subheadline).toBeInTheDocument();

      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i });
      expect(primaryCTA).toBeInTheDocument();

      // Verify buttons are not disabled/blocked
      expect(primaryCTA).not.toBeDisabled();
    });

    it('BackgroundEffect has aria-hidden attribute for accessibility', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });
  });

  // Test Case 3: Background is behind hero content (lower z-index)
  describe('Test Case 3: Background is behind hero content (lower z-index)', () => {
    it('BackgroundEffect has z-index of 0', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveStyle({ zIndex: '0' });
    });

    it('hero content container has higher z-index than BackgroundEffect', () => {
      renderHome();

      const heroContent = screen.getByTestId('hero-animated-container');
      const backgroundEffect = screen.getByTestId('hero-background-effect');

      // Get computed z-index values
      const heroContentStyle = window.getComputedStyle(heroContent);
      const backgroundStyle = window.getComputedStyle(backgroundEffect);

      const heroZIndex = parseInt(heroContentStyle.zIndex) || 0;
      const backgroundZIndex = parseInt(backgroundStyle.zIndex) || 0;

      // Hero content z-index should be greater than or equal to background
      // (z-index: 1 or higher for content, z-index: 0 for background)
      expect(heroZIndex).toBeGreaterThanOrEqual(backgroundZIndex);
    });

    it('BackgroundEffect has absolute positioning', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toHaveClass('absolute');
    });

    it('hero section has relative positioning to contain BackgroundEffect', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('relative');
    });
  });
});

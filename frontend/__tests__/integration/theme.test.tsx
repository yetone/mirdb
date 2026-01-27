/**
 * Theme Integration Tests.
 * Owner: Scenarios 10, 11, 12 - Theme Support
 *
 * Tests for dark mode, light mode, and theme switching functionality.
 * Verifies that all components render correctly with appropriate theme styling.
 */

import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ThemeProvider, useTheme } from '../../src/contexts/ThemeContext';
import { Home } from '../../src/pages/Home';
import { GlassMorphismCard } from '../../src/components/GlassMorphismCard';
import { FuturisticButton } from '../../src/components/FuturisticButton';

// Helper component to set theme for testing
const ThemeController: React.FC<{ theme: 'light' | 'dark' | 'cyberpunk' | 'synthwave'; children: React.ReactNode }> = ({ theme, children }) => {
  const ThemeSetter = () => {
    const { setTheme } = useTheme();
    React.useEffect(() => {
      setTheme(theme);
    }, [setTheme]);
    return null;
  };

  return (
    <>
      <ThemeSetter />
      {children}
    </>
  );
};

// Wrapper component with all necessary providers and theme control
const renderWithDarkTheme = (ui: React.ReactElement) => {
  // Mock localStorage to return dark theme
  const localStorageMock = {
    getItem: vi.fn(() => 'dark'),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
  };
  Object.defineProperty(window, 'localStorage', { value: localStorageMock, writable: true });

  return render(
    <ThemeProvider>
      <BrowserRouter>
        <ThemeController theme="dark">
          {ui}
        </ThemeController>
      </BrowserRouter>
    </ThemeProvider>
  );
};

describe('Scenario 10: Theme Support - Dark Mode', () => {
  beforeEach(() => {
    // Reset document theme attribute before each test
    document.documentElement.removeAttribute('data-theme');
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Page renders with dark background colors', () => {
    it('should set data-theme attribute to dark on document element', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('should render the landing page container', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        const landingPage = screen.getByTestId('landing-page');
        expect(landingPage).toBeInTheDocument();
      });
    });

    it('should render with min-h-screen for full page coverage', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        const landingPage = screen.getByTestId('landing-page');
        expect(landingPage).toHaveClass('min-h-screen');
      });
    });

    it('should render all major sections in dark mode', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // Check features section exists with dark-aware classes
        const featuresSection = screen.getByTestId('features-section');
        expect(featuresSection).toBeInTheDocument();

        // Check social proof section exists with dark-aware classes
        const socialProofSection = screen.getByTestId('social-proof-section');
        expect(socialProofSection).toBeInTheDocument();

        // Check footer exists
        const footer = screen.getByTestId('footer');
        expect(footer).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Text is legible with appropriate contrast on dark background', () => {
    it('should render heading text with base-content class for theme-aware coloring', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // Check that the hero headline uses theme-aware text color
        const headline = screen.getByRole('heading', { level: 1 });
        expect(headline).toHaveClass('text-base-content');
      });
    });

    it('should render subheadline with reduced opacity for visual hierarchy', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // The subheadline should use text-base-content/70 for proper contrast
        // Use a more specific selector - the hero subheadline contains "Gain powerful insights"
        const subheadline = screen.getByText(/Gain powerful insights with detailed analytics/i);
        expect(subheadline).toHaveClass('text-base-content/70');
      });
    });

    it('should render section headings with proper styling', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // Features section heading
        const featuresHeading = screen.getByRole('heading', { name: /Powerful Features/i });
        expect(featuresHeading).toBeInTheDocument();
        expect(featuresHeading).toHaveClass('font-bold');

        // How It Works heading
        const howItWorksHeading = screen.getByRole('heading', { name: /How It Works/i });
        expect(howItWorksHeading).toBeInTheDocument();
        expect(howItWorksHeading).toHaveClass('font-bold');
      });
    });

    it('should render feature descriptions with readable text', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        const featureDescription = screen.getByTestId('feature-description-0');
        expect(featureDescription).toHaveClass('text-base-content/70');
      });
    });
  });

  describe('Test Case 3: GlassMorphismCard components render with dark mode styling', () => {
    it('should render GlassMorphismCard with backdrop blur effect', async () => {
      renderWithDarkTheme(
        <GlassMorphismCard>
          <div>Test content</div>
        </GlassMorphismCard>
      );

      await waitFor(() => {
        const card = screen.getByTestId('glassmorphism-card');
        expect(card).toBeInTheDocument();
        expect(card).toHaveClass('backdrop-blur-md');
      });
    });

    it('should render GlassMorphismCard with theme-aware background', async () => {
      renderWithDarkTheme(
        <GlassMorphismCard>
          <div>Test content</div>
        </GlassMorphismCard>
      );

      await waitFor(() => {
        const card = screen.getByTestId('glassmorphism-card');
        // bg-base-100/30 provides semi-transparent background that adapts to theme
        expect(card).toHaveClass('bg-base-100/30');
      });
    });

    it('should render GlassMorphismCard with theme-aware border', async () => {
      renderWithDarkTheme(
        <GlassMorphismCard>
          <div>Test content</div>
        </GlassMorphismCard>
      );

      await waitFor(() => {
        const card = screen.getByTestId('glassmorphism-card');
        // border-base-content/10 provides subtle border that adapts to theme
        expect(card).toHaveClass('border-base-content/10');
      });
    });

    it('should render feature cards using GlassMorphismCard in dark mode', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // All GlassMorphismCards should be present with proper styling
        const cards = screen.getAllByTestId('glassmorphism-card');
        expect(cards.length).toBeGreaterThan(0);

        cards.forEach(card => {
          expect(card).toHaveClass('backdrop-blur-md');
          expect(card).toHaveClass('bg-base-100/30');
        });
      });
    });
  });

  describe('Test Case 4: FuturisticButton components render with dark mode styling', () => {
    it('should render FuturisticButton with primary variant', async () => {
      renderWithDarkTheme(
        <FuturisticButton variant="primary">
          Get Started
        </FuturisticButton>
      );

      await waitFor(() => {
        const button = screen.getByRole('button', { name: /Get Started/i });
        expect(button).toBeInTheDocument();
        expect(button).toHaveClass('btn');
        expect(button).toHaveClass('btn-primary');
      });
    });

    it('should render FuturisticButton with secondary variant', async () => {
      renderWithDarkTheme(
        <FuturisticButton variant="secondary">
          Secondary Action
        </FuturisticButton>
      );

      await waitFor(() => {
        const button = screen.getByRole('button', { name: /Secondary Action/i });
        expect(button).toHaveClass('btn-secondary');
      });
    });

    it('should render FuturisticButton with ghost variant', async () => {
      renderWithDarkTheme(
        <FuturisticButton variant="ghost">
          Ghost Action
        </FuturisticButton>
      );

      await waitFor(() => {
        const button = screen.getByRole('button', { name: /Ghost Action/i });
        expect(button).toHaveClass('btn-ghost');
      });
    });

    it('should render hero CTA buttons with proper styling in dark mode', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // Primary CTA (Get Started)
        const primaryButton = screen.getByRole('button', { name: /Get Started/i });
        expect(primaryButton).toBeInTheDocument();
        expect(primaryButton).toHaveClass('btn');
        expect(primaryButton).toHaveClass('btn-primary');
        expect(primaryButton).toHaveClass('btn-lg');

        // Login CTA button
        const loginButton = screen.getByRole('button', { name: /Login/i });
        expect(loginButton).toBeInTheDocument();
        expect(loginButton).toHaveClass('btn');
        expect(loginButton).toHaveClass('btn-lg');
      });
    });

    it('should render buttons with proper size classes', async () => {
      renderWithDarkTheme(
        <>
          <FuturisticButton size="sm">Small</FuturisticButton>
          <FuturisticButton size="md">Medium</FuturisticButton>
          <FuturisticButton size="lg">Large</FuturisticButton>
        </>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /Small/i })).toHaveClass('btn-sm');
        expect(screen.getByRole('button', { name: /Medium/i })).toHaveClass('btn-md');
        expect(screen.getByRole('button', { name: /Large/i })).toHaveClass('btn-lg');
      });
    });
  });

  describe('Dark mode comprehensive rendering', () => {
    it('should render footer with dark-aware styling', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        const footer = screen.getByTestId('footer');
        // Footer uses bg-base-200 which adapts to dark theme
        expect(footer).toHaveClass('bg-base-200');
        expect(footer).toHaveClass('border-base-300');
      });
    });

    it('should render footer links with theme-aware text colors', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        const homeLink = screen.getByTestId('footer-home-link');
        expect(homeLink).toHaveClass('text-base-content/70');
      });
    });

    it('should render how-it-works section with dark-aware background', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // How it works section uses bg-base-200/50 for subtle background
        const howItWorksSection = screen.getByRole('region', { name: /how it works/i });
        expect(howItWorksSection).toHaveClass('bg-base-200/50');
      });
    });

    it('should render social proof stat cards with dark-aware styling', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        const statCard = screen.getByTestId('stat-card-0');
        // Stat cards use bg-base-100/50 for semi-transparent background
        expect(statCard).toHaveClass('bg-base-100/50');
        expect(statCard).toHaveClass('border-base-content/5');
      });
    });

    it('should ensure all text elements maintain readability in dark mode', async () => {
      renderWithDarkTheme(<Home />);

      await waitFor(() => {
        // Check stat labels use theme-aware colors
        const statLabel = screen.getByTestId('stat-label-0');
        expect(statLabel).toHaveClass('text-base-content/70');

        // Check step descriptions in how-it-works
        const stepDescription = screen.getByTestId('step-1-description');
        expect(stepDescription).toHaveClass('text-base-content/70');
      });
    });
  });
});

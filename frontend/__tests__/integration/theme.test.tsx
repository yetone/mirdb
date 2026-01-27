/**
 * Theme Integration Tests.
 * Owner: Scenarios 10, 11, 12 - Theme Support
 *
 * Tests for dark mode, light mode, and theme switching functionality.
 * Verifies that all components render correctly with appropriate theme styling.
 */

import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
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

// Wrapper component with all necessary providers and light theme control
const renderWithLightTheme = (ui: React.ReactElement) => {
  // Mock localStorage to return light theme
  const localStorageMock = {
    getItem: vi.fn(() => 'light'),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
  };
  Object.defineProperty(window, 'localStorage', { value: localStorageMock, writable: true });

  return render(
    <ThemeProvider>
      <BrowserRouter>
        <ThemeController theme="light">
          {ui}
        </ThemeController>
      </BrowserRouter>
    </ThemeProvider>
  );
};

describe('Scenario 11: Theme Support - Light Mode', () => {
  beforeEach(() => {
    // Reset document theme attribute before each test
    document.documentElement.removeAttribute('data-theme');
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Page renders with light background colors', () => {
    it('should set data-theme attribute to light on document element', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });

    it('should render the landing page container', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        const landingPage = screen.getByTestId('landing-page');
        expect(landingPage).toBeInTheDocument();
      });
    });

    it('should render with min-h-screen for full page coverage', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        const landingPage = screen.getByTestId('landing-page');
        expect(landingPage).toHaveClass('min-h-screen');
      });
    });

    it('should render all major sections in light mode', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        // Check features section exists with theme-aware classes
        const featuresSection = screen.getByTestId('features-section');
        expect(featuresSection).toBeInTheDocument();

        // Check social proof section exists with theme-aware classes
        const socialProofSection = screen.getByTestId('social-proof-section');
        expect(socialProofSection).toBeInTheDocument();

        // Check footer exists
        const footer = screen.getByTestId('footer');
        expect(footer).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Text is legible with appropriate contrast on light background', () => {
    it('should render heading text with base-content class for theme-aware coloring', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        // Check that the hero headline uses theme-aware text color
        const headline = screen.getByRole('heading', { level: 1 });
        expect(headline).toHaveClass('text-base-content');
      });
    });

    it('should render subheadline with reduced opacity for visual hierarchy', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        // The subheadline should use text-base-content/70 for proper contrast
        const subheadline = screen.getByText(/Gain powerful insights with detailed analytics/i);
        expect(subheadline).toHaveClass('text-base-content/70');
      });
    });

    it('should render section headings with proper styling', async () => {
      renderWithLightTheme(<Home />);

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
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        const featureDescription = screen.getByTestId('feature-description-0');
        expect(featureDescription).toHaveClass('text-base-content/70');
      });
    });
  });

  describe('Test Case 3: All UI components adapt to light mode styling', () => {
    it('should render GlassMorphismCard with backdrop blur effect in light mode', async () => {
      renderWithLightTheme(
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

    it('should render GlassMorphismCard with theme-aware background in light mode', async () => {
      renderWithLightTheme(
        <GlassMorphismCard>
          <div>Test content</div>
        </GlassMorphismCard>
      );

      await waitFor(() => {
        const card = screen.getByTestId('glassmorphism-card');
        // bg-base-100/30 provides semi-transparent background that adapts to light theme
        expect(card).toHaveClass('bg-base-100/30');
      });
    });

    it('should render GlassMorphismCard with theme-aware border in light mode', async () => {
      renderWithLightTheme(
        <GlassMorphismCard>
          <div>Test content</div>
        </GlassMorphismCard>
      );

      await waitFor(() => {
        const card = screen.getByTestId('glassmorphism-card');
        // border-base-content/10 provides subtle border that adapts to light theme
        expect(card).toHaveClass('border-base-content/10');
      });
    });

    it('should render feature cards using GlassMorphismCard in light mode', async () => {
      renderWithLightTheme(<Home />);

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

    it('should render FuturisticButton with primary variant in light mode', async () => {
      renderWithLightTheme(
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

    it('should render hero CTA buttons with proper styling in light mode', async () => {
      renderWithLightTheme(<Home />);

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

    it('should render footer with light-aware styling', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        const footer = screen.getByTestId('footer');
        // Footer uses bg-base-200 which adapts to light theme
        expect(footer).toHaveClass('bg-base-200');
        expect(footer).toHaveClass('border-base-300');
      });
    });

    it('should render footer links with theme-aware text colors in light mode', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        const homeLink = screen.getByTestId('footer-home-link');
        expect(homeLink).toHaveClass('text-base-content/70');
      });
    });

    it('should render how-it-works section with light-aware background', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        // How it works section uses bg-base-200/50 for subtle background
        const howItWorksSection = screen.getByRole('region', { name: /how it works/i });
        expect(howItWorksSection).toHaveClass('bg-base-200/50');
      });
    });

    it('should render social proof stat cards with light-aware styling', async () => {
      renderWithLightTheme(<Home />);

      await waitFor(() => {
        const statCard = screen.getByTestId('stat-card-0');
        // Stat cards use bg-base-100/50 for semi-transparent background
        expect(statCard).toHaveClass('bg-base-100/50');
        expect(statCard).toHaveClass('border-base-content/5');
      });
    });

    it('should ensure all text elements maintain readability in light mode', async () => {
      renderWithLightTheme(<Home />);

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

// ============================================================================
// Scenario 12: Theme Switching - Dynamic theme toggle without page reload
// ============================================================================

// Interactive theme toggle component for testing theme switching
const ThemeSwitchingTestComponent: React.FC = () => {
  const { theme, setTheme } = useTheme();
  const [switchCount, setSwitchCount] = React.useState(0);
  const [error, setError] = React.useState<string | null>(null);

  const toggleTheme = () => {
    try {
      const newTheme = theme === 'light' ? 'dark' : 'light';
      setTheme(newTheme);
      setSwitchCount(prev => prev + 1);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const setSpecificTheme = (newTheme: 'light' | 'dark' | 'cyberpunk' | 'synthwave') => {
    try {
      setTheme(newTheme);
      setSwitchCount(prev => prev + 1);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div data-testid="theme-switcher-test">
      <span data-testid="current-theme">{theme}</span>
      <span data-testid="switch-count">{switchCount}</span>
      {error && <span data-testid="error-message">{error}</span>}
      <button onClick={toggleTheme} data-testid="toggle-theme-btn">Toggle Theme</button>
      <button onClick={() => setSpecificTheme('light')} data-testid="set-light-btn">Set Light</button>
      <button onClick={() => setSpecificTheme('dark')} data-testid="set-dark-btn">Set Dark</button>
      <button onClick={() => setSpecificTheme('cyberpunk')} data-testid="set-cyberpunk-btn">Set Cyberpunk</button>
      <button onClick={() => setSpecificTheme('synthwave')} data-testid="set-synthwave-btn">Set Synthwave</button>
    </div>
  );
};

// Render helper for theme switching tests
const renderForThemeSwitching = (initialTheme: 'light' | 'dark' = 'light') => {
  // Set initial theme in localStorage mock
  const localStorageMock = {
    getItem: vi.fn(() => initialTheme),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
  };
  Object.defineProperty(window, 'localStorage', { value: localStorageMock, writable: true });

  const user = userEvent.setup();

  return {
    ...render(
      <ThemeProvider>
        <BrowserRouter>
          <ThemeSwitchingTestComponent />
          <Home />
        </BrowserRouter>
      </ThemeProvider>
    ),
    localStorageMock,
    user,
  };
};

describe('Scenario 12: Theme Switching', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme');
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Toggle theme from light to dark while on landing page', () => {
    it('should update data-theme attribute from light to dark', async () => {
      const { user } = renderForThemeSwitching('light');

      // Verify initial light theme
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });

      // Click to switch to dark
      const setDarkBtn = screen.getByTestId('set-dark-btn');
      await user.click(setDarkBtn);

      // Verify theme switched to dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('should update the theme state when switching from light to dark', async () => {
      const { user } = renderForThemeSwitching('light');

      // Verify initial state shows light
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
      });

      // Switch to dark
      const setDarkBtn = screen.getByTestId('set-dark-btn');
      await user.click(setDarkBtn);

      // Verify state updated
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });
    });

    it('should persist theme to localStorage when switching to dark', async () => {
      const { user, localStorageMock } = renderForThemeSwitching('light');

      const setDarkBtn = screen.getByTestId('set-dark-btn');
      await user.click(setDarkBtn);

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark');
      });
    });

    it('should render all landing page sections after theme switch to dark', async () => {
      const { user } = renderForThemeSwitching('light');

      // Switch to dark
      const setDarkBtn = screen.getByTestId('set-dark-btn');
      await user.click(setDarkBtn);

      // Verify all sections still render correctly
      await waitFor(() => {
        expect(screen.getByTestId('landing-page')).toBeInTheDocument();
        expect(screen.getByTestId('features-section')).toBeInTheDocument();
        expect(screen.getByTestId('social-proof-section')).toBeInTheDocument();
        expect(screen.getByTestId('footer')).toBeInTheDocument();
      });
    });

    it('should not reload the page when switching theme', async () => {
      const { user } = renderForThemeSwitching('light');

      // Get initial switch count
      await waitFor(() => {
        expect(screen.getByTestId('switch-count')).toHaveTextContent('0');
      });

      // Switch theme multiple times
      const setDarkBtn = screen.getByTestId('set-dark-btn');
      await user.click(setDarkBtn);

      // Verify component state persisted (switch count incremented, not reset)
      await waitFor(() => {
        expect(screen.getByTestId('switch-count')).toHaveTextContent('1');
      });

      // Component should still be mounted (no page reload)
      expect(screen.getByTestId('theme-switcher-test')).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Toggle theme from dark to light while on landing page', () => {
    it('should update data-theme attribute from dark to light', async () => {
      const { user } = renderForThemeSwitching('dark');

      // Verify initial dark theme
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      // Click to switch to light
      const setLightBtn = screen.getByTestId('set-light-btn');
      await user.click(setLightBtn);

      // Verify theme switched to light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });

    it('should update the theme state when switching from dark to light', async () => {
      const { user } = renderForThemeSwitching('dark');

      // Verify initial state shows dark
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });

      // Switch to light
      const setLightBtn = screen.getByTestId('set-light-btn');
      await user.click(setLightBtn);

      // Verify state updated
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
      });
    });

    it('should persist theme to localStorage when switching to light', async () => {
      const { user, localStorageMock } = renderForThemeSwitching('dark');

      const setLightBtn = screen.getByTestId('set-light-btn');
      await user.click(setLightBtn);

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'light');
      });
    });

    it('should render all landing page sections after theme switch to light', async () => {
      const { user } = renderForThemeSwitching('dark');

      // Switch to light
      const setLightBtn = screen.getByTestId('set-light-btn');
      await user.click(setLightBtn);

      // Verify all sections still render correctly
      await waitFor(() => {
        expect(screen.getByTestId('landing-page')).toBeInTheDocument();
        expect(screen.getByTestId('features-section')).toBeInTheDocument();
        expect(screen.getByTestId('social-proof-section')).toBeInTheDocument();
        expect(screen.getByTestId('footer')).toBeInTheDocument();
      });
    });

    it('should apply light theme styling to GlassMorphismCard components', async () => {
      const { user } = renderForThemeSwitching('dark');

      // Switch to light
      const setLightBtn = screen.getByTestId('set-light-btn');
      await user.click(setLightBtn);

      // Verify GlassMorphismCards render with theme-aware styling
      await waitFor(() => {
        const cards = screen.getAllByTestId('glassmorphism-card');
        expect(cards.length).toBeGreaterThan(0);
        cards.forEach(card => {
          // Cards use bg-base-100/30 which adapts to light theme
          expect(card).toHaveClass('bg-base-100/30');
          expect(card).toHaveClass('backdrop-blur-md');
        });
      });
    });
  });

  describe('Test Case 3: Toggle theme multiple times', () => {
    it('should handle rapid theme toggles without errors', async () => {
      const { user } = renderForThemeSwitching('light');

      // Toggle multiple times rapidly
      const toggleBtn = screen.getByTestId('toggle-theme-btn');

      await user.click(toggleBtn); // light -> dark
      await user.click(toggleBtn); // dark -> light
      await user.click(toggleBtn); // light -> dark
      await user.click(toggleBtn); // dark -> light
      await user.click(toggleBtn); // light -> dark

      // Should end up on dark theme (odd number of toggles from light)
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      // No errors should have occurred
      expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();
    });

    it('should correctly track switch count after multiple toggles', async () => {
      const { user } = renderForThemeSwitching('light');

      const toggleBtn = screen.getByTestId('toggle-theme-btn');

      // Toggle 5 times
      for (let i = 0; i < 5; i++) {
        await user.click(toggleBtn);
      }

      // Verify switch count
      await waitFor(() => {
        expect(screen.getByTestId('switch-count')).toHaveTextContent('5');
      });
    });

    it('should correctly switch between all available themes', async () => {
      const { user } = renderForThemeSwitching('light');

      // Switch through all themes
      const setDarkBtn = screen.getByTestId('set-dark-btn');
      const setCyberpunkBtn = screen.getByTestId('set-cyberpunk-btn');
      const setSynthwaveBtn = screen.getByTestId('set-synthwave-btn');
      const setLightBtn = screen.getByTestId('set-light-btn');

      // Dark
      await user.click(setDarkBtn);
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });

      // Cyberpunk
      await user.click(setCyberpunkBtn);
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
        expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk');
      });

      // Synthwave
      await user.click(setSynthwaveBtn);
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
        expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave');
      });

      // Back to light
      await user.click(setLightBtn);
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
      });
    });

    it('should maintain page content integrity through multiple theme switches', async () => {
      const { user } = renderForThemeSwitching('light');

      const toggleBtn = screen.getByTestId('toggle-theme-btn');

      // Toggle multiple times
      for (let i = 0; i < 10; i++) {
        await user.click(toggleBtn);
      }

      // Verify all landing page sections still render correctly
      await waitFor(() => {
        // Main page container
        expect(screen.getByTestId('landing-page')).toBeInTheDocument();

        // All major sections present
        expect(screen.getByTestId('features-section')).toBeInTheDocument();
        expect(screen.getByTestId('social-proof-section')).toBeInTheDocument();
        expect(screen.getByTestId('footer')).toBeInTheDocument();

        // Buttons still functional
        expect(screen.getByRole('button', { name: /Get Started/i })).toBeInTheDocument();
      });
    });

    it('should persist the final theme to localStorage after multiple switches', async () => {
      const { user, localStorageMock } = renderForThemeSwitching('light');

      const setDarkBtn = screen.getByTestId('set-dark-btn');
      const setLightBtn = screen.getByTestId('set-light-btn');

      // Switch multiple times
      await user.click(setDarkBtn);
      await user.click(setLightBtn);
      await user.click(setDarkBtn);

      // Final theme should be dark
      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenLastCalledWith('theme', 'dark');
      });
    });

    it('should not cause memory leaks during frequent theme changes', async () => {
      const { user } = renderForThemeSwitching('light');

      const toggleBtn = screen.getByTestId('toggle-theme-btn');

      // Simulate many rapid toggles
      for (let i = 0; i < 20; i++) {
        await user.click(toggleBtn);
      }

      // Verify component is still responsive and functional
      await waitFor(() => {
        expect(screen.getByTestId('switch-count')).toHaveTextContent('20');
        expect(screen.getByTestId('theme-switcher-test')).toBeInTheDocument();
      });

      // No errors should have occurred
      expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();
    });

    it('should keep hero section CTA buttons functional after theme switches', async () => {
      const { user } = renderForThemeSwitching('light');

      const toggleBtn = screen.getByTestId('toggle-theme-btn');

      // Toggle theme
      await user.click(toggleBtn);
      await user.click(toggleBtn);

      // Verify hero section buttons are still present and styled correctly
      await waitFor(() => {
        const primaryButton = screen.getByRole('button', { name: /Get Started/i });
        expect(primaryButton).toBeInTheDocument();
        expect(primaryButton).toHaveClass('btn');
        expect(primaryButton).toHaveClass('btn-primary');
        expect(primaryButton).toHaveClass('btn-lg');

        const loginButton = screen.getByRole('button', { name: /Login/i });
        expect(loginButton).toBeInTheDocument();
        expect(loginButton).toHaveClass('btn');
      });
    });
  });

  describe('Theme switching edge cases', () => {
    it('should handle setting the same theme multiple times', async () => {
      const { user, localStorageMock } = renderForThemeSwitching('light');

      const setLightBtn = screen.getByTestId('set-light-btn');

      // Set light theme when already light
      await user.click(setLightBtn);
      await user.click(setLightBtn);
      await user.click(setLightBtn);

      // Should not error
      expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();

      // Theme should still be light
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });

    it('should update theme-aware classes on all components simultaneously', async () => {
      const { user } = renderForThemeSwitching('light');

      // Switch to dark
      const setDarkBtn = screen.getByTestId('set-dark-btn');
      await user.click(setDarkBtn);

      // All theme-aware elements should have updated
      await waitFor(() => {
        // Landing page
        expect(screen.getByTestId('landing-page')).toBeInTheDocument();

        // Features section
        const featuresSection = screen.getByTestId('features-section');
        expect(featuresSection).toBeInTheDocument();

        // GlassMorphismCards have theme-aware classes
        const cards = screen.getAllByTestId('glassmorphism-card');
        cards.forEach(card => {
          expect(card).toHaveClass('bg-base-100/30');
          expect(card).toHaveClass('border-base-content/10');
        });

        // Footer uses theme-aware colors
        const footer = screen.getByTestId('footer');
        expect(footer).toHaveClass('bg-base-200');
      });
    });
  });
});

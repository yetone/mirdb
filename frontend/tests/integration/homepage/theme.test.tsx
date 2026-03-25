/**
 * Theme Integration Tests
 * Owner: Scenario 8 - Theme System Support
 *
 * Tests homepage rendering with all 8 theme options and dynamic theme toggling.
 * Validates REQ-8, NFR-2, NFR-3 compliance.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, act, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React, { ReactNode } from 'react';
import { render } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider, useTheme, Theme } from '../../../src/contexts/ThemeContext';
import { Home } from '../../../src/pages/Home';
import { GlassMorphismCard } from '../../../src/components/GlassMorphismCard';
import { FuturisticButton } from '../../../src/components/FuturisticButton';
import { HeroSection } from '../../../src/components/homepage/HeroSection';
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection';

// Theme list as defined in PRD (REQ-8, NFR-2)
const ALL_THEMES: Theme[] = ['light', 'dark', 'system', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'];

// Test wrapper that provides all required contexts with theme support
interface TestWrapperProps {
  children: ReactNode;
  theme?: Theme;
  isAuthenticated?: boolean;
}

function TestWrapper({ children, theme = 'light', isAuthenticated = false }: TestWrapperProps) {
  const authState = {
    isAuthenticated,
    user: isAuthenticated ? { id: '1', username: 'testuser', email: 'test@example.com' } : null,
  };

  return (
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider initialState={authState}>
        <ThemeProvider initialTheme={theme}>
          {children}
        </ThemeProvider>
      </AuthProvider>
    </MemoryRouter>
  );
}

// Component to expose theme context for testing
function ThemeDisplay() {
  const { theme } = useTheme();
  return <div data-testid="theme-display" data-current-theme={theme}>{theme}</div>;
}

// Component to test theme toggling
interface ThemeTogglerProps {
  targetTheme: Theme;
}

function ThemeToggler({ targetTheme }: ThemeTogglerProps) {
  const { theme, setTheme } = useTheme();
  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <button
        data-testid="toggle-theme-button"
        onClick={() => setTheme(targetTheme)}
      >
        Switch to {targetTheme}
      </button>
    </div>
  );
}

describe('Scenario 8: Theme System Support', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Test Case 1: Light Theme Rendering', () => {
    it('should render homepage with light theme applied correctly', () => {
      render(
        <TestWrapper theme="light">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      // Verify page renders
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Verify theme context has correct value
      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-current-theme', 'light');
      expect(themeDisplay).toHaveTextContent('light');
    });

    it('should render HeroSection with light theme', () => {
      render(
        <TestWrapper theme="light">
          <HeroSection isAuthenticated={false} />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('light');
    });

    it('should render FeaturesSection with light theme', () => {
      render(
        <TestWrapper theme="light">
          <FeaturesSection />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('light');
    });

    it('should render GlassMorphismCard correctly with light theme', () => {
      render(
        <TestWrapper theme="light">
          <GlassMorphismCard data-testid="glass-card">
            <span>Test content</span>
          </GlassMorphismCard>
          <ThemeDisplay />
        </TestWrapper>
      );

      const card = screen.getByTestId('glass-card');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('backdrop-blur-md');
      expect(card).toHaveClass('bg-base-100/30');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('light');
    });

    it('should render FuturisticButton with light theme styling', () => {
      render(
        <TestWrapper theme="light">
          <FuturisticButton data-testid="futuristic-btn">Click me</FuturisticButton>
          <ThemeDisplay />
        </TestWrapper>
      );

      const button = screen.getByTestId('futuristic-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('bg-primary');
      expect(button).toHaveClass('text-primary-content');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('light');
    });
  });

  describe('Test Case 2: Dark Theme Rendering', () => {
    it('should render homepage with dark theme applied correctly', () => {
      render(
        <TestWrapper theme="dark">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-current-theme', 'dark');
      expect(themeDisplay).toHaveTextContent('dark');
    });

    it('should render all homepage sections with dark theme', () => {
      render(
        <TestWrapper theme="dark">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      // All major sections should render
      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('dark');
    });

    it('should render GlassMorphismCard with dark theme', () => {
      render(
        <TestWrapper theme="dark">
          <GlassMorphismCard data-testid="dark-glass-card">
            <span>Dark content</span>
          </GlassMorphismCard>
          <ThemeDisplay />
        </TestWrapper>
      );

      const card = screen.getByTestId('dark-glass-card');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('backdrop-blur-md');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('dark');
    });

    it('should render FuturisticButton with dark theme styling', () => {
      render(
        <TestWrapper theme="dark">
          <FuturisticButton data-testid="dark-btn" variant="secondary">
            Dark button
          </FuturisticButton>
          <ThemeDisplay />
        </TestWrapper>
      );

      const button = screen.getByTestId('dark-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('bg-secondary');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('dark');
    });
  });

  describe('Test Case 3: Cyberpunk Theme Rendering', () => {
    it('should render homepage with cyberpunk theme applied correctly', () => {
      render(
        <TestWrapper theme="cyberpunk">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-current-theme', 'cyberpunk');
      expect(themeDisplay).toHaveTextContent('cyberpunk');
    });

    it('should render GlassMorphismCard with cyberpunk neon aesthetic', () => {
      render(
        <TestWrapper theme="cyberpunk">
          <GlassMorphismCard data-testid="cyberpunk-card">
            <span>Neon content</span>
          </GlassMorphismCard>
          <ThemeDisplay />
        </TestWrapper>
      );

      const card = screen.getByTestId('cyberpunk-card');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('bg-base-100/30');
      expect(card).toHaveClass('border');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('cyberpunk');
    });

    it('should render FuturisticButton with cyberpunk styling', () => {
      render(
        <TestWrapper theme="cyberpunk">
          <FuturisticButton data-testid="cyberpunk-btn">Hack</FuturisticButton>
          <ThemeDisplay />
        </TestWrapper>
      );

      const button = screen.getByTestId('cyberpunk-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('shadow-lg');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('cyberpunk');
    });

    it('should render HeroSection with cyberpunk theme', () => {
      render(
        <TestWrapper theme="cyberpunk">
          <HeroSection isAuthenticated={false} />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('cyberpunk');
    });
  });

  describe('Test Case 4: Synthwave Theme Rendering', () => {
    it('should render homepage with synthwave retro colors applied', () => {
      render(
        <TestWrapper theme="synthwave">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-current-theme', 'synthwave');
      expect(themeDisplay).toHaveTextContent('synthwave');
    });

    it('should render all homepage components with synthwave theme', () => {
      render(
        <TestWrapper theme="synthwave">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('synthwave');
    });

    it('should render GlassMorphismCard with synthwave theme', () => {
      render(
        <TestWrapper theme="synthwave">
          <GlassMorphismCard data-testid="synthwave-card">
            <span>Retro vibes</span>
          </GlassMorphismCard>
          <ThemeDisplay />
        </TestWrapper>
      );

      const card = screen.getByTestId('synthwave-card');
      expect(card).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('synthwave');
    });

    it('should render FuturisticButton with synthwave retro aesthetic', () => {
      render(
        <TestWrapper theme="synthwave">
          <FuturisticButton data-testid="synthwave-btn">80s Vibes</FuturisticButton>
          <ThemeDisplay />
        </TestWrapper>
      );

      const button = screen.getByTestId('synthwave-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('transition-all');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('synthwave');
    });
  });

  describe('Test Case 5: Retro Theme Rendering', () => {
    it('should render homepage with retro theme styles applied', () => {
      render(
        <TestWrapper theme="retro">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-current-theme', 'retro');
      expect(themeDisplay).toHaveTextContent('retro');
    });

    it('should render GlassMorphismCard with retro theme', () => {
      render(
        <TestWrapper theme="retro">
          <GlassMorphismCard data-testid="retro-card">
            <span>Classic look</span>
          </GlassMorphismCard>
          <ThemeDisplay />
        </TestWrapper>
      );

      const card = screen.getByTestId('retro-card');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('rounded-2xl');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('retro');
    });

    it('should render FuturisticButton with retro styling', () => {
      render(
        <TestWrapper theme="retro">
          <FuturisticButton data-testid="retro-btn" variant="outline">
            Vintage
          </FuturisticButton>
          <ThemeDisplay />
        </TestWrapper>
      );

      const button = screen.getByTestId('retro-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('border-2');
      expect(button).toHaveClass('border-primary');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('retro');
    });

    it('should render FeaturesSection with retro theme', () => {
      render(
        <TestWrapper theme="retro">
          <FeaturesSection />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('retro');
    });
  });

  describe('Test Case 6: Valentine Theme Rendering', () => {
    it('should render homepage with valentine theme styles applied', () => {
      render(
        <TestWrapper theme="valentine">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-current-theme', 'valentine');
      expect(themeDisplay).toHaveTextContent('valentine');
    });

    it('should render all sections with valentine theme', () => {
      render(
        <TestWrapper theme="valentine">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('valentine');
    });

    it('should render GlassMorphismCard with valentine theme', () => {
      render(
        <TestWrapper theme="valentine">
          <GlassMorphismCard data-testid="valentine-card">
            <span>Love vibes</span>
          </GlassMorphismCard>
          <ThemeDisplay />
        </TestWrapper>
      );

      const card = screen.getByTestId('valentine-card');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('p-6');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('valentine');
    });

    it('should render FuturisticButton with valentine theme styling', () => {
      render(
        <TestWrapper theme="valentine">
          <FuturisticButton data-testid="valentine-btn">Hearts</FuturisticButton>
          <ThemeDisplay />
        </TestWrapper>
      );

      const button = screen.getByTestId('valentine-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('font-semibold');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('valentine');
    });
  });

  describe('Test Case 7: Night Theme Rendering', () => {
    it('should render homepage with night theme styles applied', () => {
      render(
        <TestWrapper theme="night">
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-current-theme', 'night');
      expect(themeDisplay).toHaveTextContent('night');
    });

    it('should render GlassMorphismCard with night theme', () => {
      render(
        <TestWrapper theme="night">
          <GlassMorphismCard data-testid="night-card">
            <span>Night mode content</span>
          </GlassMorphismCard>
          <ThemeDisplay />
        </TestWrapper>
      );

      const card = screen.getByTestId('night-card');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('shadow-xl');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('night');
    });

    it('should render FuturisticButton with night theme styling', () => {
      render(
        <TestWrapper theme="night">
          <FuturisticButton data-testid="night-btn" size="lg">
            Dark Mode
          </FuturisticButton>
          <ThemeDisplay />
        </TestWrapper>
      );

      const button = screen.getByTestId('night-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('px-8');
      expect(button).toHaveClass('py-4');
      expect(screen.getByTestId('theme-display')).toHaveTextContent('night');
    });

    it('should render HeroSection with night theme', () => {
      render(
        <TestWrapper theme="night">
          <HeroSection isAuthenticated={false} />
          <ThemeDisplay />
        </TestWrapper>
      );

      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('theme-display')).toHaveTextContent('night');
    });
  });

  describe('Test Case 8: Dynamic Theme Toggling', () => {
    it('should change theme without page reload when toggling from light to dark', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper theme="light">
          <Home />
          <ThemeToggler targetTheme="dark" />
        </TestWrapper>
      );

      // Initial theme should be light
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Toggle to dark theme
      const toggleButton = screen.getByTestId('toggle-theme-button');
      await user.click(toggleButton);

      // Theme should change without unmounting the page
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });

    it('should change theme without page reload when toggling to cyberpunk', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper theme="light">
          <Home />
          <ThemeToggler targetTheme="cyberpunk" />
        </TestWrapper>
      );

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light');

      const toggleButton = screen.getByTestId('toggle-theme-button');
      await user.click(toggleButton);

      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });

    it('should change theme without page reload when toggling to synthwave', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper theme="dark">
          <Home />
          <ThemeToggler targetTheme="synthwave" />
        </TestWrapper>
      );

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');

      const toggleButton = screen.getByTestId('toggle-theme-button');
      await user.click(toggleButton);

      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });

    it('should support multiple consecutive theme changes', async () => {
      const user = userEvent.setup();

      // Component that allows cycling through themes
      function ThemeCycler() {
        const { theme, setTheme } = useTheme();
        const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'night'];
        const nextTheme = themes[(themes.indexOf(theme) + 1) % themes.length];

        return (
          <div>
            <span data-testid="cycle-current-theme">{theme}</span>
            <button data-testid="cycle-button" onClick={() => setTheme(nextTheme)}>
              Next
            </button>
          </div>
        );
      }

      render(
        <TestWrapper theme="light">
          <Home />
          <ThemeCycler />
        </TestWrapper>
      );

      const cycleButton = screen.getByTestId('cycle-button');
      const currentThemeDisplay = screen.getByTestId('cycle-current-theme');

      // Cycle through themes
      expect(currentThemeDisplay).toHaveTextContent('light');

      await user.click(cycleButton);
      expect(currentThemeDisplay).toHaveTextContent('dark');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      await user.click(cycleButton);
      expect(currentThemeDisplay).toHaveTextContent('cyberpunk');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      await user.click(cycleButton);
      expect(currentThemeDisplay).toHaveTextContent('night');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      await user.click(cycleButton);
      expect(currentThemeDisplay).toHaveTextContent('light');
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });

    it('should preserve component state when theme changes', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper theme="light">
          <Home />
          <ThemeToggler targetTheme="dark" />
        </TestWrapper>
      );

      // Verify components are present before toggle
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      const featuresSection = screen.getByTestId('features-section');
      expect(heroSection).toBeInTheDocument();
      expect(featuresSection).toBeInTheDocument();

      // Toggle theme
      const toggleButton = screen.getByTestId('toggle-theme-button');
      await user.click(toggleButton);

      // Components should still be present after toggle
      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
    });
  });

  describe('Component Consistency Across Themes (NFR-3)', () => {
    it.each(ALL_THEMES)('should render GlassMorphismCard with consistent structure in %s theme', (theme) => {
      render(
        <TestWrapper theme={theme}>
          <GlassMorphismCard data-testid={`${theme}-consistency-card`}>
            <span>Consistency test</span>
          </GlassMorphismCard>
        </TestWrapper>
      );

      const card = screen.getByTestId(`${theme}-consistency-card`);
      expect(card).toBeInTheDocument();
      // Core structural classes should be present in all themes
      expect(card).toHaveClass('backdrop-blur-md');
      expect(card).toHaveClass('rounded-2xl');
      expect(card).toHaveClass('p-6');
      expect(card).toHaveClass('shadow-xl');
    });

    it.each(ALL_THEMES)('should render FuturisticButton with consistent structure in %s theme', (theme) => {
      render(
        <TestWrapper theme={theme}>
          <FuturisticButton data-testid={`${theme}-consistency-btn`}>
            Consistent
          </FuturisticButton>
        </TestWrapper>
      );

      const button = screen.getByTestId(`${theme}-consistency-btn`);
      expect(button).toBeInTheDocument();
      // Core structural classes should be present in all themes
      expect(button).toHaveClass('transition-all');
      expect(button).toHaveClass('font-semibold');
      expect(button).toHaveClass('rounded-lg');
    });

    it.each(ALL_THEMES)('should render Home page with all sections in %s theme', (theme) => {
      render(
        <TestWrapper theme={theme}>
          <Home />
        </TestWrapper>
      );

      // All sections should render regardless of theme
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      expect(screen.getByRole('region', { name: /hero section/i })).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
    });
  });

  describe('Theme Context Integration', () => {
    it('should throw error when useTheme is used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      function InvalidComponent() {
        const { theme } = useTheme();
        return <div>{theme}</div>;
      }

      expect(() => {
        render(<InvalidComponent />);
      }).toThrow('useTheme must be used within a ThemeProvider');

      consoleSpy.mockRestore();
    });

    it('should provide default light theme when no initialTheme is specified', () => {
      function ThemeChecker() {
        const { theme } = useTheme();
        return <div data-testid="default-theme">{theme}</div>;
      }

      render(
        <MemoryRouter>
          <AuthProvider>
            <ThemeProvider>
              <ThemeChecker />
            </ThemeProvider>
          </AuthProvider>
        </MemoryRouter>
      );

      expect(screen.getByTestId('default-theme')).toHaveTextContent('light');
    });

    it('should allow setTheme to be called with any valid theme', async () => {
      const user = userEvent.setup();

      function ThemeSetter() {
        const { theme, setTheme } = useTheme();
        return (
          <div>
            <span data-testid="setter-theme">{theme}</span>
            <button data-testid="set-valentine" onClick={() => setTheme('valentine')}>
              Valentine
            </button>
            <button data-testid="set-retro" onClick={() => setTheme('retro')}>
              Retro
            </button>
          </div>
        );
      }

      render(
        <TestWrapper theme="light">
          <ThemeSetter />
        </TestWrapper>
      );

      expect(screen.getByTestId('setter-theme')).toHaveTextContent('light');

      await user.click(screen.getByTestId('set-valentine'));
      expect(screen.getByTestId('setter-theme')).toHaveTextContent('valentine');

      await user.click(screen.getByTestId('set-retro'));
      expect(screen.getByTestId('setter-theme')).toHaveTextContent('retro');
    });
  });

  describe('Theme-Aware Component Rendering', () => {
    it('should render FuturisticButton variants correctly across themes', () => {
      const variants: Array<'primary' | 'secondary' | 'outline'> = ['primary', 'secondary', 'outline'];

      render(
        <TestWrapper theme="cyberpunk">
          {variants.map((variant) => (
            <FuturisticButton key={variant} variant={variant} data-testid={`btn-${variant}`}>
              {variant}
            </FuturisticButton>
          ))}
        </TestWrapper>
      );

      expect(screen.getByTestId('btn-primary')).toHaveClass('bg-primary');
      expect(screen.getByTestId('btn-secondary')).toHaveClass('bg-secondary');
      expect(screen.getByTestId('btn-outline')).toHaveClass('border-primary');
    });

    it('should render FuturisticButton sizes correctly across themes', () => {
      const sizes: Array<'sm' | 'md' | 'lg'> = ['sm', 'md', 'lg'];

      render(
        <TestWrapper theme="night">
          {sizes.map((size) => (
            <FuturisticButton key={size} size={size} data-testid={`btn-${size}`}>
              {size}
            </FuturisticButton>
          ))}
        </TestWrapper>
      );

      expect(screen.getByTestId('btn-sm')).toHaveClass('px-4', 'py-2');
      expect(screen.getByTestId('btn-md')).toHaveClass('px-6', 'py-3');
      expect(screen.getByTestId('btn-lg')).toHaveClass('px-8', 'py-4');
    });
  });
});

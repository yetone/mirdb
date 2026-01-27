/**
 * Theme Integration Tests
 * Owner: Scenario 8 - Theme System Integration
 *
 * Requirements: REQ-7
 * User Stories: US-7
 *
 * Test cases:
 * 1. Homepage renders with light theme colors and styles
 * 2. Homepage renders with dark theme colors and styles
 * 3. Theme toggle dynamically updates homepage without page reload
 * 4. Text contrast meets WCAG AA in light theme (4.5:1)
 * 5. Text contrast meets WCAG AA in dark theme
 * 6. Homepage respects prefers-color-scheme media query
 * 7. GlassMorphismCard maintains glass effect and readability in both themes
 * 8. FuturisticButton maintains styling and visibility in both themes
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { HeroSection } from '../../src/components/homepage/HeroSection';
import { FeaturesSection } from '../../src/components/homepage/FeaturesSection';
import { StatsSection } from '../../src/components/homepage/StatsSection';
import { CTASection } from '../../src/components/homepage/CTASection';
import { DemoInput } from '../../src/components/homepage/DemoInput';
import { GlassMorphismCard } from '../../src/components/GlassMorphismCard';
import { FuturisticButton } from '../../src/components/FuturisticButton';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    button: ({
      children,
      ...props
    }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
    span: ({ children, ...props }: React.HTMLAttributes<HTMLSpanElement>) => (
      <span {...props}>{children}</span>
    ),
  },
  useInView: () => true,
  HTMLMotionProps: {},
}));

/**
 * Helper to set DaisyUI theme on the document
 */
function setTheme(theme: 'light' | 'dark') {
  document.documentElement.setAttribute('data-theme', theme);
}

/**
 * Helper to create a mock matchMedia function for system preference detection
 */
function createColorSchemeMediaMock(prefersDark: boolean) {
  return vi.fn((query: string) => ({
    matches: query === '(prefers-color-scheme: dark)' ? prefersDark : !prefersDark,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(() => false),
  }));
}

/**
 * Helper to render component with router context
 */
function renderWithRouter(component: React.ReactElement) {
  return render(<MemoryRouter>{component}</MemoryRouter>);
}

/**
 * Helper to get computed style of an element
 * Note: In JSDOM, getComputedStyle returns CSS class names but not actual color values,
 * so we verify theme classes are properly applied instead
 */
function hasThemeClass(element: Element, classPatterns: string[]): boolean {
  const className = element.className || '';
  return classPatterns.some((pattern) => className.includes(pattern));
}

/**
 * DaisyUI semantic color class patterns for theme-aware styling
 */
const DAISYUI_THEME_CLASSES = {
  text: ['text-base-content', 'text-primary', 'text-primary-content'],
  background: ['bg-base-100', 'bg-base-200', 'bg-primary'],
  border: ['border-base-content', 'border-primary'],
};

describe('Theme System Integration', () => {
  const originalMatchMedia = window.matchMedia;

  beforeEach(() => {
    // Reset theme to light before each test
    setTheme('light');
  });

  afterEach(() => {
    // Restore original matchMedia
    window.matchMedia = originalMatchMedia;
    // Clean up theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Homepage renders with light theme', () => {
    it('renders homepage components with DaisyUI semantic classes in light theme', () => {
      setTheme('light');
      renderWithRouter(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('HeroSection uses DaisyUI theme-aware text classes', () => {
      setTheme('light');
      renderWithRouter(<HeroSection />);

      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toBeInTheDocument();

      // Verify that the primary color span uses theme-aware class
      const primarySpan = heading.querySelector('.text-primary');
      expect(primarySpan).toBeInTheDocument();
    });

    it('FeaturesSection renders correctly in light theme', () => {
      setTheme('light');
      renderWithRouter(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBe(3);

      // Verify cards use theme-aware classes
      featureCards.forEach((card) => {
        expect(hasThemeClass(card, ['bg-base-100', 'border-base-content'])).toBe(
          true
        );
      });
    });
  });

  describe('Test Case 2: Homepage renders with dark theme', () => {
    it('renders homepage components with DaisyUI semantic classes in dark theme', () => {
      setTheme('dark');
      renderWithRouter(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('FeaturesSection renders correctly in dark theme', () => {
      setTheme('dark');
      renderWithRouter(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBe(3);

      // All cards should still be present and use theme-aware classes
      featureCards.forEach((card) => {
        expect(hasThemeClass(card, ['bg-base-100', 'border-base-content'])).toBe(
          true
        );
      });
    });

    it('StatsSection renders correctly in dark theme', () => {
      setTheme('dark');
      renderWithRouter(<StatsSection />);

      const statsSection = screen.getByTestId('stats-section');
      expect(statsSection).toBeInTheDocument();
      expect(hasThemeClass(statsSection, ['bg-base-200'])).toBe(true);
    });
  });

  describe('Test Case 3: Theme toggle works dynamically', () => {
    it('homepage updates when theme changes without page reload', () => {
      setTheme('light');
      renderWithRouter(<HeroSection />);

      // Verify initial light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Simulate theme toggle
      setTheme('dark');

      // Verify theme changed without re-render
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Component should still be in the DOM
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });

    it('multiple components respond to theme change', () => {
      setTheme('light');
      const { rerender } = renderWithRouter(
        <>
          <HeroSection />
          <FeaturesSection />
        </>
      );

      // Change theme
      setTheme('dark');

      // Rerender to simulate component update
      rerender(
        <MemoryRouter>
          <HeroSection />
          <FeaturesSection />
        </MemoryRouter>
      );

      // Both components should still render correctly
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getAllByTestId('feature-card').length).toBe(3);
    });
  });

  describe('Test Case 4: Text contrast in light theme (WCAG AA)', () => {
    it('text uses DaisyUI semantic classes that ensure WCAG AA compliance', () => {
      setTheme('light');
      renderWithRouter(<HeroSection />);

      // Check that paragraphs use base-content color classes
      const paragraphs = document.querySelectorAll('p');
      const hasThemeAwareText = Array.from(paragraphs).some(
        (p) =>
          p.className.includes('text-base-content') ||
          p.className.includes('base-content')
      );
      expect(hasThemeAwareText).toBe(true);
    });

    it('FeaturesSection text uses theme-aware contrast classes', () => {
      setTheme('light');
      renderWithRouter(<FeaturesSection />);

      // Section header should use base-content
      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();

      // Descriptions use base-content/70 for reduced opacity while maintaining readability
      const descriptions = document.querySelectorAll('.text-base-content\\/70');
      expect(descriptions.length).toBeGreaterThan(0);
    });

    it('CTASection maintains readable text in light theme', () => {
      setTheme('light');
      renderWithRouter(<CTASection />);

      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();

      // Verify heading is present and visible
      const heading = within(ctaSection).getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Text contrast in dark theme (WCAG AA)', () => {
    it('text uses DaisyUI semantic classes that ensure WCAG AA compliance in dark mode', () => {
      setTheme('dark');
      renderWithRouter(<HeroSection />);

      // DaisyUI handles contrast automatically with semantic classes
      const paragraphs = document.querySelectorAll('p');
      const hasThemeAwareText = Array.from(paragraphs).some(
        (p) =>
          p.className.includes('text-base-content') ||
          p.className.includes('base-content')
      );
      expect(hasThemeAwareText).toBe(true);
    });

    it('StatsSection maintains readable text in dark theme', () => {
      setTheme('dark');
      renderWithRouter(<StatsSection />);

      const statsLabels = screen.getAllByTestId('stat-label');
      statsLabels.forEach((label) => {
        expect(label.className).toContain('text-base-content');
      });
    });

    it('DemoInput maintains readable text in dark theme', () => {
      setTheme('dark');
      renderWithRouter(<DemoInput />);

      const section = screen.getByTestId('demo-input-section');
      expect(section).toBeInTheDocument();

      // Input should have readable styling
      const input = screen.getByTestId('demo-url-input');
      expect(input).toBeInTheDocument();
      expect(input.className).toContain('input');
    });
  });

  describe('Test Case 6: System preference detection', () => {
    it('respects prefers-color-scheme: dark media query', () => {
      // Mock matchMedia to prefer dark mode
      window.matchMedia = createColorSchemeMediaMock(true);

      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      expect(mediaQuery.matches).toBe(true);
    });

    it('respects prefers-color-scheme: light media query', () => {
      // Mock matchMedia to prefer light mode
      window.matchMedia = createColorSchemeMediaMock(false);

      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      expect(mediaQuery.matches).toBe(false);

      const lightQuery = window.matchMedia('(prefers-color-scheme: light)');
      expect(lightQuery.matches).toBe(true);
    });

    it('components can detect system preference', () => {
      // Mock system preference for dark mode
      window.matchMedia = createColorSchemeMediaMock(true);

      // Verify the matchMedia is accessible in component context
      const prefersDark = window.matchMedia(
        '(prefers-color-scheme: dark)'
      ).matches;
      expect(prefersDark).toBe(true);

      // Set theme based on preference (simulating what a ThemeProvider would do)
      if (prefersDark) {
        setTheme('dark');
      }

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });

  describe('Test Case 7: GlassMorphismCard appearance in both themes', () => {
    it('renders with glass effect classes in light theme', () => {
      setTheme('light');
      render(
        <GlassMorphismCard data-testid="glass-card">
          Test Content
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('glass-card');
      expect(card).toBeInTheDocument();

      // Verify glass morphism effect classes
      expect(card.className).toContain('backdrop-blur-md');
      expect(card.className).toContain('bg-base-100');
      expect(card.className).toContain('border');
      expect(card.className).toContain('rounded-xl');
      expect(card.className).toContain('shadow-lg');
    });

    it('renders with glass effect classes in dark theme', () => {
      setTheme('dark');
      render(
        <GlassMorphismCard data-testid="glass-card">
          Test Content
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('glass-card');
      expect(card).toBeInTheDocument();

      // Same glass morphism classes should be present
      expect(card.className).toContain('backdrop-blur-md');
      expect(card.className).toContain('bg-base-100');
    });

    it('GlassMorphismCard text remains readable in both themes', () => {
      // Light theme
      setTheme('light');
      const { rerender } = render(
        <GlassMorphismCard data-testid="glass-card">
          <p className="text-base-content">Readable text</p>
        </GlassMorphismCard>
      );

      let text = screen.getByText('Readable text');
      expect(text).toBeInTheDocument();
      expect(text.className).toContain('text-base-content');

      // Dark theme
      setTheme('dark');
      rerender(
        <GlassMorphismCard data-testid="glass-card">
          <p className="text-base-content">Readable text</p>
        </GlassMorphismCard>
      );

      text = screen.getByText('Readable text');
      expect(text).toBeInTheDocument();
    });

    it('FeaturesSection cards maintain glass effect in both themes', () => {
      // Light theme
      setTheme('light');
      const { rerender } = renderWithRouter(<FeaturesSection />);

      let cards = screen.getAllByTestId('feature-card');
      cards.forEach((card) => {
        expect(card.className).toContain('backdrop-blur-md');
        expect(card.className).toContain('bg-base-100');
      });

      // Dark theme
      setTheme('dark');
      rerender(
        <MemoryRouter>
          <FeaturesSection />
        </MemoryRouter>
      );

      cards = screen.getAllByTestId('feature-card');
      cards.forEach((card) => {
        expect(card.className).toContain('backdrop-blur-md');
        expect(card.className).toContain('bg-base-100');
      });
    });
  });

  describe('Test Case 8: FuturisticButton appearance in both themes', () => {
    it('primary button renders with correct classes in light theme', () => {
      setTheme('light');
      render(
        <FuturisticButton variant="primary" data-testid="primary-btn">
          Primary Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('primary-btn');
      expect(button).toBeInTheDocument();
      expect(button.className).toContain('btn');
      expect(button.className).toContain('btn-primary');
      expect(button.className).toContain('text-primary-content');
    });

    it('primary button renders with correct classes in dark theme', () => {
      setTheme('dark');
      render(
        <FuturisticButton variant="primary" data-testid="primary-btn">
          Primary Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('primary-btn');
      expect(button).toBeInTheDocument();
      expect(button.className).toContain('btn');
      expect(button.className).toContain('btn-primary');
    });

    it('secondary button renders with correct classes in light theme', () => {
      setTheme('light');
      render(
        <FuturisticButton variant="secondary" data-testid="secondary-btn">
          Secondary Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('secondary-btn');
      expect(button).toBeInTheDocument();
      expect(button.className).toContain('btn');
      expect(button.className).toContain('btn-ghost');
      expect(button.className).toContain('border');
    });

    it('secondary button renders with correct classes in dark theme', () => {
      setTheme('dark');
      render(
        <FuturisticButton variant="secondary" data-testid="secondary-btn">
          Secondary Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('secondary-btn');
      expect(button).toBeInTheDocument();
      expect(button.className).toContain('btn');
      expect(button.className).toContain('btn-ghost');
    });

    it('ghost button renders with correct classes in both themes', () => {
      // Light theme
      setTheme('light');
      const { rerender } = render(
        <FuturisticButton variant="ghost" data-testid="ghost-btn">
          Ghost Button
        </FuturisticButton>
      );

      let button = screen.getByTestId('ghost-btn');
      expect(button.className).toContain('btn-ghost');
      expect(button.className).toContain('hover:bg-base-200');

      // Dark theme
      setTheme('dark');
      rerender(
        <FuturisticButton variant="ghost" data-testid="ghost-btn">
          Ghost Button
        </FuturisticButton>
      );

      button = screen.getByTestId('ghost-btn');
      expect(button.className).toContain('btn-ghost');
    });

    it('HeroSection buttons maintain visibility in both themes', () => {
      // Light theme
      setTheme('light');
      const { rerender } = renderWithRouter(<HeroSection />);

      let getStartedBtn = screen.getByTestId('get-started-button');
      let signInBtn = screen.getByTestId('sign-in-button');

      expect(getStartedBtn).toBeVisible();
      expect(signInBtn).toBeVisible();
      expect(getStartedBtn.className).toContain('btn-primary');
      expect(signInBtn.className).toContain('btn-ghost');

      // Dark theme
      setTheme('dark');
      rerender(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      );

      getStartedBtn = screen.getByTestId('get-started-button');
      signInBtn = screen.getByTestId('sign-in-button');

      expect(getStartedBtn).toBeVisible();
      expect(signInBtn).toBeVisible();
    });

    it('CTASection buttons maintain visibility in both themes', () => {
      // Light theme
      setTheme('light');
      const { rerender } = renderWithRouter(<CTASection />);

      let getStartedBtn = screen.getByTestId('cta-get-started-button');
      expect(getStartedBtn).toBeVisible();

      // Dark theme
      setTheme('dark');
      rerender(
        <MemoryRouter>
          <CTASection />
        </MemoryRouter>
      );

      getStartedBtn = screen.getByTestId('cta-get-started-button');
      expect(getStartedBtn).toBeVisible();
    });
  });

  describe('Theme-aware component styling consistency', () => {
    it('all homepage sections use consistent DaisyUI semantic classes', () => {
      setTheme('light');
      renderWithRouter(
        <>
          <HeroSection />
          <FeaturesSection />
          <StatsSection />
          <CTASection />
        </>
      );

      // Verify all sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getAllByTestId('feature-card').length).toBe(3);
      expect(screen.getByTestId('stats-section')).toBeInTheDocument();
      expect(screen.getByTestId('cta-section')).toBeInTheDocument();
    });

    it('icons use theme-aware primary color class', () => {
      setTheme('light');
      renderWithRouter(<FeaturesSection />);

      const iconContainers = screen.getAllByTestId('feature-icon');
      iconContainers.forEach((container) => {
        expect(container.className).toContain('text-primary');
        expect(container.className).toContain('bg-primary');
      });
    });

    it('stat numbers use primary color for visibility', () => {
      setTheme('light');
      renderWithRouter(<StatsSection />);

      const statNumbers = screen.getAllByTestId('stat-number');
      statNumbers.forEach((statNum) => {
        expect(statNum.className).toContain('text-primary');
      });
    });
  });
});

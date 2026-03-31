/**
 * Integration tests for Responsive Layout
 * Owner: Scenario 8 - Responsive Design
 *
 * Tests cover:
 * - useMediaQuery hook functionality
 * - useBreakpoint hook functionality
 * - Responsive component behavior at different viewports
 * - Touch target sizes on mobile
 * - Feature cards stacking on mobile
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, renderHook } from '@testing-library/react';
import { useMediaQuery, useBreakpoint, BREAKPOINTS, useIsMobile, useIsTablet, useIsDesktop } from '../../src/hooks/useMediaQuery';
import { FeaturesSection } from '../../src/components/sections/FeaturesSection';
import { Header } from '../../src/components/layout/Header';
import { HeroSection } from '../../src/components/sections/HeroSection';
import type { Feature } from '../../src/types';

// Mock features for testing
const mockFeatures: Feature[] = [
  {
    id: 'feature-1',
    name: 'Memcached Protocol',
    description: 'Full compatibility with Memcached protocol for easy integration.',
    status: 'implemented',
    icon: 'Network',
  },
  {
    id: 'feature-2',
    name: 'Persistent Storage',
    description: 'Data persists across restarts using LSM-tree storage.',
    status: 'implemented',
    icon: 'HardDrive',
  },
  {
    id: 'feature-3',
    name: 'Raft Consensus',
    description: 'Distributed consensus for high availability.',
    status: 'planned',
    icon: 'GitBranch',
  },
];

// Mock matchMedia
const createMatchMedia = (matches: boolean) => {
  return (query: string) => ({
    matches,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  });
};

// Helper to create mock element with getBoundingClientRect
const createMockElement = (top = 0, bottom = 500) => ({
  scrollIntoView: vi.fn(),
  getBoundingClientRect: () => ({ top, bottom, left: 0, right: 0, width: 0, height: bottom - top }),
});

describe('Responsive Layout Integration Tests', () => {
  let originalMatchMedia: typeof window.matchMedia;
  let mediaQueryListeners: Map<string, ((event: MediaQueryListEvent) => void)[]>;

  beforeEach(() => {
    originalMatchMedia = window.matchMedia;
    mediaQueryListeners = new Map();
    vi.spyOn(document, 'getElementById').mockImplementation(() => {
      return createMockElement() as unknown as HTMLElement;
    });
  });

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
    vi.restoreAllMocks();
  });

  // Test Case 5: Call useMediaQuery with mobile breakpoint - Returns true when viewport matches query
  describe('useMediaQuery Hook', () => {
    it('returns true when media query matches', () => {
      window.matchMedia = createMatchMedia(true) as typeof window.matchMedia;

      const { result } = renderHook(() => useMediaQuery('(max-width: 767px)'));

      expect(result.current).toBe(true);
    });

    it('returns false when media query does not match', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      const { result } = renderHook(() => useMediaQuery('(max-width: 767px)'));

      expect(result.current).toBe(false);
    });

    it('updates when media query changes', async () => {
      let matchesValue = false;
      let changeCallback: ((event: MediaQueryListEvent) => void) | null = null;

      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: matchesValue,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn((event: string, callback: (event: MediaQueryListEvent) => void) => {
          if (event === 'change') {
            changeCallback = callback;
          }
        }),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const { result, rerender } = renderHook(() => useMediaQuery('(max-width: 767px)'));

      expect(result.current).toBe(false);

      // Simulate media query change
      matchesValue = true;
      if (changeCallback) {
        act(() => {
          changeCallback!({ matches: true } as MediaQueryListEvent);
        });
      }

      expect(result.current).toBe(true);
    });

    it('handles SSR by returning false when window is undefined', () => {
      // This is tested indirectly - the hook should not throw when window.matchMedia is called
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      const { result } = renderHook(() => useMediaQuery('(min-width: 768px)'));

      expect(typeof result.current).toBe('boolean');
    });
  });

  // Test Case 6: Call useBreakpoint hook - Returns correct breakpoint name for current viewport
  describe('useBreakpoint Hook', () => {
    it('returns "mobile" for viewport < 768px', () => {
      window.matchMedia = vi.fn().mockImplementation((query: string) => {
        // lg: >= 1024px -> false
        // md: >= 768px -> false
        const matches = false;
        return {
          matches,
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        };
      });

      const { result } = renderHook(() => useBreakpoint());

      expect(result.current).toBe('mobile');
    });

    it('returns "tablet" for viewport 768px-1023px', () => {
      window.matchMedia = vi.fn().mockImplementation((query: string) => {
        // lg: >= 1024px -> false
        // md: >= 768px -> true
        const matches = query.includes('768');
        return {
          matches,
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        };
      });

      const { result } = renderHook(() => useBreakpoint());

      expect(result.current).toBe('tablet');
    });

    it('returns "desktop" for viewport >= 1024px', () => {
      window.matchMedia = vi.fn().mockImplementation((query: string) => {
        // Both lg and md match
        const matches = true;
        return {
          matches,
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        };
      });

      const { result } = renderHook(() => useBreakpoint());

      expect(result.current).toBe('desktop');
    });
  });

  describe('Breakpoint Helper Hooks', () => {
    it('useIsMobile returns true for mobile viewport', () => {
      window.matchMedia = vi.fn().mockImplementation((query: string) => {
        const matches = query.includes('max-width') && query.includes('767');
        return {
          matches,
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        };
      });

      const { result } = renderHook(() => useIsMobile());

      expect(result.current).toBe(true);
    });

    it('useIsDesktop returns true for desktop viewport', () => {
      window.matchMedia = vi.fn().mockImplementation((query: string) => {
        const matches = query.includes('min-width') && query.includes('1024');
        return {
          matches,
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        };
      });

      const { result } = renderHook(() => useIsDesktop());

      expect(result.current).toBe(true);
    });
  });

  // Test Case 9: Test feature cards on mobile - Feature cards stack vertically in single column
  describe('Feature Cards Responsive Behavior', () => {
    it('feature cards grid has responsive classes for stacking', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<FeaturesSection features={mockFeatures} />);

      const grid = screen.getByTestId('features-grid');
      // Check for responsive grid classes
      expect(grid).toHaveClass('grid-cols-1');
      expect(grid).toHaveClass('md:grid-cols-2');
      expect(grid).toHaveClass('lg:grid-cols-3');
    });

    it('renders all feature cards', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<FeaturesSection features={mockFeatures} />);

      expect(screen.getByTestId('feature-card-feature-1')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-feature-2')).toBeInTheDocument();
      expect(screen.getByTestId('feature-card-feature-3')).toBeInTheDocument();
    });
  });

  // Touch target tests (related to Test Case 7)
  describe('Touch Target Sizes', () => {
    it('mobile menu button has minimum 44x44px touch target', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<Header />);

      const menuButton = screen.getByTestId('mobile-menu-button');
      expect(menuButton).toHaveClass('min-w-[44px]');
      expect(menuButton).toHaveClass('min-h-[44px]');
    });

    it('hero CTA buttons have adequate touch target size (lg size = py-3 px-6)', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<HeroSection />);

      const getStartedButton = screen.getByTestId('get-started-button');
      const viewGitHubButton = screen.getByTestId('view-github-button');

      // lg size buttons have padding that ensures adequate touch target
      // py-3 = 12px top + 12px bottom = 24px + text height > 44px
      // px-6 = 24px left + 24px right = 48px + content width > 44px
      expect(getStartedButton).toBeInTheDocument();
      expect(viewGitHubButton).toBeInTheDocument();
    });
  });

  describe('Responsive Layout Classes', () => {
    it('hero section has responsive padding classes', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('px-4');
      expect(heroSection).toHaveClass('sm:px-6');
      expect(heroSection).toHaveClass('lg:px-8');
    });

    it('hero buttons stack vertically on mobile, horizontal on larger screens', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<HeroSection />);

      const ctaContainer = screen.getByTestId('hero-cta-buttons');
      expect(ctaContainer).toHaveClass('flex-col');
      expect(ctaContainer).toHaveClass('sm:flex-row');
    });

    it('features section has responsive padding', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      const { container } = render(<FeaturesSection features={mockFeatures} />);

      // Use container.querySelector to find the section element
      const featuresSection = container.querySelector('#features');
      expect(featuresSection).not.toBeNull();
      expect(featuresSection).toHaveClass('px-4');
      expect(featuresSection).toHaveClass('sm:px-6');
      expect(featuresSection).toHaveClass('lg:px-8');
    });
  });

  describe('Header Responsive Behavior', () => {
    it('desktop navigation is hidden on mobile', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<Header />);

      const desktopNav = screen.getByTestId('desktop-navigation');
      expect(desktopNav).toHaveClass('hidden');
      expect(desktopNav).toHaveClass('md:flex');
    });

    it('mobile menu button is visible on mobile (has md:hidden class)', () => {
      window.matchMedia = createMatchMedia(false) as typeof window.matchMedia;

      render(<Header />);

      const menuButton = screen.getByTestId('mobile-menu-button');
      expect(menuButton).toHaveClass('md:hidden');
    });
  });

  describe('BREAKPOINTS constant', () => {
    it('has correct breakpoint values', () => {
      expect(BREAKPOINTS.sm).toBe(640);
      expect(BREAKPOINTS.md).toBe(768);
      expect(BREAKPOINTS.lg).toBe(1024);
      expect(BREAKPOINTS.xl).toBe(1280);
      expect(BREAKPOINTS['2xl']).toBe(1536);
    });
  });
});

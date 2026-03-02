/**
 * Unit tests for useReducedMotion hook.
 * Owner: Scenario 14 - Reduced Motion Support
 *
 * Tests the prefers-reduced-motion media query detection hook:
 * - Returns true when reduced motion is preferred
 * - Returns false when no preference or no-preference set
 * - Updates when preference changes
 * - Properly cleans up event listeners
 */
import { renderHook, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { useReducedMotion } from '../../../src/hooks/useReducedMotion';

describe('useReducedMotion', () => {
  let matchMediaMock: ReturnType<typeof vi.fn>;
  let addEventListenerMock: ReturnType<typeof vi.fn>;
  let removeEventListenerMock: ReturnType<typeof vi.fn>;
  let mediaQueryListeners: Map<string, (event: MediaQueryListEvent) => void>;

  beforeEach(() => {
    // Track listeners for simulation
    mediaQueryListeners = new Map();
    addEventListenerMock = vi.fn((event: string, handler: (event: MediaQueryListEvent) => void) => {
      mediaQueryListeners.set(event, handler);
    });
    removeEventListenerMock = vi.fn((event: string) => {
      mediaQueryListeners.delete(event);
    });

    // Default matchMedia mock - no reduced motion preference
    matchMediaMock = vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: addEventListenerMock,
      removeEventListener: removeEventListenerMock,
      dispatchEvent: vi.fn(),
    }));

    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: matchMediaMock,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('returns false when prefers-reduced-motion is not set', () => {
    const { result } = renderHook(() => useReducedMotion());

    expect(result.current).toBe(false);
    expect(matchMediaMock).toHaveBeenCalledWith('(prefers-reduced-motion: reduce)');
  });

  it('returns true when prefers-reduced-motion: reduce is set', () => {
    // Mock reduced motion preference
    matchMediaMock.mockImplementation((query: string) => ({
      matches: true,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: addEventListenerMock,
      removeEventListener: removeEventListenerMock,
      dispatchEvent: vi.fn(),
    }));

    const { result } = renderHook(() => useReducedMotion());

    expect(result.current).toBe(true);
  });

  it('adds event listener for preference changes', () => {
    renderHook(() => useReducedMotion());

    expect(addEventListenerMock).toHaveBeenCalledWith('change', expect.any(Function));
  });

  it('removes event listener on unmount', () => {
    const { unmount } = renderHook(() => useReducedMotion());

    unmount();

    expect(removeEventListenerMock).toHaveBeenCalledWith('change', expect.any(Function));
  });

  it('updates when reduced motion preference changes to reduce', () => {
    const { result } = renderHook(() => useReducedMotion());

    expect(result.current).toBe(false);

    // Simulate preference change to reduced motion
    act(() => {
      const changeHandler = mediaQueryListeners.get('change');
      if (changeHandler) {
        changeHandler({ matches: true } as MediaQueryListEvent);
      }
    });

    expect(result.current).toBe(true);
  });

  it('updates when reduced motion preference changes from reduce to no-preference', () => {
    // Start with reduced motion enabled
    matchMediaMock.mockImplementation((query: string) => ({
      matches: true,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: addEventListenerMock,
      removeEventListener: removeEventListenerMock,
      dispatchEvent: vi.fn(),
    }));

    const { result } = renderHook(() => useReducedMotion());

    expect(result.current).toBe(true);

    // Simulate preference change back to no preference
    act(() => {
      const changeHandler = mediaQueryListeners.get('change');
      if (changeHandler) {
        changeHandler({ matches: false } as MediaQueryListEvent);
      }
    });

    expect(result.current).toBe(false);
  });

  it('queries the correct media query string', () => {
    renderHook(() => useReducedMotion());

    expect(matchMediaMock).toHaveBeenCalledTimes(2); // Once in useState, once in useEffect
    expect(matchMediaMock).toHaveBeenCalledWith('(prefers-reduced-motion: reduce)');
  });

  describe('Framer Motion component integration patterns', () => {
    it('can be used to conditionally set animation duration', () => {
      // This test documents the expected usage pattern
      const { result } = renderHook(() => useReducedMotion());

      // Pattern: Use hook result to determine animation config
      const getAnimationConfig = (prefersReducedMotion: boolean) => ({
        duration: prefersReducedMotion ? 0 : 0.5,
        ease: prefersReducedMotion ? undefined : 'easeOut',
      });

      const config = getAnimationConfig(result.current);

      expect(config.duration).toBe(0.5); // No reduced motion = normal animations
    });

    it('provides zero duration when reduced motion is preferred', () => {
      // Enable reduced motion
      matchMediaMock.mockImplementation((query: string) => ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
        dispatchEvent: vi.fn(),
      }));

      const { result } = renderHook(() => useReducedMotion());

      const getAnimationConfig = (prefersReducedMotion: boolean) => ({
        duration: prefersReducedMotion ? 0 : 0.5,
        ease: prefersReducedMotion ? undefined : 'easeOut',
      });

      const config = getAnimationConfig(result.current);

      expect(config.duration).toBe(0); // Reduced motion = instant transitions
      expect(config.ease).toBeUndefined();
    });
  });

  describe('fallback for older browsers', () => {
    it('uses addListener fallback when addEventListener is not available', () => {
      const addListenerMock = vi.fn();
      const removeListenerMock = vi.fn();

      matchMediaMock.mockImplementation((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: addListenerMock,
        removeListener: removeListenerMock,
        addEventListener: undefined, // Not available in older browsers
        removeEventListener: undefined,
        dispatchEvent: vi.fn(),
      }));

      const { unmount } = renderHook(() => useReducedMotion());

      expect(addListenerMock).toHaveBeenCalled();

      unmount();

      expect(removeListenerMock).toHaveBeenCalled();
    });
  });

  describe('Hero section animation patterns', () => {
    it('disables hero animations when reduced motion is preferred', () => {
      // Enable reduced motion
      matchMediaMock.mockImplementation((query: string) => ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
        dispatchEvent: vi.fn(),
      }));

      const { result } = renderHook(() => useReducedMotion());

      // Pattern for hero section: use minimal fade or no animation when reduced motion is preferred
      const getHeroAnimationConfig = (prefersReducedMotion: boolean) => ({
        // Initial state
        initial: prefersReducedMotion ? { opacity: 1 } : { opacity: 0, y: 20 },
        // Animate to
        animate: { opacity: 1, y: 0 },
        // Transition
        transition: prefersReducedMotion
          ? { duration: 0 } // Instant transition
          : { duration: 0.5, ease: 'easeOut' },
      });

      const config = getHeroAnimationConfig(result.current);

      // When reduced motion is preferred, animations should be disabled
      expect(result.current).toBe(true);
      expect(config.transition.duration).toBe(0);
      expect(config.initial.opacity).toBe(1); // No fade-in, start fully visible
    });

    it('enables full hero animations when no reduced motion preference', () => {
      const { result } = renderHook(() => useReducedMotion());

      const getHeroAnimationConfig = (prefersReducedMotion: boolean) => ({
        initial: prefersReducedMotion ? { opacity: 1 } : { opacity: 0, y: 20 },
        animate: { opacity: 1, y: 0 },
        transition: prefersReducedMotion
          ? { duration: 0 }
          : { duration: 0.5, ease: 'easeOut' },
      });

      const config = getHeroAnimationConfig(result.current);

      // When no reduced motion preference, full animations should be enabled
      expect(result.current).toBe(false);
      expect(config.transition.duration).toBe(0.5);
      expect(config.initial.opacity).toBe(0); // Start invisible for fade-in
      expect(config.initial.y).toBe(20); // Start offset for slide-up
    });
  });

  describe('Scroll-triggered animation patterns', () => {
    it('disables scroll animations when reduced motion is preferred', () => {
      // Enable reduced motion
      matchMediaMock.mockImplementation((query: string) => ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
        dispatchEvent: vi.fn(),
      }));

      const { result } = renderHook(() => useReducedMotion());

      // Pattern for scroll-triggered animations (like How It Works section)
      const getScrollAnimationVariants = (prefersReducedMotion: boolean) => ({
        container: {
          hidden: { opacity: prefersReducedMotion ? 1 : 0 },
          visible: {
            opacity: 1,
            transition: {
              staggerChildren: prefersReducedMotion ? 0 : 0.2,
              delayChildren: prefersReducedMotion ? 0 : 0.1,
            },
          },
        },
        item: {
          hidden: {
            opacity: prefersReducedMotion ? 1 : 0,
            y: prefersReducedMotion ? 0 : 20,
          },
          visible: {
            opacity: 1,
            y: 0,
            transition: {
              duration: prefersReducedMotion ? 0 : 0.5,
              ease: 'easeOut',
            },
          },
        },
      });

      const variants = getScrollAnimationVariants(result.current);

      // When reduced motion is preferred, scroll animations should be disabled
      expect(result.current).toBe(true);
      expect(variants.container.visible.transition.staggerChildren).toBe(0);
      expect(variants.item.hidden.opacity).toBe(1); // Start visible
      expect(variants.item.visible.transition.duration).toBe(0); // Instant
    });

    it('enables scroll animations when no reduced motion preference', () => {
      const { result } = renderHook(() => useReducedMotion());

      const getScrollAnimationVariants = (prefersReducedMotion: boolean) => ({
        container: {
          hidden: { opacity: prefersReducedMotion ? 1 : 0 },
          visible: {
            opacity: 1,
            transition: {
              staggerChildren: prefersReducedMotion ? 0 : 0.2,
              delayChildren: prefersReducedMotion ? 0 : 0.1,
            },
          },
        },
        item: {
          hidden: {
            opacity: prefersReducedMotion ? 1 : 0,
            y: prefersReducedMotion ? 0 : 20,
          },
          visible: {
            opacity: 1,
            y: 0,
            transition: {
              duration: prefersReducedMotion ? 0 : 0.5,
              ease: 'easeOut',
            },
          },
        },
      });

      const variants = getScrollAnimationVariants(result.current);

      // When no reduced motion preference, scroll animations should work
      expect(result.current).toBe(false);
      expect(variants.container.visible.transition.staggerChildren).toBe(0.2);
      expect(variants.item.hidden.opacity).toBe(0); // Start hidden
      expect(variants.item.visible.transition.duration).toBe(0.5);
    });
  });

  describe('Framer Motion component validation patterns', () => {
    it('provides pattern for checking all motion components respect reduced motion', () => {
      // This test documents the expected pattern for all Framer Motion components
      // Each component should use useReducedMotion() to conditionally set animation props

      // Enable reduced motion
      matchMediaMock.mockImplementation((query: string) => ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
        dispatchEvent: vi.fn(),
      }));

      const { result } = renderHook(() => useReducedMotion());

      // Pattern: Create a motion component configuration factory
      const createMotionConfig = (
        prefersReducedMotion: boolean,
        defaultConfig: {
          initial?: Record<string, number>;
          animate?: Record<string, number>;
          transition?: Record<string, unknown>;
        }
      ) => {
        if (prefersReducedMotion) {
          return {
            initial: defaultConfig.animate, // Start at final state
            animate: defaultConfig.animate,
            transition: { duration: 0 },
          };
        }
        return defaultConfig;
      };

      // Example: Feature card animation config
      const featureCardConfig = createMotionConfig(result.current, {
        initial: { opacity: 0, y: 30 },
        animate: { opacity: 1, y: 0 },
        transition: { duration: 0.5, delay: 0.15, ease: 'easeOut' },
      });

      // Verify reduced motion is respected
      expect(featureCardConfig.initial).toEqual(featureCardConfig.animate);
      expect(featureCardConfig.transition.duration).toBe(0);
    });

    it('provides pattern for parallax effects to be disabled with reduced motion', () => {
      // Enable reduced motion
      matchMediaMock.mockImplementation((query: string) => ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
        dispatchEvent: vi.fn(),
      }));

      const { result } = renderHook(() => useReducedMotion());

      // Pattern for parallax effects
      const getParallaxRange = (prefersReducedMotion: boolean) => {
        // When reduced motion is preferred, no parallax movement
        if (prefersReducedMotion) {
          return [0, 0];
        }
        // Normal parallax range
        return [20, -20];
      };

      const parallaxRange = getParallaxRange(result.current);

      // When reduced motion is preferred, parallax should be disabled
      expect(parallaxRange).toEqual([0, 0]);
    });

    it('provides pattern for theme transitions to respect reduced motion', () => {
      // Enable reduced motion
      matchMediaMock.mockImplementation((query: string) => ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock,
        dispatchEvent: vi.fn(),
      }));

      const { result } = renderHook(() => useReducedMotion());

      // Pattern for theme transition
      const getThemeTransitionStyle = (prefersReducedMotion: boolean) => ({
        transition: prefersReducedMotion
          ? 'none'
          : 'background-color 0.3s ease, color 0.3s ease',
      });

      const style = getThemeTransitionStyle(result.current);

      // When reduced motion is preferred, theme transitions should be instant
      expect(style.transition).toBe('none');
    });
  });
});

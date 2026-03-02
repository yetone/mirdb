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
});

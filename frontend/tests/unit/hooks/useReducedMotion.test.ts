/**
 * Unit tests for useReducedMotion hook
 * Owner: Scenario 12 - Visual Effects and Animations
 *
 * Tests for detecting user's prefers-reduced-motion preference.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useReducedMotion } from '../../../src/hooks/useReducedMotion';

// Helper to create a mock MediaQueryList
function createMockMediaQueryList(matches: boolean) {
  const listeners: Array<(event: MediaQueryListEvent) => void> = [];

  return {
    matches,
    media: '(prefers-reduced-motion: reduce)',
    onchange: null as ((this: MediaQueryList, ev: MediaQueryListEvent) => void) | null,
    addListener: vi.fn((listener: (event: MediaQueryListEvent) => void) => {
      listeners.push(listener);
    }),
    removeListener: vi.fn((listener: (event: MediaQueryListEvent) => void) => {
      const index = listeners.indexOf(listener);
      if (index > -1) listeners.splice(index, 1);
    }),
    addEventListener: vi.fn((event: string, listener: (event: MediaQueryListEvent) => void) => {
      listeners.push(listener);
    }),
    removeEventListener: vi.fn((event: string, listener: (event: MediaQueryListEvent) => void) => {
      const index = listeners.indexOf(listener);
      if (index > -1) listeners.splice(index, 1);
    }),
    dispatchEvent: vi.fn((event: Event): boolean => {
      return false;
    }),
    // Helper to trigger change events in tests
    _triggerChange: (newMatches: boolean) => {
      const event = {
        matches: newMatches,
        media: '(prefers-reduced-motion: reduce)',
      } as MediaQueryListEvent;
      listeners.forEach((listener) => listener(event));
    },
    _listeners: listeners,
  };
}

describe('useReducedMotion', () => {
  let mockMatchMedia: ReturnType<typeof createMockMediaQueryList>;
  let originalMatchMedia: typeof window.matchMedia;

  beforeEach(() => {
    // Store original
    originalMatchMedia = window.matchMedia;

    // Create default mock that returns false (no reduced motion)
    mockMatchMedia = createMockMediaQueryList(false);

    // Override window.matchMedia
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn((query: string) => {
        if (query === '(prefers-reduced-motion: reduce)') {
          return mockMatchMedia;
        }
        return createMockMediaQueryList(false);
      }),
    });
  });

  afterEach(() => {
    // Restore original
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: originalMatchMedia,
    });
    vi.clearAllMocks();
  });

  it('should return false when user has no motion preference set', () => {
    mockMatchMedia = createMockMediaQueryList(false);
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn(() => mockMatchMedia),
    });

    const { result } = renderHook(() => useReducedMotion());

    expect(result.current).toBe(false);
  });

  it('should return true when user prefers reduced motion', () => {
    mockMatchMedia = createMockMediaQueryList(true);
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn(() => mockMatchMedia),
    });

    const { result } = renderHook(() => useReducedMotion());

    expect(result.current).toBe(true);
  });

  it('should update when motion preference changes from false to true', async () => {
    mockMatchMedia = createMockMediaQueryList(false);
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn(() => mockMatchMedia),
    });

    const { result } = renderHook(() => useReducedMotion());

    // Initial state should be false
    expect(result.current).toBe(false);

    // Simulate preference change
    act(() => {
      mockMatchMedia._triggerChange(true);
    });

    // Should now be true
    expect(result.current).toBe(true);
  });

  it('should update when motion preference changes from true to false', async () => {
    mockMatchMedia = createMockMediaQueryList(true);
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn(() => mockMatchMedia),
    });

    const { result } = renderHook(() => useReducedMotion());

    // Initial state should be true
    expect(result.current).toBe(true);

    // Simulate preference change
    act(() => {
      mockMatchMedia._triggerChange(false);
    });

    // Should now be false
    expect(result.current).toBe(false);
  });

  it('should add event listener on mount', () => {
    mockMatchMedia = createMockMediaQueryList(false);
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn(() => mockMatchMedia),
    });

    renderHook(() => useReducedMotion());

    expect(mockMatchMedia.addEventListener).toHaveBeenCalledWith(
      'change',
      expect.any(Function)
    );
  });

  it('should remove event listener on unmount', () => {
    mockMatchMedia = createMockMediaQueryList(false);
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn(() => mockMatchMedia),
    });

    const { unmount } = renderHook(() => useReducedMotion());

    unmount();

    expect(mockMatchMedia.removeEventListener).toHaveBeenCalledWith(
      'change',
      expect.any(Function)
    );
  });

  it('should handle multiple rapid preference changes', () => {
    mockMatchMedia = createMockMediaQueryList(false);
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn(() => mockMatchMedia),
    });

    const { result } = renderHook(() => useReducedMotion());

    // Start with false
    expect(result.current).toBe(false);

    // Multiple rapid changes
    act(() => {
      mockMatchMedia._triggerChange(true);
    });
    expect(result.current).toBe(true);

    act(() => {
      mockMatchMedia._triggerChange(false);
    });
    expect(result.current).toBe(false);

    act(() => {
      mockMatchMedia._triggerChange(true);
    });
    expect(result.current).toBe(true);
  });
});

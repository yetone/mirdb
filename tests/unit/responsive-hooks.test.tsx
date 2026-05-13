import { describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import {
  useMediaQuery,
  useIsMobile,
  useIsTablet,
  useIsDesktop,
  BREAKPOINTS,
} from '../../src/hooks/useMediaQuery';

function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
}

describe('useMediaQuery', () => {
  it('returns false for an initially-unmatched query', () => {
    const { result } = renderHook(() => useMediaQuery('(min-width: 2000px)'));
    expect(result.current).toBe(false);
  });

  it('updates when the media query match changes', () => {
    // Default viewport is 1024px, so (min-width: 1000px) is initially true
    const { result } = renderHook(() => useMediaQuery('(min-width: 1000px)'));
    expect(result.current).toBe(true);

    act(() => {
      setViewportWidth(500);
    });

    expect(result.current).toBe(false);
  });

  it('returns true for a broad query that matches default viewport', () => {
    // Default viewport (1024px) is less than 9999px
    const { result } = renderHook(() => useMediaQuery('(max-width: 9999px)'));
    expect(result.current).toBe(true);
  });
});

describe('BREAKPOINTS', () => {
  it('defines sm as 640px', () => {
    expect(BREAKPOINTS.sm).toBe(640);
  });

  it('defines md as 768px', () => {
    expect(BREAKPOINTS.md).toBe(768);
  });

  it('defines lg as 1024px', () => {
    expect(BREAKPOINTS.lg).toBe(1024);
  });

  it('defines xl as 1280px', () => {
    expect(BREAKPOINTS.xl).toBe(1280);
  });

  it('maintains ascending order', () => {
    expect(BREAKPOINTS.sm).toBeLessThan(BREAKPOINTS.md);
    expect(BREAKPOINTS.md).toBeLessThan(BREAKPOINTS.lg);
    expect(BREAKPOINTS.lg).toBeLessThan(BREAKPOINTS.xl);
  });

  it('has mobile breakpoint threshold at 767px (md - 1)', () => {
    // Mobile is < md, so the max-width query is (md - 1)px
    expect(BREAKPOINTS.md - 1).toBe(767);
  });
});

describe('useIsMobile', () => {
  it('returns true at 375px viewport (iPhone)', () => {
    act(() => setViewportWidth(375));
    const { result } = renderHook(() => useIsMobile());
    expect(result.current).toBe(true);
  });

  it('returns true at 320px viewport (small phone)', () => {
    act(() => setViewportWidth(320));
    const { result } = renderHook(() => useIsMobile());
    expect(result.current).toBe(true);
  });

  it('returns false at 768px viewport', () => {
    act(() => setViewportWidth(768));
    const { result } = renderHook(() => useIsMobile());
    expect(result.current).toBe(false);
  });

  it('returns false at 1024px viewport (desktop)', () => {
    act(() => setViewportWidth(1024));
    const { result } = renderHook(() => useIsMobile());
    expect(result.current).toBe(false);
  });
});

describe('useIsTablet', () => {
  it('returns false at 375px viewport (mobile)', () => {
    act(() => setViewportWidth(375));
    const { result } = renderHook(() => useIsTablet());
    expect(result.current).toBe(false);
  });

  it('returns true at 800px viewport (tablet)', () => {
    act(() => setViewportWidth(800));
    const { result } = renderHook(() => useIsTablet());
    expect(result.current).toBe(true);
  });

  it('returns false at 1024px viewport (desktop)', () => {
    act(() => setViewportWidth(1024));
    const { result } = renderHook(() => useIsTablet());
    expect(result.current).toBe(false);
  });

  it('returns false at 1280px viewport (large desktop)', () => {
    act(() => setViewportWidth(1280));
    const { result } = renderHook(() => useIsTablet());
    expect(result.current).toBe(false);
  });
});

describe('useIsDesktop', () => {
  it('returns false at 375px viewport (mobile)', () => {
    act(() => setViewportWidth(375));
    const { result } = renderHook(() => useIsDesktop());
    expect(result.current).toBe(false);
  });

  it('returns false at 800px viewport (tablet)', () => {
    act(() => setViewportWidth(800));
    const { result } = renderHook(() => useIsDesktop());
    expect(result.current).toBe(false);
  });

  it('returns true at 1024px viewport', () => {
    act(() => setViewportWidth(1024));
    const { result } = renderHook(() => useIsDesktop());
    expect(result.current).toBe(true);
  });

  it('returns true at 1280px viewport (large desktop)', () => {
    act(() => setViewportWidth(1280));
    const { result } = renderHook(() => useIsDesktop());
    expect(result.current).toBe(true);
  });

  it('returns true at 2560px viewport (ultra-wide)', () => {
    act(() => setViewportWidth(2560));
    const { result } = renderHook(() => useIsDesktop());
    expect(result.current).toBe(true);
  });
});

/**
 * Viewport resize utilities for responsive design tests.
 * Owner: Scenario 5 - Mobile Responsive Design
 *
 * Expected exports:
 * - setViewport(width: number, height?: number): void
 * - VIEWPORTS: { mobile: 375, tablet: 768, desktop: 1280 }
 * - resetViewport(): void
 */

import { vi } from 'vitest';

export const VIEWPORTS = {
  mobile: 375,
  tablet: 768,
  desktop: 1280,
} as const;

const originalInnerWidth = window.innerWidth;
const originalInnerHeight = window.innerHeight;

/**
 * Set the viewport dimensions for responsive testing.
 * Mocks window.innerWidth, window.innerHeight, and matchMedia for the given width.
 */
export function setViewport(width: number, height: number = 768): void {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });

  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: height,
  });

  // Update matchMedia to properly match media queries
  window.matchMedia = vi.fn().mockImplementation((query: string) => {
    // Parse common breakpoint queries
    const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/);
    const maxWidthMatch = query.match(/\(max-width:\s*(\d+)px\)/);

    let matches = false;

    if (minWidthMatch) {
      const minWidth = parseInt(minWidthMatch[1], 10);
      matches = width >= minWidth;
    } else if (maxWidthMatch) {
      const maxWidth = parseInt(maxWidthMatch[1], 10);
      matches = width <= maxWidth;
    }

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

  // Dispatch resize event to trigger any listeners
  window.dispatchEvent(new Event('resize'));
}

/**
 * Reset viewport to original dimensions.
 */
export function resetViewport(): void {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: originalInnerWidth,
  });

  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: originalInnerHeight,
  });

  window.dispatchEvent(new Event('resize'));
}

/**
 * Get computed style helper for checking element dimensions.
 */
export function getElementDimensions(element: HTMLElement): { width: number; height: number } {
  const computedStyle = window.getComputedStyle(element);
  return {
    width: parseFloat(computedStyle.width),
    height: parseFloat(computedStyle.height),
  };
}

/**
 * Check if element has horizontal overflow.
 */
export function hasHorizontalOverflow(element: HTMLElement): boolean {
  return element.scrollWidth > element.clientWidth;
}

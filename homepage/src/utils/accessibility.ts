/**
 * Accessibility utility functions.
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Provides utility functions for accessibility testing and validation.
 */

/**
 * Parses a color string (hex, rgb, hsl) and returns RGB values.
 */
function parseColor(color: string): { r: number; g: number; b: number } | null {
  // Handle hex colors
  if (color.startsWith('#')) {
    const hex = color.slice(1);
    if (hex.length === 3) {
      const r = parseInt(hex[0] + hex[0], 16);
      const g = parseInt(hex[1] + hex[1], 16);
      const b = parseInt(hex[2] + hex[2], 16);
      return { r, g, b };
    }
    if (hex.length === 6) {
      const r = parseInt(hex.slice(0, 2), 16);
      const g = parseInt(hex.slice(2, 4), 16);
      const b = parseInt(hex.slice(4, 6), 16);
      return { r, g, b };
    }
  }

  // Handle rgb/rgba colors
  const rgbMatch = color.match(
    /rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*[\d.]+)?\s*\)/i
  );
  if (rgbMatch) {
    return {
      r: parseInt(rgbMatch[1], 10),
      g: parseInt(rgbMatch[2], 10),
      b: parseInt(rgbMatch[3], 10),
    };
  }

  return null;
}

/**
 * Calculates the relative luminance of a color.
 * Based on WCAG 2.1 definition.
 *
 * @param r - Red value (0-255)
 * @param g - Green value (0-255)
 * @param b - Blue value (0-255)
 * @returns Relative luminance value (0-1)
 */
function getRelativeLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculates the contrast ratio between two colors.
 * Based on WCAG 2.1 contrast ratio formula.
 *
 * @param color1 - First color (hex, rgb, or rgba)
 * @param color2 - Second color (hex, rgb, or rgba)
 * @returns Contrast ratio (1 to 21)
 */
export function getContrastRatio(color1: string, color2: string): number {
  const parsed1 = parseColor(color1);
  const parsed2 = parseColor(color2);

  if (!parsed1 || !parsed2) {
    return 0;
  }

  const lum1 = getRelativeLuminance(parsed1.r, parsed1.g, parsed1.b);
  const lum2 = getRelativeLuminance(parsed2.r, parsed2.g, parsed2.b);

  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Checks if a contrast ratio meets WCAG AA standards.
 *
 * @param ratio - Contrast ratio to check
 * @param isLargeText - Whether the text is considered "large" (≥18pt or ≥14pt bold)
 * @returns true if the ratio meets WCAG AA
 */
export function meetsWCAGAA(ratio: number, isLargeText: boolean = false): boolean {
  // WCAG AA: 4.5:1 for normal text, 3:1 for large text
  const threshold = isLargeText ? 3 : 4.5;
  return ratio >= threshold;
}

/**
 * Checks if a contrast ratio meets WCAG AAA standards.
 *
 * @param ratio - Contrast ratio to check
 * @param isLargeText - Whether the text is considered "large"
 * @returns true if the ratio meets WCAG AAA
 */
export function meetsWCAGAAA(ratio: number, isLargeText: boolean = false): boolean {
  // WCAG AAA: 7:1 for normal text, 4.5:1 for large text
  const threshold = isLargeText ? 4.5 : 7;
  return ratio >= threshold;
}

/**
 * Programmatically skips to main content for keyboard users.
 * Should be called when user activates a "Skip to main content" link.
 */
export function skipToMainContent(): void {
  const mainContent = document.getElementById('main-content') || document.querySelector('main');

  if (mainContent) {
    // Make main focusable if not already
    if (!mainContent.hasAttribute('tabindex')) {
      mainContent.setAttribute('tabindex', '-1');
    }

    mainContent.focus();
    mainContent.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

/**
 * Announces a message to screen readers using ARIA live region.
 *
 * @param message - Message to announce
 * @param priority - 'polite' or 'assertive'
 */
export function announceToScreenReader(
  message: string,
  priority: 'polite' | 'assertive' = 'polite'
): void {
  const announcement = document.createElement('div');
  announcement.setAttribute('aria-live', priority);
  announcement.setAttribute('aria-atomic', 'true');
  announcement.className = 'visually-hidden';
  announcement.textContent = message;

  document.body.appendChild(announcement);

  // Remove after announcement
  setTimeout(() => {
    document.body.removeChild(announcement);
  }, 1000);
}

/**
 * Traps focus within a container element.
 * Useful for modal dialogs and dropdown menus.
 *
 * @param containerElement - The element to trap focus within
 * @returns Cleanup function to remove the trap
 */
export function trapFocus(containerElement: HTMLElement): () => void {
  const focusableElements = containerElement.querySelectorAll<HTMLElement>(
    'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
  );

  const firstFocusable = focusableElements[0];
  const lastFocusable = focusableElements[focusableElements.length - 1];

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key !== 'Tab') return;

    if (e.shiftKey) {
      // Shift + Tab
      if (document.activeElement === firstFocusable) {
        e.preventDefault();
        lastFocusable?.focus();
      }
    } else {
      // Tab
      if (document.activeElement === lastFocusable) {
        e.preventDefault();
        firstFocusable?.focus();
      }
    }
  };

  containerElement.addEventListener('keydown', handleKeyDown);

  // Focus first element
  firstFocusable?.focus();

  return () => {
    containerElement.removeEventListener('keydown', handleKeyDown);
  };
}

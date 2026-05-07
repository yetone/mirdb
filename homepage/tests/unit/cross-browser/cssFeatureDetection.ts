/**
 * CSS feature detection utilities for cross-browser compatibility.
 *
 * Used at runtime to detect support for critical CSS features and
 * apply graceful fallbacks when needed.
 */

export function supportsCSSFeature(property: string, value: string): boolean {
  if (typeof CSS === 'undefined' || typeof CSS.supports !== 'function') {
    return false;
  }
  try {
    return CSS.supports(property, value);
  } catch {
    return false;
  }
}

export function supportsCSSVariables(): boolean {
  return supportsCSSFeature('--custom-property', '0');
}

export function supportsFlexbox(): boolean {
  return supportsCSSFeature('display', 'flex');
}

export function supportsGrid(): boolean {
  return supportsCSSFeature('display', 'grid');
}

export function supportsScrollBehavior(): boolean {
  return supportsCSSFeature('scroll-behavior', 'smooth');
}

export function supportsBackdropFilter(): boolean {
  return (
    supportsCSSFeature('backdrop-filter', 'blur(10px)') ||
    supportsCSSFeature('-webkit-backdrop-filter', 'blur(10px)')
  );
}

export function supportsCSSCustomProperties(): boolean {
  return supportsCSSVariables();
}

export interface CriticalFeatureSupport {
  cssVariables: boolean;
  flexbox: boolean;
  grid: boolean;
  scrollBehavior: boolean;
  backdropFilter: boolean;
}

export function checkCriticalCSSFeatures(): CriticalFeatureSupport {
  return {
    cssVariables: supportsCSSVariables(),
    flexbox: supportsFlexbox(),
    grid: supportsGrid(),
    scrollBehavior: supportsScrollBehavior(),
    backdropFilter: supportsBackdropFilter(),
  };
}

/**
 * Returns true when all critical features needed for the homepage to render
 * faithfully are supported. CSS variables, flexbox and grid are required;
 * scroll-behavior and backdrop-filter have graceful fallbacks.
 */
export function hasCriticalSupport(support: CriticalFeatureSupport): boolean {
  return support.cssVariables && support.flexbox && support.grid;
}

/**
 * External Links Utility
 * Owner: Scenario 15 - External Links Behavior
 *
 * Provides utilities for handling external links with proper security attributes.
 *
 * Expected exports:
 * - isExternalLink(href: string): boolean - Check if URL is external
 * - getExternalLinkAttributes(): object - Get security attributes for external links
 * - validateExternalLink(element: HTMLAnchorElement): boolean - Validate link has proper attributes
 */

/**
 * Check if a URL is external (starts with http:// or https://)
 * @param {string} href - The URL to check
 * @returns {boolean} - True if the URL is external
 */
export function isExternalLink(href) {
  if (!href || typeof href !== 'string') {
    return false;
  }
  return href.startsWith('http://') || href.startsWith('https://');
}

/**
 * Get the required security attributes for external links
 * These attributes prevent tabnabbing attacks and privacy leaks
 * @returns {object} - Object with target and rel attributes
 */
export function getExternalLinkAttributes() {
  return {
    target: '_blank',
    rel: 'noopener noreferrer',
  };
}

/**
 * Validate that an external link element has proper security attributes
 * @param {HTMLAnchorElement} element - The anchor element to validate
 * @returns {object} - Validation result with isValid and errors array
 */
export function validateExternalLink(element) {
  const errors = [];
  const href = element?.getAttribute?.('href') || element?.href;

  // Check if it's an external link
  if (!isExternalLink(href)) {
    return { isValid: true, errors: [], isExternal: false };
  }

  // Validate target attribute
  const target = element?.getAttribute?.('target') || element?.target;
  if (target !== '_blank') {
    errors.push('External link must have target="_blank"');
  }

  // Validate rel attribute
  const rel = element?.getAttribute?.('rel') || element?.rel;
  if (!rel) {
    errors.push('External link must have rel attribute');
  } else {
    if (!rel.includes('noopener')) {
      errors.push('External link must have rel="noopener" to prevent window.opener access');
    }
    if (!rel.includes('noreferrer')) {
      errors.push('External link must have rel="noreferrer" to prevent Referer header leak');
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    isExternal: true,
  };
}

/**
 * Apply external link attributes to an anchor element
 * @param {HTMLAnchorElement} element - The anchor element to modify
 * @returns {HTMLAnchorElement} - The modified element
 */
export function applyExternalLinkAttributes(element) {
  if (!element) return element;

  const href = element.getAttribute?.('href') || element.href;
  if (!isExternalLink(href)) {
    return element;
  }

  const attrs = getExternalLinkAttributes();
  element.setAttribute('target', attrs.target);
  element.setAttribute('rel', attrs.rel);

  return element;
}

/**
 * External Links Utility Unit Tests
 * Owner: Scenario 15 - External Links Behavior
 *
 * Tests for:
 * - isExternalLink function
 * - getExternalLinkAttributes function
 * - validateExternalLink function
 * - applyExternalLinkAttributes function
 */
import { describe, it, expect, beforeEach } from 'vitest';
import {
  isExternalLink,
  getExternalLinkAttributes,
  validateExternalLink,
  applyExternalLinkAttributes,
} from '../../src/js/utils/external-links.js';

describe('External Links Utility', () => {
  describe('isExternalLink', () => {
    it('should return true for https URLs', () => {
      expect(isExternalLink('https://github.com/akiozihao/mirdb')).toBe(true);
      expect(isExternalLink('https://example.com')).toBe(true);
    });

    it('should return true for http URLs', () => {
      expect(isExternalLink('http://example.com')).toBe(true);
    });

    it('should return false for relative URLs', () => {
      expect(isExternalLink('/about')).toBe(false);
      expect(isExternalLink('./styles.css')).toBe(false);
      expect(isExternalLink('../images/logo.png')).toBe(false);
    });

    it('should return false for anchor links', () => {
      expect(isExternalLink('#features')).toBe(false);
      expect(isExternalLink('#quick-start')).toBe(false);
    });

    it('should return false for empty or invalid inputs', () => {
      expect(isExternalLink('')).toBe(false);
      expect(isExternalLink(null as unknown as string)).toBe(false);
      expect(isExternalLink(undefined as unknown as string)).toBe(false);
    });
  });

  describe('getExternalLinkAttributes', () => {
    it('should return target="_blank"', () => {
      const attrs = getExternalLinkAttributes();
      expect(attrs.target).toBe('_blank');
    });

    it('should return rel="noopener noreferrer"', () => {
      const attrs = getExternalLinkAttributes();
      expect(attrs.rel).toBe('noopener noreferrer');
    });

    it('should include both security attributes', () => {
      const attrs = getExternalLinkAttributes();
      expect(attrs.rel).toContain('noopener');
      expect(attrs.rel).toContain('noreferrer');
    });
  });

  describe('validateExternalLink', () => {
    let mockElement: HTMLAnchorElement;

    beforeEach(() => {
      mockElement = document.createElement('a');
    });

    it('should return valid for properly configured external link', () => {
      mockElement.href = 'https://github.com/akiozihao/mirdb';
      mockElement.target = '_blank';
      mockElement.rel = 'noopener noreferrer';

      const result = validateExternalLink(mockElement);
      expect(result.isValid).toBe(true);
      expect(result.errors).toHaveLength(0);
      expect(result.isExternal).toBe(true);
    });

    it('should return invalid when target="_blank" is missing', () => {
      mockElement.href = 'https://github.com/akiozihao/mirdb';
      mockElement.rel = 'noopener noreferrer';
      // target is not set

      const result = validateExternalLink(mockElement);
      expect(result.isValid).toBe(false);
      expect(result.errors).toContain('External link must have target="_blank"');
    });

    it('should return invalid when rel attribute is missing', () => {
      mockElement.href = 'https://github.com/akiozihao/mirdb';
      mockElement.target = '_blank';
      // rel is not set

      const result = validateExternalLink(mockElement);
      expect(result.isValid).toBe(false);
      expect(result.errors.some(e => e.includes('rel'))).toBe(true);
    });

    it('should return invalid when noopener is missing', () => {
      mockElement.href = 'https://github.com/akiozihao/mirdb';
      mockElement.target = '_blank';
      mockElement.rel = 'noreferrer';

      const result = validateExternalLink(mockElement);
      expect(result.isValid).toBe(false);
      expect(result.errors.some(e => e.includes('noopener'))).toBe(true);
    });

    it('should return invalid when noreferrer is missing', () => {
      mockElement.href = 'https://github.com/akiozihao/mirdb';
      mockElement.target = '_blank';
      mockElement.rel = 'noopener';

      const result = validateExternalLink(mockElement);
      expect(result.isValid).toBe(false);
      expect(result.errors.some(e => e.includes('noreferrer'))).toBe(true);
    });

    it('should return valid for internal links (skip validation)', () => {
      mockElement.href = '#features';

      const result = validateExternalLink(mockElement);
      expect(result.isValid).toBe(true);
      expect(result.isExternal).toBe(false);
    });

    it('should return valid for relative URLs (skip validation)', () => {
      mockElement.href = '/about';

      const result = validateExternalLink(mockElement);
      expect(result.isValid).toBe(true);
      expect(result.isExternal).toBe(false);
    });
  });

  describe('applyExternalLinkAttributes', () => {
    let mockElement: HTMLAnchorElement;

    beforeEach(() => {
      mockElement = document.createElement('a');
    });

    it('should add target="_blank" to external links', () => {
      mockElement.href = 'https://github.com/akiozihao/mirdb';

      applyExternalLinkAttributes(mockElement);

      expect(mockElement.getAttribute('target')).toBe('_blank');
    });

    it('should add rel="noopener noreferrer" to external links', () => {
      mockElement.href = 'https://github.com/akiozihao/mirdb';

      applyExternalLinkAttributes(mockElement);

      expect(mockElement.getAttribute('rel')).toBe('noopener noreferrer');
    });

    it('should not modify internal links', () => {
      mockElement.href = '#features';

      applyExternalLinkAttributes(mockElement);

      expect(mockElement.getAttribute('target')).toBeNull();
      expect(mockElement.getAttribute('rel')).toBeNull();
    });

    it('should not modify relative URLs', () => {
      mockElement.href = '/about';

      applyExternalLinkAttributes(mockElement);

      expect(mockElement.getAttribute('target')).toBeNull();
      expect(mockElement.getAttribute('rel')).toBeNull();
    });

    it('should handle null element gracefully', () => {
      const result = applyExternalLinkAttributes(null as unknown as HTMLAnchorElement);
      expect(result).toBeNull();
    });
  });
});

describe('ExternalLink Component Security', () => {
  it('should prevent tabnabbing with noopener', () => {
    // noopener prevents the new page from accessing window.opener
    const attrs = getExternalLinkAttributes();
    expect(attrs.rel).toContain('noopener');
  });

  it('should prevent referrer leakage with noreferrer', () => {
    // noreferrer prevents sending the Referer header to the target page
    const attrs = getExternalLinkAttributes();
    expect(attrs.rel).toContain('noreferrer');
  });

  it('should ensure links open in new tab with target="_blank"', () => {
    const attrs = getExternalLinkAttributes();
    expect(attrs.target).toBe('_blank');
  });
});

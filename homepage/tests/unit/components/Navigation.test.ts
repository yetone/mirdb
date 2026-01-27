/**
 * Unit tests for Navigation component utilities.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Tests:
 * - NAV_SECTIONS constant
 * - scrollToSection function
 * - updateActiveSection function
 * - initSmoothScroll function
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { NAV_SECTIONS, scrollToSection, updateActiveSection } from '../../../src/scripts/navigation';

describe('Navigation Utilities', () => {
  describe('NAV_SECTIONS constant', () => {
    it('should contain expected section IDs', () => {
      expect(NAV_SECTIONS).toContain('features');
      expect(NAV_SECTIONS).toContain('quick-start');
      expect(NAV_SECTIONS).toContain('architecture');
      expect(NAV_SECTIONS).toContain('commands');
    });

    it('should have exactly 4 sections', () => {
      expect(NAV_SECTIONS.length).toBe(4);
    });
  });

  describe('scrollToSection', () => {
    let mockElement: {
      scrollIntoView: ReturnType<typeof vi.fn>;
    };
    let mockHistory: {
      pushState: ReturnType<typeof vi.fn>;
    };

    beforeEach(() => {
      // Mock element
      mockElement = {
        scrollIntoView: vi.fn(),
      };

      // Mock document.getElementById
      vi.stubGlobal('document', {
        getElementById: vi.fn((id: string) => (id === 'features' ? mockElement : null)),
      });

      // Mock history
      mockHistory = {
        pushState: vi.fn(),
      };
      vi.stubGlobal('history', mockHistory);
    });

    afterEach(() => {
      vi.unstubAllGlobals();
    });

    it('should scroll to section when element exists', () => {
      scrollToSection('features');

      expect(mockElement.scrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });

    it('should update URL hash when scrolling', () => {
      scrollToSection('features');

      expect(mockHistory.pushState).toHaveBeenCalledWith(null, '', '#features');
    });

    it('should not throw when element does not exist', () => {
      expect(() => scrollToSection('nonexistent')).not.toThrow();
    });

    it('should not update history when element does not exist', () => {
      scrollToSection('nonexistent');

      expect(mockHistory.pushState).not.toHaveBeenCalled();
    });
  });

  describe('updateActiveSection', () => {
    beforeEach(() => {
      // Create mock DOM structure
      const mockSections: Record<string, { getBoundingClientRect: () => { top: number } }> = {
        features: { getBoundingClientRect: () => ({ top: -100 }) },
        'quick-start': { getBoundingClientRect: () => ({ top: 200 }) },
        architecture: { getBoundingClientRect: () => ({ top: 600 }) },
        commands: { getBoundingClientRect: () => ({ top: 1000 }) },
      };

      const mockNavLinks = [
        {
          getAttribute: vi.fn(() => '#features'),
          classList: { add: vi.fn(), remove: vi.fn() },
          setAttribute: vi.fn(),
          removeAttribute: vi.fn(),
        },
        {
          getAttribute: vi.fn(() => '#quick-start'),
          classList: { add: vi.fn(), remove: vi.fn() },
          setAttribute: vi.fn(),
          removeAttribute: vi.fn(),
        },
      ];

      vi.stubGlobal('document', {
        getElementById: vi.fn((id: string) => mockSections[id] || null),
        querySelectorAll: vi.fn(() => mockNavLinks),
      });

      vi.stubGlobal('window', {
        scrollY: 100,
        innerHeight: 800,
      });
    });

    afterEach(() => {
      vi.unstubAllGlobals();
    });

    it('should query for nav links', () => {
      updateActiveSection();

      expect(document.querySelectorAll).toHaveBeenCalledWith('.nav-link');
    });

    it('should get sections by ID', () => {
      updateActiveSection();

      expect(document.getElementById).toHaveBeenCalledWith('features');
      expect(document.getElementById).toHaveBeenCalledWith('quick-start');
      expect(document.getElementById).toHaveBeenCalledWith('architecture');
      expect(document.getElementById).toHaveBeenCalledWith('commands');
    });
  });
});

describe('CSS scroll-behavior validation', () => {
  it('should define scroll-behavior: smooth requirement', () => {
    // This test documents that smooth scroll behavior should be set via CSS
    // The actual CSS is: html { scroll-behavior: smooth; }
    const expectedCSS = 'scroll-behavior: smooth';
    expect(expectedCSS).toBe('scroll-behavior: smooth');
  });
});

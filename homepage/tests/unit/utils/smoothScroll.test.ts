/**
 * Smooth Scroll Utility Unit Tests.
 * Owner: Scenario 7 - Navigation and Header
 *
 * Test cases:
 * 3. Call scrollToSection with valid section ID - Page smoothly scrolls to the target section
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { scrollToSection, initSmoothScroll } from '../../../src/utils/smoothScroll';

describe('Smooth Scroll Utility', () => {
  beforeEach(() => {
    document.body.innerHTML = '';
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('scrollToSection', () => {
    it('should call scrollIntoView on element with valid section ID', () => {
      // Create a target section
      const section = document.createElement('section');
      section.id = 'features';
      document.body.appendChild(section);

      const scrollIntoViewMock = vi.fn();
      section.scrollIntoView = scrollIntoViewMock;

      scrollToSection('features');

      expect(scrollIntoViewMock).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });

    it('should call scrollIntoView with smooth behavior', () => {
      const section = document.createElement('section');
      section.id = 'quickstart';
      document.body.appendChild(section);

      const scrollIntoViewMock = vi.fn();
      section.scrollIntoView = scrollIntoViewMock;

      scrollToSection('quickstart');

      expect(scrollIntoViewMock).toHaveBeenCalledWith(
        expect.objectContaining({
          behavior: 'smooth',
        })
      );
    });

    it('should not throw when section ID does not exist', () => {
      expect(() => scrollToSection('nonexistent')).not.toThrow();
    });

    it('should not call scrollIntoView when element does not exist', () => {
      const scrollIntoViewMock = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoViewMock;

      scrollToSection('nonexistent');

      // Should not have been called since element doesn't exist
      expect(scrollIntoViewMock).not.toHaveBeenCalled();
    });

    it('should scroll to different sections correctly', () => {
      const sections = ['features', 'protocol', 'configuration', 'architecture'];

      sections.forEach((sectionId) => {
        const section = document.createElement('section');
        section.id = sectionId;
        const scrollMock = vi.fn();
        section.scrollIntoView = scrollMock;
        document.body.appendChild(section);
      });

      // Test each section
      sections.forEach((sectionId) => {
        const section = document.getElementById(sectionId);
        const scrollMock = vi.fn();
        if (section) {
          section.scrollIntoView = scrollMock;
          scrollToSection(sectionId);
          expect(scrollMock).toHaveBeenCalled();
        }
      });
    });
  });

  describe('initSmoothScroll', () => {
    it('should add click event listener to document', () => {
      const addEventListenerSpy = vi.spyOn(document, 'addEventListener');

      initSmoothScroll();

      expect(addEventListenerSpy).toHaveBeenCalledWith('click', expect.any(Function));
    });

    it('should prevent default and scroll when clicking hash link', () => {
      // Create a target section
      const section = document.createElement('section');
      section.id = 'features';
      document.body.appendChild(section);

      const scrollIntoViewMock = vi.fn();
      section.scrollIntoView = scrollIntoViewMock;

      // Create and add a link
      const link = document.createElement('a');
      link.href = '#features';
      document.body.appendChild(link);

      initSmoothScroll();

      // Simulate click
      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });
      const preventDefaultSpy = vi.spyOn(clickEvent, 'preventDefault');

      link.dispatchEvent(clickEvent);

      expect(preventDefaultSpy).toHaveBeenCalled();
      expect(scrollIntoViewMock).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });

    it('should not prevent default for non-hash links', () => {
      const externalLink = document.createElement('a');
      externalLink.href = 'https://example.com';
      document.body.appendChild(externalLink);

      initSmoothScroll();

      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });
      const preventDefaultSpy = vi.spyOn(clickEvent, 'preventDefault');

      externalLink.dispatchEvent(clickEvent);

      expect(preventDefaultSpy).not.toHaveBeenCalled();
    });

    it('should not scroll for empty hash link (#)', () => {
      const emptyHashLink = document.createElement('a');
      emptyHashLink.href = '#';
      document.body.appendChild(emptyHashLink);

      const section = document.createElement('section');
      section.id = '';
      const scrollMock = vi.fn();
      section.scrollIntoView = scrollMock;
      document.body.appendChild(section);

      initSmoothScroll();

      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });

      emptyHashLink.dispatchEvent(clickEvent);

      expect(scrollMock).not.toHaveBeenCalled();
    });

    it('should work when clicking an element inside an anchor', () => {
      // Create a target section
      const section = document.createElement('section');
      section.id = 'protocol';
      document.body.appendChild(section);

      const scrollIntoViewMock = vi.fn();
      section.scrollIntoView = scrollIntoViewMock;

      // Create a link with nested element
      const link = document.createElement('a');
      link.href = '#protocol';
      const span = document.createElement('span');
      span.textContent = 'Click me';
      link.appendChild(span);
      document.body.appendChild(link);

      initSmoothScroll();

      // Click on the nested span
      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });

      span.dispatchEvent(clickEvent);

      expect(scrollIntoViewMock).toHaveBeenCalled();
    });

    it('should update browser history with hash', () => {
      const section = document.createElement('section');
      section.id = 'architecture';
      document.body.appendChild(section);

      const scrollIntoViewMock = vi.fn();
      section.scrollIntoView = scrollIntoViewMock;

      const pushStateSpy = vi.spyOn(window.history, 'pushState');

      const link = document.createElement('a');
      link.href = '#architecture';
      document.body.appendChild(link);

      initSmoothScroll();

      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });
      link.dispatchEvent(clickEvent);

      expect(pushStateSpy).toHaveBeenCalledWith(null, '', '#architecture');
    });
  });
});

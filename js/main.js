/**
 * MirDB Homepage - JavaScript
 * Owner: Scenarios 3 & 4
 *
 * Purpose: Minimal JavaScript for interactivity
 */

(function() {
  'use strict';

  /**
   * Copy text to clipboard with Clipboard API and fallback
   * @param {string} text - Text to copy
   * @returns {Promise<boolean>} - Success status
   */
  async function copyToClipboard(text) {
    // Clean text by removing HTML tags and normalizing whitespace
    const cleanText = text
      .replace(/<[^>]*>/g, '')
      .replace(/\s+/g, ' ')
      .trim();

    // Try modern Clipboard API first
    if (navigator.clipboard && navigator.clipboard.writeText) {
      try {
        await navigator.clipboard.writeText(cleanText);
        return true;
      } catch (err) {
        console.warn('Clipboard API failed, trying fallback:', err);
      }
    }

    // Fallback for older browsers
    try {
      const textArea = document.createElement('textarea');
      textArea.value = cleanText;
      textArea.style.position = 'fixed';
      textArea.style.left = '-9999px';
      textArea.style.top = '-9999px';
      textArea.setAttribute('readonly', '');
      document.body.appendChild(textArea);
      textArea.select();
      textArea.setSelectionRange(0, cleanText.length);
      const success = document.execCommand('copy');
      document.body.removeChild(textArea);
      return success;
    } catch (err) {
      console.error('Copy fallback failed:', err);
      return false;
    }
  }

  /**
   * Show visual feedback after copy action
   * @param {HTMLButtonElement} button - The copy button
   * @param {boolean} success - Whether copy was successful
   */
  function showCopyFeedback(button, success) {
    const originalText = button.querySelector('.copy-text').textContent;
    const textSpan = button.querySelector('.copy-text');

    if (success) {
      button.classList.add('copied');
      textSpan.textContent = 'Copied!';
      button.setAttribute('aria-label', 'Copied to clipboard');
    } else {
      textSpan.textContent = 'Failed';
      button.setAttribute('aria-label', 'Failed to copy');
    }

    setTimeout(() => {
      button.classList.remove('copied');
      textSpan.textContent = originalText;
      button.setAttribute('aria-label', button.dataset.originalLabel || 'Copy to clipboard');
    }, 2000);
  }

  /**
   * Initialize copy button click handlers
   */
  function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-button');

    copyButtons.forEach(button => {
      // Store original aria-label
      button.dataset.originalLabel = button.getAttribute('aria-label');

      button.addEventListener('click', async function(e) {
        e.preventDefault();

        const targetId = this.dataset.target;
        const targetElement = document.getElementById(targetId);

        if (targetElement) {
          const textToCopy = targetElement.textContent;
          const success = await copyToClipboard(textToCopy);
          showCopyFeedback(this, success);
        }
      });
    });
  }

  /**
   * Toggle mobile navigation menu
   */
  function toggleMobileMenu() {
    const navToggle = document.querySelector('.nav-toggle');
    const navMenu = document.querySelector('.nav-menu');

    if (!navToggle || !navMenu) return;

    const isExpanded = navToggle.getAttribute('aria-expanded') === 'true';
    navToggle.setAttribute('aria-expanded', !isExpanded);
    navMenu.classList.toggle('active');
  }

  /**
   * Initialize mobile menu toggle
   */
  function initMobileMenu() {
    const navToggle = document.querySelector('.nav-toggle');

    if (!navToggle) return;

    navToggle.addEventListener('click', toggleMobileMenu);

    // Close menu when clicking a link
    const navLinks = document.querySelectorAll('.nav-menu a');
    navLinks.forEach(link => {
      link.addEventListener('click', () => {
        const navMenu = document.querySelector('.nav-menu');
        const navToggle = document.querySelector('.nav-toggle');
        if (navMenu && navMenu.classList.contains('active')) {
          navMenu.classList.remove('active');
          navToggle.setAttribute('aria-expanded', 'false');
        }
      });
    });

    // Close menu on escape key
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        const navMenu = document.querySelector('.nav-menu');
        const navToggle = document.querySelector('.nav-toggle');
        if (navMenu && navMenu.classList.contains('active')) {
          navMenu.classList.remove('active');
          navToggle.setAttribute('aria-expanded', 'false');
          navToggle.focus();
        }
      }
    });
  }

  /**
   * Initialize all functionality when DOM is ready
   */
  function init() {
    initCopyButtons();
    initMobileMenu();
  }

  // Wait for DOM to be ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

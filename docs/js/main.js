/**
 * MirDB Homepage Interactivity
 * Owner: Scenario 17 - JavaScript Interactivity
 *
 * Features:
 * - Copy-to-clipboard for code examples
 * - Mobile hamburger menu toggle
 * - Smooth scroll for anchor navigation
 *
 * Requirements:
 * - Minimal footprint (under 10KB)
 * - No external dependencies
 * - Graceful degradation without JS (NFR-4)
 * - Respects prefers-reduced-motion
 */

(function() {
  'use strict';

  /**
   * Check if user prefers reduced motion
   * @returns {boolean}
   */
  function prefersReducedMotion() {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  /**
   * Mobile hamburger menu toggle
   */
  function initMobileMenu() {
    var hamburger = document.querySelector('.hamburger');
    var navLinks = document.querySelector('.nav-links');

    if (!hamburger || !navLinks) return;

    hamburger.addEventListener('click', function() {
      var isOpen = navLinks.classList.toggle('nav-open');
      hamburger.classList.toggle('is-active', isOpen);
      hamburger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    });

    // Close menu when clicking on a nav link
    navLinks.querySelectorAll('a').forEach(function(link) {
      link.addEventListener('click', function() {
        navLinks.classList.remove('nav-open');
        hamburger.classList.remove('is-active');
        hamburger.setAttribute('aria-expanded', 'false');
      });
    });

    // Close menu when clicking outside
    document.addEventListener('click', function(e) {
      if (!hamburger.contains(e.target) && !navLinks.contains(e.target)) {
        navLinks.classList.remove('nav-open');
        hamburger.classList.remove('is-active');
        hamburger.setAttribute('aria-expanded', 'false');
      }
    });
  }

  /**
   * Copy-to-clipboard functionality for code blocks
   */
  function initCopyButtons() {
    var copyButtons = document.querySelectorAll('.copy-btn, [data-copy]');

    copyButtons.forEach(function(btn) {
      btn.addEventListener('click', function() {
        var codeContainer = btn.closest('.code-container');
        var codeBlock = codeContainer ? codeContainer.querySelector('pre code') : null;

        if (!codeBlock) {
          // Fallback: try to find code in previous sibling
          var prev = btn.previousElementSibling;
          if (prev && prev.tagName === 'PRE') {
            codeBlock = prev.querySelector('code') || prev;
          }
        }

        if (!codeBlock) return;

        var text = codeBlock.textContent || codeBlock.innerText;

        // Use Clipboard API with fallback
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(function() {
            showCopyFeedback(btn, true);
          }).catch(function() {
            fallbackCopy(text, btn);
          });
        } else {
          fallbackCopy(text, btn);
        }
      });
    });
  }

  /**
   * Fallback copy method for older browsers
   * @param {string} text
   * @param {HTMLElement} btn
   */
  function fallbackCopy(text, btn) {
    var textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.left = '-9999px';
    document.body.appendChild(textarea);
    textarea.select();
    try {
      document.execCommand('copy');
      showCopyFeedback(btn, true);
    } catch (e) {
      showCopyFeedback(btn, false);
    }
    document.body.removeChild(textarea);
  }

  /**
   * Show visual feedback after copy
   * @param {HTMLElement} btn
   * @param {boolean} success
   */
  function showCopyFeedback(btn, success) {
    var originalText = btn.textContent;
    btn.textContent = success ? 'Copied!' : 'Failed';
    btn.classList.add('copy-success');
    btn.setAttribute('data-copied', 'true');

    setTimeout(function() {
      btn.textContent = originalText;
      btn.classList.remove('copy-success');
      btn.removeAttribute('data-copied');
    }, 2000);
  }

  /**
   * Smooth scroll for anchor navigation
   */
  function initSmoothScroll() {
    // Get all anchor links that point to sections on this page
    var anchorLinks = document.querySelectorAll('a[href^="#"]');

    anchorLinks.forEach(function(link) {
      link.addEventListener('click', function(e) {
        var href = link.getAttribute('href');
        if (!href || href === '#') return;

        var target = document.querySelector(href);
        if (!target) return;

        e.preventDefault();

        // Check for reduced motion preference
        var behavior = prefersReducedMotion() ? 'auto' : 'smooth';

        // Scroll to target
        target.scrollIntoView({
          behavior: behavior,
          block: 'start'
        });

        // Update URL hash without jumping
        if (history.pushState) {
          history.pushState(null, null, href);
        }
      });
    });
  }

  /**
   * Initialize all interactivity
   */
  function init() {
    initMobileMenu();
    initCopyButtons();
    initSmoothScroll();
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

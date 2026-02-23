/**
 * Main JavaScript Entry Point
 * Owner: Scenario 5 (navigation), Scenario 8 (theme init)
 *
 * Expected content:
 * - DOM ready handler
 * - Theme initialization
 * - Smooth scroll navigation
 * - Mobile menu toggle
 * - Copy to clipboard functionality (Scenario 4)
 */

// Main JavaScript Entry Point
document.addEventListener('DOMContentLoaded', function() {
  // ====================
  // THEME INITIALIZATION (Scenario 8)
  // ====================
  if (typeof MirDBTheme !== 'undefined' && MirDBTheme.init) {
    MirDBTheme.init();
  }

  // ====================
  // NAVIGATION (Scenario 5)
  // ====================
  initSmoothScroll();
  initMobileMenu();

  // ====================
  // COPY TO CLIPBOARD (Scenario 4)
  // ====================
  initCopyButtons();
});

/**
 * Initialize smooth scroll for navigation links
 * Owner: Scenario 5 - Navigation and Resources
 */
function initSmoothScroll() {
  // Get all navigation links that point to internal anchors
  const navLinks = document.querySelectorAll('a[href^="#"]');

  navLinks.forEach(function(link) {
    link.addEventListener('click', function(e) {
      const href = this.getAttribute('href');

      // Skip if href is just "#" (go to top)
      if (href === '#') {
        e.preventDefault();
        window.scrollTo({
          top: 0,
          behavior: 'smooth'
        });
        return;
      }

      // Find the target element
      const targetId = href.substring(1);
      const targetElement = document.getElementById(targetId);

      if (targetElement) {
        e.preventDefault();

        // Calculate offset for fixed header
        const headerHeight = document.querySelector('.header').offsetHeight;
        const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset - headerHeight;

        // Smooth scroll to target
        window.scrollTo({
          top: targetPosition,
          behavior: 'smooth'
        });

        // Update URL hash without jumping
        history.pushState(null, null, href);

        // Close mobile menu if open
        closeMobileMenu();
      }
    });
  });
}

/**
 * Initialize mobile menu toggle functionality
 * Owner: Scenario 5 - Navigation and Resources
 */
function initMobileMenu() {
  const menuToggle = document.querySelector('.header__menu-toggle');
  const nav = document.querySelector('.header__nav');

  if (!menuToggle || !nav) return;

  menuToggle.addEventListener('click', function() {
    const isExpanded = this.getAttribute('aria-expanded') === 'true';
    this.setAttribute('aria-expanded', !isExpanded);
    nav.classList.toggle('header__nav--open');
    document.body.classList.toggle('menu-open');
  });

  // Close menu when clicking outside
  document.addEventListener('click', function(e) {
    if (!nav.contains(e.target) && !menuToggle.contains(e.target)) {
      closeMobileMenu();
    }
  });

  // Close menu on escape key
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      closeMobileMenu();
    }
  });
}

/**
 * Close the mobile menu
 * Owner: Scenario 5 - Navigation and Resources
 */
function closeMobileMenu() {
  const menuToggle = document.querySelector('.header__menu-toggle');
  const nav = document.querySelector('.header__nav');

  if (menuToggle) {
    menuToggle.setAttribute('aria-expanded', 'false');
  }
  if (nav) {
    nav.classList.remove('header__nav--open');
  }
  document.body.classList.remove('menu-open');
}

/**
 * Initialize copy to clipboard functionality for code blocks
 * Owner: Scenario 4 - Quick Start Section
 */
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('[data-copy]');

  copyButtons.forEach(function(button) {
    button.addEventListener('click', async function() {
      const container = button.closest('.quick-start__code-container');
      if (!container) return;

      const codeBlock = container.querySelector('code');
      if (!codeBlock) return;

      const textToCopy = codeBlock.textContent;

      try {
        await navigator.clipboard.writeText(textToCopy);

        // Update button state
        const textSpan = button.querySelector('.quick-start__copy-text');
        const originalText = textSpan ? textSpan.textContent : 'Copy';

        if (textSpan) {
          textSpan.textContent = 'Copied!';
        }
        button.classList.add('copied');

        // Reset after delay
        setTimeout(function() {
          if (textSpan) {
            textSpan.textContent = originalText;
          }
          button.classList.remove('copied');
        }, 2000);
      } catch (err) {
        console.error('Failed to copy text:', err);
        // Fallback for older browsers
        fallbackCopyText(textToCopy);
      }
    });
  });
}

/**
 * Fallback copy function for browsers without clipboard API
 */
function fallbackCopyText(text) {
  const textArea = document.createElement('textarea');
  textArea.value = text;
  textArea.style.position = 'fixed';
  textArea.style.left = '-999999px';
  textArea.style.top = '-999999px';
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  try {
    document.execCommand('copy');
  } catch (err) {
    console.error('Fallback copy failed:', err);
  }

  document.body.removeChild(textArea);
}

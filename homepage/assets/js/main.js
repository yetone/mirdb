/**
 * Main JavaScript
 * Owner: Scenario 8 - Navigation and Footer
 *
 * Expected functionality:
 * - Smooth scroll for anchor links
 * - Mobile hamburger menu toggle
 * - Copy-to-clipboard for code blocks (Scenario 4)
 */

// ============================================
// SECTION: Smooth Scroll (Scenario 8)
// ============================================

/**
 * Initialize smooth scroll for all anchor links.
 * When an anchor link is clicked, smoothly scroll to the target section.
 */
function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
      const targetId = this.getAttribute('href');
      // Skip if href is just '#' or empty
      if (!targetId || targetId === '#') return;

      const target = document.querySelector(targetId);
      if (target) {
        e.preventDefault();

        // Close mobile menu if open
        const navLinks = document.querySelector('.nav-links');
        const navToggle = document.querySelector('.nav-toggle');
        if (navLinks && navLinks.classList.contains('nav-links--open')) {
          navLinks.classList.remove('nav-links--open');
          if (navToggle) {
            navToggle.setAttribute('aria-expanded', 'false');
          }
        }

        // Calculate offset for fixed header
        const header = document.querySelector('.site-header');
        const headerHeight = header ? header.offsetHeight : 0;
        const targetPosition = target.getBoundingClientRect().top + window.pageYOffset - headerHeight;

        window.scrollTo({
          top: targetPosition,
          behavior: 'smooth'
        });

        // Update focus for accessibility
        target.setAttribute('tabindex', '-1');
        target.focus({ preventScroll: true });
      }
    });
  });
}

// ============================================
// SECTION: Mobile Hamburger Menu (Scenario 8)
// ============================================

/**
 * Initialize mobile hamburger menu toggle.
 * Toggles the navigation menu visibility on mobile devices.
 */
function initMobileMenu() {
  const navToggle = document.querySelector('.nav-toggle');
  const navLinks = document.querySelector('.nav-links');

  if (!navToggle || !navLinks) return;

  navToggle.addEventListener('click', function() {
    const isExpanded = this.getAttribute('aria-expanded') === 'true';

    // Toggle menu state
    this.setAttribute('aria-expanded', !isExpanded);
    navLinks.classList.toggle('nav-links--open');

    // Prevent body scroll when menu is open
    document.body.classList.toggle('menu-open', !isExpanded);
  });

  // Close menu when clicking outside
  document.addEventListener('click', function(e) {
    if (!navToggle.contains(e.target) && !navLinks.contains(e.target)) {
      navToggle.setAttribute('aria-expanded', 'false');
      navLinks.classList.remove('nav-links--open');
      document.body.classList.remove('menu-open');
    }
  });

  // Close menu on escape key
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && navLinks.classList.contains('nav-links--open')) {
      navToggle.setAttribute('aria-expanded', 'false');
      navLinks.classList.remove('nav-links--open');
      document.body.classList.remove('menu-open');
      navToggle.focus();
    }
  });
}

// Initialize navigation functionality when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
  initSmoothScroll();
  initMobileMenu();
});

// ============================================
// SECTION: Copy-to-Clipboard (Scenario 4)
// ============================================

/**
 * Initialize copy-to-clipboard functionality for code blocks.
 * When a copy button is clicked, copy the code content to clipboard
 * and provide visual feedback.
 */
function initCopyToClipboard() {
  const copyButtons = document.querySelectorAll('.code-block__copy');

  copyButtons.forEach(button => {
    button.addEventListener('click', async function() {
      const codeBlock = this.closest('.code-block');
      const codeElement = codeBlock.querySelector('code');
      const textToCopy = codeElement.textContent;
      const copyText = this.querySelector('.code-block__copy-text');

      try {
        await navigator.clipboard.writeText(textToCopy);

        // Visual feedback
        const originalText = copyText.textContent;
        copyText.textContent = 'Copied!';
        this.classList.add('code-block__copy--success');

        // Reset after 2 seconds
        setTimeout(() => {
          copyText.textContent = originalText;
          this.classList.remove('code-block__copy--success');
        }, 2000);
      } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = textToCopy;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();

        try {
          document.execCommand('copy');
          copyText.textContent = 'Copied!';
          this.classList.add('code-block__copy--success');

          setTimeout(() => {
            copyText.textContent = 'Copy';
            this.classList.remove('code-block__copy--success');
          }, 2000);
        } catch (fallbackErr) {
          copyText.textContent = 'Failed';
          setTimeout(() => {
            copyText.textContent = 'Copy';
          }, 2000);
        }

        document.body.removeChild(textArea);
      }
    });
  });
}

// Initialize copy functionality when DOM is ready
document.addEventListener('DOMContentLoaded', initCopyToClipboard);

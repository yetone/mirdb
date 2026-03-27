/**
 * MirDB Homepage JavaScript
 * Owner: Scenario 5 - Navigation and GitHub Link
 *
 * Expected functions:
 * - Mobile navigation toggle
 * - Smooth scroll for anchor links
 * - Copy-to-clipboard for code blocks (Scenario 3)
 *
 * Must work without JavaScript for basic viewing (progressive enhancement)
 */

// Initialize all interactive features when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
  initCopyToClipboard();
  initSmoothScroll();
  initMobileNav();
});

/**
 * Copy-to-clipboard functionality for code blocks (Scenario 3)
 * Adds click handlers to all copy buttons within code blocks
 */
function initCopyToClipboard() {
  const copyButtons = document.querySelectorAll('.code-block .copy-btn');

  copyButtons.forEach(function(button) {
    button.addEventListener('click', async function() {
      const codeBlock = button.closest('.code-block');
      const codeElement = codeBlock.querySelector('code');

      if (!codeElement) return;

      try {
        // Get text content, preserving newlines but removing extra whitespace
        const text = codeElement.textContent.trim();

        // Use Clipboard API if available, fallback to execCommand
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(text);
        } else {
          // Fallback for older browsers
          const textArea = document.createElement('textarea');
          textArea.value = text;
          textArea.style.position = 'fixed';
          textArea.style.left = '-9999px';
          document.body.appendChild(textArea);
          textArea.select();
          document.execCommand('copy');
          document.body.removeChild(textArea);
        }

        // Visual feedback
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.classList.add('copied');

        setTimeout(function() {
          button.textContent = originalText;
          button.classList.remove('copied');
        }, 2000);
      } catch (err) {
        console.error('Failed to copy text:', err);
        button.textContent = 'Failed';
        setTimeout(function() {
          button.textContent = 'Copy';
        }, 2000);
      }
    });
  });
}

/**
 * Smooth scroll for anchor links (Scenario 5)
 */
function initSmoothScroll() {
  const links = document.querySelectorAll('a[href^="#"]');

  links.forEach(function(link) {
    link.addEventListener('click', function(e) {
      const targetId = link.getAttribute('href');
      if (targetId === '#') return;

      const target = document.querySelector(targetId);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
}

/**
 * Mobile navigation toggle (Scenario 5)
 */
function initMobileNav() {
  const mobileMenuBtn = document.querySelector('#mobile-menu-btn');
  const mobileMenu = document.querySelector('#mobile-menu');

  if (mobileMenuBtn && mobileMenu) {
    mobileMenuBtn.addEventListener('click', function() {
      const isExpanded = mobileMenuBtn.getAttribute('aria-expanded') === 'true';
      mobileMenuBtn.setAttribute('aria-expanded', !isExpanded);
      mobileMenu.classList.toggle('hidden');
    });
  }
}

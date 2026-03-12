/**
 * MirDB Homepage JavaScript
 * Owner: Scenario 8 - Progressive Enhancement
 *
 * Purpose: Optional enhancements that improve UX but are NOT required
 * for core functionality. Page MUST work fully without this script.
 *
 * Potential enhancements:
 * - Copy-to-clipboard for code blocks
 * - Smooth scroll behavior
 * - Animation triggers
 *
 * IMPORTANT: All features must degrade gracefully when JS is disabled.
 */

// Progressive enhancement - all features are optional
(function() {
  'use strict';

  // Code block copy functionality (enhancement only)
  function initCodeBlockCopy() {
    const codeBlocks = document.querySelectorAll('.code-block');

    codeBlocks.forEach(block => {
      const copyButton = document.createElement('button');
      copyButton.textContent = 'Copy';
      copyButton.className = 'code-copy-btn';
      copyButton.setAttribute('aria-label', 'Copy code to clipboard');

      copyButton.addEventListener('click', async () => {
        const code = block.querySelector('code');
        if (code) {
          try {
            await navigator.clipboard.writeText(code.textContent);
            copyButton.textContent = 'Copied!';
            setTimeout(() => {
              copyButton.textContent = 'Copy';
            }, 2000);
          } catch (err) {
            // Graceful degradation - button just won't work
            console.warn('Copy failed:', err);
          }
        }
      });

      block.style.position = 'relative';
      block.appendChild(copyButton);
    });
  }

  // Initialize enhancements when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCodeBlockCopy);
  } else {
    initCodeBlockCopy();
  }
})();

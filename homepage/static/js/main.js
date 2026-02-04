/**
 * Main JavaScript for MirDB Homepage
 * Owner: Scenario 3 - Code Examples (primary)
 *
 * Expected functionality:
 * - Tab switching for code examples
 * - Copy-to-clipboard functionality
 * - Progressive enhancement (works without JS for core content)
 *
 * NOTE: Core content must be accessible without JavaScript
 */

(function() {
    'use strict';

    // Copy to clipboard functionality
    function initCopyButtons() {
        const codeBlocks = document.querySelectorAll('.code-examples pre');

        codeBlocks.forEach(function(block) {
            const button = document.createElement('button');
            button.className = 'copy-btn';
            button.textContent = 'Copy';
            button.setAttribute('aria-label', 'Copy code to clipboard');

            button.addEventListener('click', function() {
                const code = block.querySelector('code');
                if (code) {
                    navigator.clipboard.writeText(code.textContent).then(function() {
                        button.textContent = 'Copied!';
                        setTimeout(function() {
                            button.textContent = 'Copy';
                        }, 2000);
                    }).catch(function(err) {
                        console.error('Failed to copy:', err);
                    });
                }
            });

            block.style.position = 'relative';
            block.appendChild(button);
        });
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initCopyButtons);
    } else {
        initCopyButtons();
    }
})();

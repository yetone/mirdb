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

    // Copy to clipboard functionality
    document.querySelectorAll('.copy-btn').forEach(function(btn) {
        btn.addEventListener('click', function() {
            var codeBlock = this.closest('.code-block');
            var code = codeBlock.querySelector('code');
            if (code) {
                navigator.clipboard.writeText(code.textContent).then(function() {
                    btn.textContent = 'Copied!';
                    setTimeout(function() {
                        btn.textContent = 'Copy';
                    }, 2000);
                });
            }
        });
    });

    // Mobile menu toggle
    var hamburger = document.querySelector('.nav-hamburger');
    var navLinks = document.querySelector('.nav-links');
    if (hamburger && navLinks) {
        hamburger.addEventListener('click', function() {
            navLinks.classList.toggle('mobile-open');
            this.classList.toggle('active');
        });
    }
})();

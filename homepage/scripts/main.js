/**
 * MirDB Homepage JavaScript
 *
 * Handles interactive functionality for the homepage.
 *
 * Features:
 * - Copy-to-clipboard for code examples
 * - Smooth scroll navigation
 *
 * Dependencies:
 * - Prism.js (loaded via CDN for syntax highlighting)
 */

document.addEventListener('DOMContentLoaded', function() {
    initCopyButtons();
    initSmoothScroll();
});

/**
 * Initialize copy-to-clipboard functionality for code blocks
 */
function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-btn');

    copyButtons.forEach(function(button) {
        button.addEventListener('click', function() {
            const codeBlock = this.closest('.code-block');
            const code = codeBlock.querySelector('code');
            const text = code.textContent;

            navigator.clipboard.writeText(text).then(function() {
                button.textContent = 'Copied!';
                button.classList.add('copied');

                setTimeout(function() {
                    button.textContent = 'Copy';
                    button.classList.remove('copied');
                }, 2000);
            }).catch(function(err) {
                console.error('Failed to copy:', err);
                button.textContent = 'Failed';
                setTimeout(function() {
                    button.textContent = 'Copy';
                }, 2000);
            });
        });
    });
}

/**
 * Initialize smooth scroll for anchor links
 */
function initSmoothScroll() {
    const links = document.querySelectorAll('a[href^="#"]');

    links.forEach(function(link) {
        link.addEventListener('click', function(e) {
            const href = this.getAttribute('href');
            if (href === '#') return;

            const target = document.querySelector(href);
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

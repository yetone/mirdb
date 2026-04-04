/**
 * MirDB Homepage - Main JavaScript
 * Optional enhancements: smooth scroll, syntax highlighting
 */

// Smooth scroll for anchor links (polyfill for older browsers)
document.addEventListener('DOMContentLoaded', function() {
    // Check if smooth scroll is not natively supported
    if (!('scrollBehavior' in document.documentElement.style)) {
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', function(e) {
                const targetId = this.getAttribute('href');
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
});

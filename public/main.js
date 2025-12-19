/**
 * MirDB Homepage JavaScript
 * Handles copy-to-clipboard functionality
 */

document.addEventListener('DOMContentLoaded', function() {
    initCopyButtons();
    initMobileMenu();
    initSmoothScroll();
});

/**
 * Initialize mobile menu toggle functionality
 */
function initMobileMenu() {
    const mobileMenuBtn = document.querySelector('.mobile-menu-btn');
    const navLinks = document.querySelector('.nav-links');

    if (mobileMenuBtn && navLinks) {
        mobileMenuBtn.addEventListener('click', function() {
            const isExpanded = this.getAttribute('aria-expanded') === 'true';
            this.setAttribute('aria-expanded', !isExpanded);
            navLinks.classList.toggle('active');
        });

        // Close menu when clicking a link
        navLinks.querySelectorAll('a').forEach(function(link) {
            link.addEventListener('click', function() {
                navLinks.classList.remove('active');
                mobileMenuBtn.setAttribute('aria-expanded', 'false');
            });
        });
    }
}

/**
 * Initialize copy-to-clipboard functionality for code blocks
 */
function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-btn');

    copyButtons.forEach(function(button) {
        button.addEventListener('click', async function() {
            const codeBlock = this.closest('.code-block-wrapper').querySelector('code');
            const textToCopy = codeBlock.textContent;

            try {
                await navigator.clipboard.writeText(textToCopy);
                showCopySuccess(this);
            } catch (err) {
                // Fallback for older browsers
                fallbackCopy(textToCopy, this);
            }
        });
    });
}

/**
 * Show success state on copy button
 */
function showCopySuccess(button) {
    const copyIcon = button.querySelector('.copy-icon');
    const checkIcon = button.querySelector('.check-icon');
    const copyText = button.querySelector('.copy-text');

    // Update button state
    button.classList.add('copied');
    copyIcon.classList.add('hidden');
    checkIcon.classList.remove('hidden');
    copyText.textContent = 'Copied!';

    // Reset after 2 seconds
    setTimeout(function() {
        button.classList.remove('copied');
        copyIcon.classList.remove('hidden');
        checkIcon.classList.add('hidden');
        copyText.textContent = 'Copy';
    }, 2000);
}

/**
 * Fallback copy method for browsers without clipboard API
 */
function fallbackCopy(text, button) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
        document.execCommand('copy');
        showCopySuccess(button);
    } catch (err) {
        console.error('Failed to copy text:', err);
    }

    document.body.removeChild(textArea);
}

/**
 * Initialize smooth scroll for anchor links
 */
function initSmoothScroll() {
    document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
        anchor.addEventListener('click', function(e) {
            const targetId = this.getAttribute('href');
            if (targetId === '#') return;

            const targetElement = document.querySelector(targetId);
            if (targetElement) {
                e.preventDefault();
                const navbar = document.querySelector('.navbar, .site-header');
                const navbarHeight = navbar ? navbar.offsetHeight : 0;
                const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset - navbarHeight;

                window.scrollTo({
                    top: targetPosition,
                    behavior: 'smooth'
                });
            }
        });
    });
}

/**
 * MirDB Homepage JavaScript
 * Owner: Scenario 4 - Navigation and Resources
 *
 * This file contains minimal JavaScript for the MirDB homepage.
 *
 * Expected functionality:
 * - Smooth scroll for anchor links
 * - Mobile navigation toggle (if applicable)
 * - Copy-to-clipboard for code examples
 *
 * Requirements traced:
 * - NFR-1: No heavy JavaScript frameworks
 * - REQ-3: Quick start code examples
 */

(function() {
    'use strict';

    /**
     * Initialize smooth scrolling for anchor links
     * Provides enhanced smooth scrolling with offset for sticky header
     */
    function initSmoothScroll() {
        const navLinks = document.querySelectorAll('a[href^="#"]');
        const headerHeight = document.querySelector('header')?.offsetHeight || 0;

        navLinks.forEach(function(link) {
            link.addEventListener('click', function(e) {
                const href = this.getAttribute('href');

                // Only handle internal anchor links
                if (href && href.startsWith('#') && href.length > 1) {
                    const targetId = href.substring(1);
                    const targetElement = document.getElementById(targetId);

                    if (targetElement) {
                        e.preventDefault();

                        const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset;
                        const offsetPosition = targetPosition - headerHeight - 20;

                        window.scrollTo({
                            top: offsetPosition,
                            behavior: 'smooth'
                        });

                        // Update URL hash without jumping
                        if (history.pushState) {
                            history.pushState(null, null, href);
                        }
                    }
                }
            });
        });
    }

    /**
     * Initialize copy-to-clipboard functionality for code blocks
     */
    function initCopyToClipboard() {
        const codeBlocks = document.querySelectorAll('pre code, .code-block');

        codeBlocks.forEach(function(codeBlock) {
            // Create copy button
            const copyButton = document.createElement('button');
            copyButton.className = 'copy-btn';
            copyButton.textContent = 'Copy';
            copyButton.setAttribute('aria-label', 'Copy code to clipboard');

            copyButton.addEventListener('click', function() {
                const code = codeBlock.textContent;

                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(code).then(function() {
                        copyButton.textContent = 'Copied!';
                        setTimeout(function() {
                            copyButton.textContent = 'Copy';
                        }, 2000);
                    }).catch(function() {
                        fallbackCopyTextToClipboard(code, copyButton);
                    });
                } else {
                    fallbackCopyTextToClipboard(code, copyButton);
                }
            });

            // Wrap code block and add button
            const wrapper = codeBlock.parentElement;
            if (wrapper && wrapper.tagName === 'PRE') {
                wrapper.style.position = 'relative';
                wrapper.appendChild(copyButton);
            }
        });
    }

    /**
     * Fallback copy method for browsers without clipboard API
     */
    function fallbackCopyTextToClipboard(text, button) {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();

        try {
            document.execCommand('copy');
            button.textContent = 'Copied!';
            setTimeout(function() {
                button.textContent = 'Copy';
            }, 2000);
        } catch (err) {
            button.textContent = 'Failed';
            setTimeout(function() {
                button.textContent = 'Copy';
            }, 2000);
        }

        document.body.removeChild(textArea);
    }

    /**
     * Initialize mobile navigation toggle
     */
    function initMobileNav() {
        const header = document.querySelector('header');
        const nav = document.querySelector('header nav');

        if (!header || !nav) return;

        // Check if mobile menu button exists, create if needed for mobile
        let mobileMenuBtn = document.querySelector('.mobile-menu-btn');

        if (!mobileMenuBtn) {
            mobileMenuBtn = document.createElement('button');
            mobileMenuBtn.className = 'mobile-menu-btn';
            mobileMenuBtn.setAttribute('aria-label', 'Toggle navigation menu');
            mobileMenuBtn.setAttribute('aria-expanded', 'false');
            mobileMenuBtn.innerHTML = '<span></span><span></span><span></span>';

            const headerContainer = header.querySelector('.container');
            if (headerContainer) {
                headerContainer.appendChild(mobileMenuBtn);
            }
        }

        mobileMenuBtn.addEventListener('click', function() {
            const isExpanded = this.getAttribute('aria-expanded') === 'true';
            this.setAttribute('aria-expanded', !isExpanded);
            nav.classList.toggle('nav-open');
            this.classList.toggle('active');
        });

        // Close mobile nav when clicking outside
        document.addEventListener('click', function(e) {
            if (!header.contains(e.target) && nav.classList.contains('nav-open')) {
                nav.classList.remove('nav-open');
                mobileMenuBtn.classList.remove('active');
                mobileMenuBtn.setAttribute('aria-expanded', 'false');
            }
        });

        // Close mobile nav when clicking a nav link
        nav.querySelectorAll('a').forEach(function(link) {
            link.addEventListener('click', function() {
                nav.classList.remove('nav-open');
                mobileMenuBtn.classList.remove('active');
                mobileMenuBtn.setAttribute('aria-expanded', 'false');
            });
        });
    }

    /**
     * Add active state to navigation based on scroll position
     */
    function initScrollSpy() {
        const sections = document.querySelectorAll('section[id]');
        const navLinks = document.querySelectorAll('header nav a[href^="#"]');

        if (sections.length === 0 || navLinks.length === 0) return;

        function updateActiveLink() {
            const scrollPosition = window.scrollY + 100;

            sections.forEach(function(section) {
                const sectionTop = section.offsetTop;
                const sectionHeight = section.offsetHeight;
                const sectionId = section.getAttribute('id');

                if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
                    navLinks.forEach(function(link) {
                        link.classList.remove('active');
                        if (link.getAttribute('href') === '#' + sectionId) {
                            link.classList.add('active');
                        }
                    });
                }
            });
        }

        window.addEventListener('scroll', updateActiveLink);
        updateActiveLink();
    }

    // Initialize all functionality when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    function init() {
        initSmoothScroll();
        initCopyToClipboard();
        initMobileNav();
        initScrollSpy();
    }
})();

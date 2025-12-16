/**
 * MirDB Homepage - Main JavaScript
 * Handles smooth scrolling, Mermaid diagrams, and any interactive elements
 */

(function() {
    'use strict';

    /**
     * Initialize Mermaid.js for architecture diagrams
     */
    function initMermaid() {
        if (typeof mermaid !== 'undefined') {
            mermaid.initialize({
                startOnLoad: true,
                theme: 'default',
                securityLevel: 'loose',
                flowchart: {
                    useMaxWidth: true,
                    htmlLabels: true,
                    curve: 'basis'
                }
            });
            console.log('Mermaid.js initialized successfully');
            return true;
        } else {
            console.warn('Mermaid.js not loaded');
            return false;
        }
    }

    /**
     * Initialize smooth scrolling for anchor links
     */
    function initSmoothScroll() {
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', function(e) {
                e.preventDefault();
                const targetId = this.getAttribute('href');
                const targetElement = document.querySelector(targetId);

                if (targetElement) {
                    targetElement.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            });
        });
    }

    /**
     * Initialize hero section functionality
     */
    function initHeroSection() {
        const heroSection = document.getElementById('hero');
        const heroTitle = document.querySelector('[data-testid="hero-title"]');
        const heroTagline = document.querySelector('[data-testid="hero-tagline"]');
        const getStartedBtn = document.querySelector('[data-testid="get-started-btn"]');
        const githubBtn = document.querySelector('[data-testid="github-btn"]');

        // Validate hero section elements exist
        if (!heroSection || !heroTitle || !heroTagline) {
            console.error('Hero section: Required elements not found');
            return false;
        }

        // Add keyboard accessibility for buttons
        [getStartedBtn, githubBtn].forEach(btn => {
            if (btn) {
                btn.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        this.click();
                    }
                });
            }
        });

        return true;
    }

    /**
     * Check for any JavaScript errors during initialization
     * @returns {boolean} - True if no errors, false otherwise
     */
    function validateHeroRender() {
        try {
            const hero = document.querySelector('[data-testid="hero-section"]');
            const title = document.querySelector('[data-testid="hero-title"]');
            const tagline = document.querySelector('[data-testid="hero-tagline"]');
            const description = document.querySelector('[data-testid="hero-description"]');
            const ctaButtons = document.querySelector('[data-testid="hero-cta-buttons"]');
            const getStartedBtn = document.querySelector('[data-testid="get-started-btn"]');
            const githubBtn = document.querySelector('[data-testid="github-btn"]');

            // Verify all required elements exist
            const elements = {
                'hero-section': hero,
                'hero-title': title,
                'hero-tagline': tagline,
                'hero-description': description,
                'hero-cta-buttons': ctaButtons,
                'get-started-btn': getStartedBtn,
                'github-btn': githubBtn
            };

            for (const [name, element] of Object.entries(elements)) {
                if (!element) {
                    console.error(`Hero section validation failed: ${name} not found`);
                    return false;
                }
            }

            // Verify content is present
            if (!title.textContent.includes('MirDB')) {
                console.error('Hero section validation failed: Title does not contain MirDB');
                return false;
            }

            console.log('Hero section rendered successfully');
            return true;
        } catch (error) {
            console.error('Hero section validation error:', error);
            return false;
        }
    }

    /**
     * Initialize copy-to-clipboard functionality for code blocks
     */
    function initCopyButtons() {
        const copyButtons = document.querySelectorAll('.copy-btn');

        copyButtons.forEach(button => {
            button.addEventListener('click', async function() {
                const codeBlock = this.closest('.code-block');
                const codeElement = codeBlock.querySelector('code');

                if (!codeElement) {
                    console.error('No code element found in code block');
                    return;
                }

                const codeText = codeElement.textContent;

                try {
                    await navigator.clipboard.writeText(codeText);

                    // Show success feedback
                    const originalText = this.textContent;
                    this.textContent = 'Copied!';
                    this.classList.add('copied');

                    // Reset button after 2 seconds
                    setTimeout(() => {
                        this.textContent = originalText;
                        this.classList.remove('copied');
                    }, 2000);
                } catch (err) {
                    // Fallback for browsers that don't support clipboard API
                    console.error('Failed to copy:', err);
                    fallbackCopyToClipboard(codeText, this);
                }
            });
        });
    }

    /**
     * Fallback copy method for browsers without clipboard API
     * @param {string} text - Text to copy
     * @param {HTMLElement} button - The copy button element
     */
    function fallbackCopyToClipboard(text, button) {
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
            const originalText = button.textContent;
            button.textContent = 'Copied!';
            button.classList.add('copied');

            setTimeout(() => {
                button.textContent = originalText;
                button.classList.remove('copied');
            }, 2000);
        } catch (err) {
            console.error('Fallback copy failed:', err);
            button.textContent = 'Error';
            setTimeout(() => {
                button.textContent = 'Copy';
            }, 2000);
        }

        document.body.removeChild(textArea);
    }

    /**
     * Validate code examples section rendering
     * @returns {boolean} - True if valid, false otherwise
     */
    function validateCodeExamplesSection() {
        try {
            const section = document.querySelector('[data-testid="code-examples-section"]');
            const title = document.querySelector('[data-testid="code-examples-title"]');
            const commandsGrid = document.querySelector('[data-testid="commands-grid"]');

            // Check if section exists
            if (!section || !title || !commandsGrid) {
                return false;
            }

            // Check for required command cards
            const requiredCommands = ['get', 'set', 'delete', 'gets', 'add', 'replace', 'append', 'prepend'];
            for (const cmd of requiredCommands) {
                const card = document.querySelector(`[data-testid="command-card-${cmd}"]`);
                if (!card) {
                    console.error(`Code examples section: Command card for '${cmd}' not found`);
                    return false;
                }
            }

            // Check that copy buttons exist
            const copyButtons = section.querySelectorAll('.copy-btn');
            if (copyButtons.length < requiredCommands.length) {
                console.error('Code examples section: Not enough copy buttons found');
                return false;
            }

            console.log('Code examples section rendered successfully');
            return true;
        } catch (error) {
            console.error('Code examples section validation error:', error);
            return false;
        }
    }

    /**
     * Validate How It Works / Architecture section rendering
     * @returns {boolean} - True if valid, false otherwise
     */
    function validateHowItWorksSection() {
        try {
            const section = document.querySelector('[data-testid="how-it-works-section"]');
            const title = document.querySelector('[data-testid="how-it-works-title"]');
            const diagramContainer = document.querySelector('[data-testid="architecture-diagram-container"]');
            const diagram = document.querySelector('[data-testid="architecture-diagram"]');
            const diagramAlt = document.querySelector('[data-testid="architecture-diagram-alt"]');
            const writePathCard = document.querySelector('[data-testid="write-path-card"]');
            const readPathCard = document.querySelector('[data-testid="read-path-card"]');

            // Check if section exists
            if (!section || !title) {
                console.error('How It Works section: Section or title not found');
                return false;
            }

            // Check diagram elements
            if (!diagramContainer || !diagram) {
                console.error('How It Works section: Diagram container or diagram not found');
                return false;
            }

            // Check for accessibility alt text
            if (!diagramAlt) {
                console.error('How It Works section: Diagram alt text not found');
                return false;
            }

            // Check for write and read path explanations
            if (!writePathCard || !readPathCard) {
                console.error('How It Works section: Write or read path card not found');
                return false;
            }

            // Check write path steps
            const writeSteps = document.querySelector('[data-testid="write-path-steps"]');
            if (!writeSteps || writeSteps.children.length < 4) {
                console.error('How It Works section: Write path steps missing or incomplete');
                return false;
            }

            // Check read path steps
            const readSteps = document.querySelector('[data-testid="read-path-steps"]');
            if (!readSteps || readSteps.children.length < 4) {
                console.error('How It Works section: Read path steps missing or incomplete');
                return false;
            }

            console.log('How It Works section rendered successfully');
            return true;
        } catch (error) {
            console.error('How It Works section validation error:', error);
            return false;
        }
    }

    /**
     * Main initialization function
     */
    function init() {
        initMermaid();
        initSmoothScroll();
        initHeroSection();
        validateHeroRender();
        initCopyButtons();
        validateCodeExamplesSection();
        validateHowItWorksSection();
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Expose validation functions for testing
    window.MirDBHomepage = {
        validateHeroRender: validateHeroRender,
        validateCodeExamplesSection: validateCodeExamplesSection,
        validateHowItWorksSection: validateHowItWorksSection
    };
})();

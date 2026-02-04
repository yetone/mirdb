/**
 * Main JavaScript for MirDB Homepage
 * Owner: Scenario 3 - Code Examples (primary)
 *
 * Features:
 * - Tab switching for code examples (keyboard accessible)
 * - Copy-to-clipboard functionality
 * - Progressive enhancement (works without JS for core content)
 */

(function() {
    'use strict';

    /**
     * Initialize tab functionality with keyboard support
     * Implements WAI-ARIA Tabs pattern
     */
    function initTabs() {
        const tablist = document.querySelector('[role="tablist"]');
        if (!tablist) return;

        const tabs = tablist.querySelectorAll('[role="tab"]');
        const panels = document.querySelectorAll('[role="tabpanel"]');

        if (tabs.length === 0) return;

        // Click handler for tabs
        tabs.forEach(function(tab) {
            tab.addEventListener('click', function() {
                activateTab(tab, tabs, panels);
            });
        });

        // Keyboard navigation for tabs (Arrow keys, Home, End)
        tablist.addEventListener('keydown', function(event) {
            const currentTab = document.activeElement;
            if (!currentTab.matches('[role="tab"]')) return;

            const tabsArray = Array.from(tabs);
            const currentIndex = tabsArray.indexOf(currentTab);
            let newIndex;

            switch (event.key) {
                case 'ArrowLeft':
                    newIndex = currentIndex - 1;
                    if (newIndex < 0) newIndex = tabsArray.length - 1;
                    break;
                case 'ArrowRight':
                    newIndex = currentIndex + 1;
                    if (newIndex >= tabsArray.length) newIndex = 0;
                    break;
                case 'Home':
                    newIndex = 0;
                    break;
                case 'End':
                    newIndex = tabsArray.length - 1;
                    break;
                default:
                    return;
            }

            event.preventDefault();
            tabsArray[newIndex].focus();
            activateTab(tabsArray[newIndex], tabs, panels);
        });
    }

    /**
     * Activate a specific tab and show its panel
     * @param {HTMLElement} selectedTab - The tab to activate
     * @param {NodeList} tabs - All tabs
     * @param {NodeList} panels - All panels
     */
    function activateTab(selectedTab, tabs, panels) {
        // Deactivate all tabs
        tabs.forEach(function(tab) {
            tab.setAttribute('aria-selected', 'false');
            tab.setAttribute('tabindex', '-1');
            tab.classList.remove('code-tabs__tab--active');
        });

        // Hide all panels
        panels.forEach(function(panel) {
            panel.setAttribute('hidden', '');
            panel.classList.remove('code-panel--active');
        });

        // Activate selected tab
        selectedTab.setAttribute('aria-selected', 'true');
        selectedTab.setAttribute('tabindex', '0');
        selectedTab.classList.add('code-tabs__tab--active');

        // Show corresponding panel
        var panelId = selectedTab.getAttribute('aria-controls');
        var panel = document.getElementById(panelId);
        if (panel) {
            panel.removeAttribute('hidden');
            panel.classList.add('code-panel--active');
        }
    }

    /**
     * Initialize copy-to-clipboard functionality
     */
    function initCopyButtons() {
        var copyButtons = document.querySelectorAll('.copy-btn');

        copyButtons.forEach(function(button) {
            button.addEventListener('click', function() {
                var codeText = button.getAttribute('data-code');
                if (!codeText) {
                    // Fallback: get text from sibling code block
                    var codeBlock = button.previousElementSibling;
                    if (codeBlock) {
                        codeText = codeBlock.textContent;
                    }
                }

                if (codeText && navigator.clipboard) {
                    navigator.clipboard.writeText(codeText).then(function() {
                        // Provide visual feedback
                        var originalText = button.textContent;
                        button.textContent = 'Copied!';
                        button.setAttribute('aria-label', 'Code copied to clipboard');

                        setTimeout(function() {
                            button.textContent = originalText;
                            button.setAttribute('aria-label', 'Copy code to clipboard');
                        }, 2000);
                    }).catch(function(err) {
                        console.error('Failed to copy:', err);
                        button.textContent = 'Failed';
                        setTimeout(function() {
                            button.textContent = 'Copy';
                        }, 2000);
                    });
                }
            });
        });
    }

    /**
     * Initialize smooth scrolling for anchor links
     * With proper focus management for accessibility
     */
    function initSmoothScroll() {
        var anchorLinks = document.querySelectorAll('a[href^="#"]');

        anchorLinks.forEach(function(link) {
            link.addEventListener('click', function(event) {
                var targetId = link.getAttribute('href').slice(1);
                var target = document.getElementById(targetId);

                if (target) {
                    event.preventDefault();

                    // Scroll to target
                    target.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });

                    // Set focus for accessibility (skip-link and navigation)
                    if (target.getAttribute('tabindex') === null) {
                        target.setAttribute('tabindex', '-1');
                    }
                    target.focus({ preventScroll: true });

                    // Update URL hash
                    history.pushState(null, '', '#' + targetId);
                }
            });
        });
    }

    /**
     * Initialize all functionality when DOM is ready
     */
    function init() {
        initTabs();
        initCopyButtons();
        initSmoothScroll();
    }

    // Run when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();

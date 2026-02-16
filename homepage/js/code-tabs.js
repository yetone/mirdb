/**
 * Code Tabs Module
 * Owner: Scenario 5 - Code Examples Section
 *
 * Handles tab switching, keyboard navigation, and state persistence
 * for the code examples section.
 */

var CodeTabs = (function() {
  'use strict';

  var STORAGE_KEY = 'mirdb-preferred-language';
  var SUPPORTED_LANGUAGES = ['python', 'go', 'nodejs'];
  var DEFAULT_LANGUAGE = 'python';

  var tablist = null;
  var tabs = [];
  var panels = [];

  /**
   * Initialize the code tabs functionality
   */
  function init() {
    tablist = document.querySelector('.code-examples__tabs[role="tablist"]');
    if (!tablist) {
      return;
    }

    tabs = Array.prototype.slice.call(tablist.querySelectorAll('[role="tab"]'));
    panels = SUPPORTED_LANGUAGES.map(function(lang) {
      return document.getElementById('panel-' + lang);
    }).filter(Boolean);

    if (tabs.length === 0 || panels.length === 0) {
      return;
    }

    // Set up event listeners
    setupEventListeners();

    // Restore saved language preference
    restoreLanguagePreference();
  }

  /**
   * Set up click and keyboard event listeners
   */
  function setupEventListeners() {
    tabs.forEach(function(tab) {
      tab.addEventListener('click', handleTabClick);
      tab.addEventListener('keydown', handleKeydown);
    });
  }

  /**
   * Handle tab click
   * @param {Event} event - Click event
   */
  function handleTabClick(event) {
    var clickedTab = event.currentTarget;
    activateTab(clickedTab);
  }

  /**
   * Handle keyboard navigation
   * @param {KeyboardEvent} event - Keyboard event
   */
  function handleKeydown(event) {
    var key = event.key;
    var currentIndex = tabs.indexOf(event.currentTarget);
    var newIndex = currentIndex;

    switch (key) {
      case 'ArrowLeft':
        newIndex = currentIndex === 0 ? tabs.length - 1 : currentIndex - 1;
        event.preventDefault();
        break;
      case 'ArrowRight':
        newIndex = currentIndex === tabs.length - 1 ? 0 : currentIndex + 1;
        event.preventDefault();
        break;
      case 'Home':
        newIndex = 0;
        event.preventDefault();
        break;
      case 'End':
        newIndex = tabs.length - 1;
        event.preventDefault();
        break;
      case 'Enter':
      case ' ':
        activateTab(tabs[currentIndex]);
        event.preventDefault();
        return;
      default:
        return;
    }

    // Move focus to new tab
    tabs[newIndex].focus();
  }

  /**
   * Activate a specific tab
   * @param {HTMLElement} selectedTab - The tab to activate
   */
  function activateTab(selectedTab) {
    if (!selectedTab) {
      return;
    }

    var selectedLanguage = getLanguageFromTab(selectedTab);

    // Deactivate all tabs
    tabs.forEach(function(tab) {
      tab.setAttribute('aria-selected', 'false');
      tab.setAttribute('tabindex', '-1');
      tab.classList.remove('code-examples__tab--active');
    });

    // Activate selected tab
    selectedTab.setAttribute('aria-selected', 'true');
    selectedTab.setAttribute('tabindex', '0');
    selectedTab.classList.add('code-examples__tab--active');

    // Hide all panels
    panels.forEach(function(panel) {
      panel.hidden = true;
      panel.classList.remove('code-examples__panel--active');
    });

    // Show selected panel
    var selectedPanel = document.getElementById('panel-' + selectedLanguage);
    if (selectedPanel) {
      selectedPanel.hidden = false;
      selectedPanel.classList.add('code-examples__panel--active');
    }

    // Save preference
    saveLanguagePreference(selectedLanguage);
  }

  /**
   * Get language identifier from tab element
   * @param {HTMLElement} tab - Tab element
   * @returns {string} Language identifier
   */
  function getLanguageFromTab(tab) {
    var id = tab.id;
    return id ? id.replace('tab-', '') : DEFAULT_LANGUAGE;
  }

  /**
   * Save language preference to localStorage
   * @param {string} language - Language identifier
   */
  function saveLanguagePreference(language) {
    try {
      localStorage.setItem(STORAGE_KEY, language);
    } catch (e) {
      // localStorage not available, ignore
    }
  }

  /**
   * Restore language preference from localStorage
   */
  function restoreLanguagePreference() {
    var savedLanguage = null;

    try {
      savedLanguage = localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      // localStorage not available, use default
    }

    // Validate saved language
    if (savedLanguage && SUPPORTED_LANGUAGES.indexOf(savedLanguage) !== -1) {
      var tabToActivate = document.getElementById('tab-' + savedLanguage);
      if (tabToActivate) {
        activateTab(tabToActivate);
        return;
      }
    }

    // Default to Python if no valid preference
    var defaultTab = document.getElementById('tab-' + DEFAULT_LANGUAGE);
    if (defaultTab) {
      activateTab(defaultTab);
    }
  }

  /**
   * Switch to a specific language tab (public API)
   * @param {string} language - Language identifier (python, go, nodejs)
   */
  function switchTab(language) {
    var normalizedLang = language.toLowerCase();
    if (SUPPORTED_LANGUAGES.indexOf(normalizedLang) === -1) {
      console.warn('Unsupported language:', language);
      return;
    }

    var tab = document.getElementById('tab-' + normalizedLang);
    if (tab) {
      activateTab(tab);
      tab.focus();
    }
  }

  /**
   * Get the currently active language
   * @returns {string} Active language identifier
   */
  function getActiveLanguage() {
    var activeTab = tablist ? tablist.querySelector('[aria-selected="true"]') : null;
    return activeTab ? getLanguageFromTab(activeTab) : DEFAULT_LANGUAGE;
  }

  // Public API
  return {
    init: init,
    switchTab: switchTab,
    getActiveLanguage: getActiveLanguage
  };
})();

// Auto-initialize on DOMContentLoaded
document.addEventListener('DOMContentLoaded', function() {
  CodeTabs.init();
});

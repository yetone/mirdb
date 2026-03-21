/**
 * MirDB Homepage JavaScript
 *
 * Functions owned by Scenario 3 (Quick Start):
 * - copyToClipboard() - Copy installation command
 * - showCopyFeedback() - Display copy confirmation
 *
 * Functions owned by Scenario 7 (Dark Mode):
 * - initTheme() - Initialize theme from storage/system
 * - toggleTheme() - Switch between light/dark mode
 * - saveThemePreference() - Persist to localStorage
 *
 * Keep JavaScript minimal per PRD requirements.
 */

// === Copy Functionality (Scenario 3) ===

/**
 * Copy text to clipboard and show feedback
 * @param {HTMLButtonElement} button - The copy button element
 * @param {string} targetSelector - Selector for the code element to copy
 */
async function copyToClipboard(button, targetSelector) {
  const codeElement = document.querySelector(targetSelector);
  if (!codeElement) {
    console.error('Target element not found:', targetSelector);
    return;
  }

  const textToCopy = codeElement.textContent.trim();

  try {
    await navigator.clipboard.writeText(textToCopy);
    showCopyFeedback(button, true);
  } catch (err) {
    // Fallback for browsers that don't support clipboard API
    try {
      const textArea = document.createElement('textarea');
      textArea.value = textToCopy;
      textArea.style.position = 'fixed';
      textArea.style.left = '-9999px';
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      showCopyFeedback(button, true);
    } catch (fallbackErr) {
      console.error('Failed to copy:', fallbackErr);
      showCopyFeedback(button, false);
    }
  }
}

/**
 * Show visual feedback after copy action
 * @param {HTMLButtonElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  const originalText = button.getAttribute('data-original-text') || button.textContent;
  const originalAriaLabel = button.getAttribute('aria-label');

  if (success) {
    button.classList.add('copied');
    button.textContent = 'Copied!';
    button.setAttribute('aria-label', 'Copied to clipboard');
  } else {
    button.textContent = 'Failed';
    button.setAttribute('aria-label', 'Failed to copy');
  }

  // Reset button after delay
  setTimeout(() => {
    button.classList.remove('copied');
    button.textContent = originalText;
    if (originalAriaLabel) {
      button.setAttribute('aria-label', originalAriaLabel);
    }
  }, 2000);
}

/**
 * Initialize copy buttons on the page
 */
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('[data-copy-target]');

  copyButtons.forEach((button) => {
    // Store original text
    button.setAttribute('data-original-text', button.textContent);

    button.addEventListener('click', () => {
      const targetSelector = button.getAttribute('data-copy-target');
      copyToClipboard(button, targetSelector);
    });
  });
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  initCopyButtons();
});

// === Theme Toggle (Scenario 7) ===

/**
 * Storage key for theme preference
 */
const THEME_STORAGE_KEY = 'mirdb-theme';

/**
 * Get the user's preferred color scheme from system settings
 * @returns {string} 'dark' or 'light'
 */
function getSystemThemePreference() {
  if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    return 'dark';
  }
  return 'light';
}

/**
 * Get the stored theme preference from localStorage
 * @returns {string|null} 'dark', 'light', or null if not set
 */
function getStoredThemePreference() {
  try {
    return localStorage.getItem(THEME_STORAGE_KEY);
  } catch (e) {
    // localStorage may be unavailable in some contexts
    console.warn('Could not access localStorage:', e);
    return null;
  }
}

/**
 * Save theme preference to localStorage
 * @param {string} theme - 'dark' or 'light'
 */
function saveThemePreference(theme) {
  try {
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch (e) {
    console.warn('Could not save to localStorage:', e);
  }
}

/**
 * Apply the theme to the document
 * @param {string} theme - 'dark' or 'light'
 */
function applyTheme(theme) {
  const root = document.documentElement;

  if (theme === 'dark') {
    root.classList.add('dark-mode');
    root.classList.remove('light-mode');
  } else {
    root.classList.remove('dark-mode');
    root.classList.add('light-mode');
  }

  // Update toggle button aria-label
  const toggleButton = document.getElementById('theme-toggle');
  if (toggleButton) {
    const currentMode = theme === 'dark' ? 'dark' : 'light';
    const nextMode = theme === 'dark' ? 'light' : 'dark';
    toggleButton.setAttribute('aria-label', `Switch to ${nextMode} mode (currently ${currentMode})`);
    toggleButton.setAttribute('title', `Switch to ${nextMode} mode`);
  }
}

/**
 * Get the current active theme
 * @returns {string} 'dark' or 'light'
 */
function getCurrentTheme() {
  if (document.documentElement.classList.contains('dark-mode')) {
    return 'dark';
  }
  // Check if we have a stored preference
  const stored = getStoredThemePreference();
  if (stored) {
    return stored;
  }
  // Fall back to system preference
  return getSystemThemePreference();
}

/**
 * Toggle between light and dark themes
 */
function toggleTheme() {
  const currentTheme = getCurrentTheme();
  const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

  applyTheme(newTheme);
  saveThemePreference(newTheme);
}

/**
 * Initialize the theme based on stored preference or system preference
 */
function initTheme() {
  // Check for stored preference first
  const storedTheme = getStoredThemePreference();

  if (storedTheme) {
    // User has a stored preference - apply it
    applyTheme(storedTheme);
  } else {
    // No stored preference - use system preference
    const systemTheme = getSystemThemePreference();
    applyTheme(systemTheme);
  }

  // Set up toggle button listener
  const toggleButton = document.getElementById('theme-toggle');
  if (toggleButton) {
    toggleButton.addEventListener('click', toggleTheme);
  }

  // Listen for system preference changes
  if (window.matchMedia) {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    mediaQuery.addEventListener('change', (e) => {
      // Only respond to system changes if user hasn't set a preference
      if (!getStoredThemePreference()) {
        applyTheme(e.matches ? 'dark' : 'light');
      }
    });
  }
}

// Initialize theme when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  initTheme();
});

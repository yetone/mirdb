/**
 * Syntax Highlighting Initialization
 * Owner: Scenario 2 - Quick Start Section
 *
 * Initializes syntax highlighting for code blocks:
 * - Uses Prism.js for highlighting
 * - Applies highlighting to all code blocks
 * - Supports bash, memcached protocol syntax
 *
 * Note: Basic styling works without JS; this enhances it.
 */

/**
 * Add memcached language support to Prism
 */
function addMemcachedLanguage() {
  if (typeof Prism === 'undefined') return;

  Prism.languages.memcached = {
    'comment': {
      pattern: /#.*/,
      greedy: true
    },
    'command': {
      pattern: /\b(set|get|gets|add|replace|append|prepend|cas|delete|incr|decr|touch|gat|gats|stats|flush_all|version|quit)\b/i,
      alias: 'keyword'
    },
    'response': {
      pattern: /\b(STORED|NOT_STORED|EXISTS|NOT_FOUND|DELETED|TOUCHED|OK|ERROR|CLIENT_ERROR|SERVER_ERROR|VALUE|END)\b/,
      alias: 'builtin'
    },
    'number': /\b\d+\b/,
    'string': {
      pattern: /[a-zA-Z_][a-zA-Z0-9_]*/,
      alias: 'variable'
    }
  };
}

/**
 * Apply syntax highlighting to all code blocks
 */
function applySyntaxHighlighting() {
  if (typeof Prism === 'undefined') {
    console.warn('Prism.js not loaded, syntax highlighting disabled');
    return;
  }

  // Add custom memcached language
  addMemcachedLanguage();

  // Highlight all code blocks
  Prism.highlightAll();
}

/**
 * Initialize syntax highlighting
 */
function initSyntaxHighlighting() {
  // Wait for Prism to load if it hasn't yet
  if (typeof Prism === 'undefined') {
    // Check periodically for Prism to load
    let attempts = 0;
    const maxAttempts = 50;
    const checkInterval = setInterval(() => {
      attempts++;
      if (typeof Prism !== 'undefined') {
        clearInterval(checkInterval);
        applySyntaxHighlighting();
      } else if (attempts >= maxAttempts) {
        clearInterval(checkInterval);
        console.warn('Prism.js failed to load');
      }
    }, 100);
  } else {
    applySyntaxHighlighting();
  }
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initSyntaxHighlighting);
} else {
  initSyntaxHighlighting();
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initSyntaxHighlighting, addMemcachedLanguage };
}

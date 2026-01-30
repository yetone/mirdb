/**
 * MirDB Homepage - Syntax Highlighting
 * Owner: Scenario 5 - Usage and Code Examples
 *
 * Lightweight syntax highlighter for code blocks
 * Supports: bash for memcached command examples
 */

(function() {
  'use strict';

  /**
   * Prism-like syntax highlighting for bash/memcached commands
   * Applies highlighting classes to code elements
   */
  const Prism = {
    /**
     * Token patterns for bash syntax
     */
    patterns: {
      comment: /#.*/g,
      string: /(["'])(?:\\.|(?!\1)[^\\])*\1/g,
      command: /\b(set|get|gets|add|replace|append|prepend|delete|info|major_compaction)\b/gi,
      response: /\b(STORED|END|DELETED|NOT_STORED|EXISTS|NOT_FOUND|VALUE|ERROR)\b/g,
      number: /\b\d+\b/g,
      key: /^([a-zA-Z_][a-zA-Z0-9_]*)\s/gm,
    },

    /**
     * Escapes HTML special characters
     * @param {string} text - Text to escape
     * @returns {string} - Escaped text
     */
    escapeHtml: function(text) {
      return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    },

    /**
     * Highlights code with the given language
     * @param {string} code - Code to highlight
     * @param {string} language - Language identifier
     * @returns {string} - Highlighted HTML
     */
    highlight: function(code, language) {
      // Escape HTML first
      let highlighted = this.escapeHtml(code);

      // Apply syntax highlighting patterns
      // Order matters: apply more specific patterns first

      // Highlight memcached responses (STORED, END, DELETED, etc.)
      highlighted = highlighted.replace(
        this.patterns.response,
        '<span class="token response">$&</span>'
      );

      // Highlight memcached commands (set, get, add, etc.)
      highlighted = highlighted.replace(
        this.patterns.command,
        '<span class="token command">$&</span>'
      );

      // Highlight numbers
      highlighted = highlighted.replace(
        this.patterns.number,
        '<span class="token number">$&</span>'
      );

      // Highlight comments
      highlighted = highlighted.replace(
        this.patterns.comment,
        '<span class="token comment">$&</span>'
      );

      return highlighted;
    },

    /**
     * Initializes syntax highlighting on all code blocks
     */
    highlightAll: function() {
      const codeBlocks = document.querySelectorAll('code[class*="language-"]');

      codeBlocks.forEach(function(block) {
        const language = block.className.match(/language-(\w+)/);
        if (language) {
          const code = block.textContent;
          block.innerHTML = Prism.highlight(code, language[1]);
          block.setAttribute('data-highlighted', 'true');
        }
      });
    },

    /**
     * Highlights a single element
     * @param {HTMLElement} element - Code element to highlight
     */
    highlightElement: function(element) {
      const language = element.className.match(/language-(\w+)/);
      if (language) {
        const code = element.textContent;
        element.innerHTML = this.highlight(code, language[1]);
        element.setAttribute('data-highlighted', 'true');
      }
    }
  };

  // Auto-initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
      Prism.highlightAll();
    });
  } else {
    Prism.highlightAll();
  }

  // Expose Prism globally
  window.Prism = Prism;
})();

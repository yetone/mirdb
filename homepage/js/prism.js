/**
 * MirDB Homepage - Syntax Highlighting
 * Owner: Scenario 5 - Usage and Code Examples
 *
 * Lightweight syntax highlighting for bash/shell commands
 * Supports: bash, shell for memcached protocol examples
 */

(function() {
  'use strict';

  /**
   * Syntax highlighting patterns for bash/shell
   */
  const PATTERNS = {
    comment: {
      pattern: /#.*/g,
      className: 'token comment'
    },
    string: {
      pattern: /(["'])(?:(?!\1)[^\\]|\\.)*\1/g,
      className: 'token string'
    },
    keyword: {
      pattern: /\b(set|get|gets|add|replace|append|prepend|delete|telnet|localhost)\b/gi,
      className: 'token keyword'
    },
    response: {
      pattern: /\b(STORED|DELETED|NOT_STORED|EXISTS|NOT_FOUND|VALUE|END)\b/g,
      className: 'token response'
    },
    number: {
      pattern: /\b\d+\b/g,
      className: 'token number'
    },
    operator: {
      pattern: /[<>]/g,
      className: 'token operator'
    }
  };

  /**
   * Escape HTML special characters
   * @param {string} text - Text to escape
   * @returns {string} - Escaped text
   */
  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  /**
   * Apply syntax highlighting to a code block
   * @param {string} code - Raw code text
   * @param {string} language - Programming language
   * @returns {string} - Highlighted HTML
   */
  function highlight(code, language) {
    if (language !== 'bash' && language !== 'shell') {
      return escapeHtml(code);
    }

    // Process line by line to preserve structure
    const lines = code.split('\n');
    const highlightedLines = lines.map(function(line) {
      // Check if line is a comment first
      if (line.trim().startsWith('#')) {
        return '<span class="token comment">' + escapeHtml(line) + '</span>';
      }

      // Check if line is a memcached response
      if (/^(STORED|DELETED|NOT_STORED|EXISTS|NOT_FOUND|END)$/.test(line.trim())) {
        return '<span class="token response">' + escapeHtml(line) + '</span>';
      }

      // Check if line starts with VALUE (response with data)
      if (line.trim().startsWith('VALUE ')) {
        return '<span class="token response">' + escapeHtml(line) + '</span>';
      }

      let highlighted = escapeHtml(line);

      // Highlight keywords (memcached commands)
      highlighted = highlighted.replace(
        /\b(set|get|gets|add|replace|append|prepend|delete|telnet)\b/gi,
        '<span class="token keyword">$1</span>'
      );

      // Highlight numbers
      highlighted = highlighted.replace(
        /\b(\d+)\b/g,
        '<span class="token number">$1</span>'
      );

      // Highlight localhost
      highlighted = highlighted.replace(
        /\b(localhost)\b/g,
        '<span class="token keyword">$1</span>'
      );

      // Highlight angle brackets (placeholders like <key>)
      highlighted = highlighted.replace(
        /(&lt;[^&]+&gt;)/g,
        '<span class="token placeholder">$1</span>'
      );

      return highlighted;
    });

    return highlightedLines.join('\n');
  }

  /**
   * Initialize syntax highlighting for all code blocks
   */
  function initHighlighting() {
    const codeBlocks = document.querySelectorAll('pre.usage__code code, pre[class*="language-"] code');

    codeBlocks.forEach(function(codeElement) {
      const preElement = codeElement.parentElement;
      const classList = preElement.className || '';

      // Extract language from class name
      let language = 'bash';
      const languageMatch = classList.match(/language-(\w+)/);
      if (languageMatch) {
        language = languageMatch[1];
      }

      // Get raw code text
      const rawCode = codeElement.textContent || '';

      // Apply highlighting
      codeElement.innerHTML = highlight(rawCode, language);

      // Add highlighted class
      preElement.classList.add('highlighted');
    });
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initHighlighting);
  } else {
    initHighlighting();
  }

  // Expose for potential external use
  window.Prism = {
    highlight: highlight,
    highlightAll: initHighlighting
  };
})();

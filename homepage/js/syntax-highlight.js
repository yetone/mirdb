/**
 * Syntax Highlighting Module
 * Owner: Scenario 3 - Usage and Code Examples
 *
 * Expected exports:
 * - highlightCode(): void - Apply syntax highlighting to code blocks
 * - highlightElement(element: HTMLElement): void - Highlight specific element
 *
 * Supports:
 * - Rust code highlighting
 * - Shell/Bash command highlighting
 * - Memcached protocol highlighting
 */

(function() {
  'use strict';

  /**
   * Escape HTML special characters to prevent XSS
   * @param {string} text - Text to escape
   * @returns {string} Escaped text
   */
  function escapeHtml(text) {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  /**
   * Token patterns for different languages
   * These are processed line-by-line to avoid cross-matching issues
   */
  const patterns = {
    memcached: {
      command: /^(set|get|gets|delete|add|replace|append|prepend|cas|incr|decr|stats|flush_all|quit|info|major_compaction)\b/,
      response: /^(STORED|NOT_STORED|EXISTS|NOT_FOUND|DELETED|ERROR|END|VALUE|CLIENT_ERROR|SERVER_ERROR)\b/,
      number: /\b(\d+)\b/g
    },
    bash: {
      comment: /^(#.*)$/,
      keyword: /\b(git|cd|npm|cargo|telnet|python|pip|curl|wget|chmod|mkdir|rm|cp|mv|cat|echo|export)\b/g
    },
    rust: {
      comment: /(\/\/.*$)/,
      keyword: /\b(fn|let|mut|const|if|else|match|loop|while|for|in|return|use|mod|pub|struct|enum|impl|trait|where|async|await|move|ref|self|Self|super|crate|type|dyn|static|unsafe|extern)\b/g,
      string: /("(?:[^"\\]|\\.)*")/g,
      number: /\b(\d+(?:\.\d+)?(?:_\d+)*)\b/g
    }
  };

  /**
   * Highlight a line of memcached code
   * @param {string} line - Line to highlight
   * @returns {string} Highlighted line
   */
  function highlightMemcachedLine(line) {
    let escaped = escapeHtml(line);

    // Check for command at start of line
    if (patterns.memcached.command.test(line)) {
      escaped = escaped.replace(patterns.memcached.command, '<span class="syntax-command">$1</span>');
    }
    // Check for response at start of line
    else if (patterns.memcached.response.test(line)) {
      escaped = escaped.replace(patterns.memcached.response, '<span class="syntax-response">$1</span>');
    }

    // Highlight numbers (but not in spans)
    escaped = escaped.replace(/\b(\d+)\b(?![^<]*>)/g, '<span class="syntax-number">$1</span>');

    return escaped;
  }

  /**
   * Highlight a line of bash code
   * @param {string} line - Line to highlight
   * @returns {string} Highlighted line
   */
  function highlightBashLine(line) {
    let escaped = escapeHtml(line);

    // Check for comment (entire line is a comment)
    if (patterns.bash.comment.test(line)) {
      return '<span class="syntax-comment">' + escaped + '</span>';
    }

    // Highlight keywords
    escaped = escaped.replace(patterns.bash.keyword, '<span class="syntax-keyword">$1</span>');

    return escaped;
  }

  /**
   * Highlight a line of rust code
   * @param {string} line - Line to highlight
   * @returns {string} Highlighted line
   */
  function highlightRustLine(line) {
    let escaped = escapeHtml(line);

    // Check for line comment
    const commentMatch = line.match(patterns.rust.comment);
    if (commentMatch) {
      const commentStart = line.indexOf('//');
      const beforeComment = escapeHtml(line.substring(0, commentStart));
      const comment = escapeHtml(line.substring(commentStart));
      return beforeComment + '<span class="syntax-comment">' + comment + '</span>';
    }

    // Highlight strings
    escaped = escaped.replace(/(&quot;(?:[^&]|&(?!quot;))*&quot;)/g, '<span class="syntax-string">$1</span>');

    // Highlight keywords
    escaped = escaped.replace(patterns.rust.keyword, '<span class="syntax-keyword">$1</span>');

    // Highlight numbers
    escaped = escaped.replace(/\b(\d+(?:\.\d+)?)\b(?![^<]*>)/g, '<span class="syntax-number">$1</span>');

    return escaped;
  }

  /**
   * Apply syntax highlighting to a code string
   * @param {string} code - The code to highlight
   * @param {string} language - The language identifier
   * @returns {string} HTML with highlighting spans
   */
  function highlightString(code, language) {
    if (!patterns[language]) {
      return escapeHtml(code);
    }

    const lines = code.split('\n');
    const highlightedLines = lines.map(line => {
      switch (language) {
        case 'memcached':
          return highlightMemcachedLine(line);
        case 'bash':
          return highlightBashLine(line);
        case 'rust':
          return highlightRustLine(line);
        default:
          return escapeHtml(line);
      }
    });

    return highlightedLines.join('\n');
  }

  /**
   * Highlight a specific code element
   * @param {HTMLElement} element - The code element to highlight
   */
  function highlightElement(element) {
    const pre = element.closest('pre');
    if (!pre) return;

    const language = pre.dataset.language || 'memcached';
    const code = element.textContent;

    // Store original code as data attribute for potential re-highlighting
    if (!element.dataset.originalCode) {
      element.dataset.originalCode = code;
    }

    element.innerHTML = highlightString(code, language);
    pre.classList.add('highlighted');
  }

  /**
   * Apply syntax highlighting to all code blocks on the page
   */
  function highlightCode() {
    const codeBlocks = document.querySelectorAll('pre.code-block code');
    codeBlocks.forEach(highlightElement);
  }

  // Export functions for use by other modules and tests
  if (typeof module !== 'undefined' && module.exports) {
    // Node.js / Jest environment
    module.exports = {
      highlightCode,
      highlightElement,
      highlightString,
      escapeHtml,
      patterns
    };
  } else {
    // Browser environment - attach to window
    window.SyntaxHighlight = {
      highlightCode,
      highlightElement,
      highlightString,
      escapeHtml,
      patterns
    };
  }
})();

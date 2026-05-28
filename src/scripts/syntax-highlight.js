/**
 * Client-Side Syntax Highlighting
 * Owner: Scenario 12 - Visual Design and Theming
 *
 * Expected exports/functions:
 * - highlightAll(): Finds all <pre><code> blocks and applies highlighting
 * - highlightElement(element): Highlights a single code block
 * - Supports memcached protocol commands and TOML config syntax
 */

(function () {
  'use strict';

  const TOKEN_PATTERNS = {
    bash: [
      { type: 'comment', regex: /#.*/g },
      { type: 'string', regex: /"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'/g },
      { type: 'keyword', regex: /\b(if|then|else|elif|fi|for|while|do|done|case|esac|in|function|return|exit|export|source)\b/g },
      { type: 'function', regex: /\b(cargo|git|cd|curl|chmod|sudo|mv|brew|echo|cat|ls|mkdir|rm|cp|scp|ssh|make|docker|kubectl)\b/g },
      { type: 'operator', regex: /[|&;<>()$=`{}]/g },
      { type: 'number', regex: /\b\d+(?:\.\d+)?(?:[KMGTP]i?)?\b/g },
    ],
    toml: [
      { type: 'comment', regex: /#.*/g },
      { type: 'string', regex: /"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'/g },
      { type: 'keyword', regex: /\b(true|false)\b/g },
      { type: 'number', regex: /\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/g },
      { type: 'function', regex: /^\s*\[.+\]\s*$/gm },
    ],
    memcached: [
      { type: 'comment', regex: /#.*/g },
      { type: 'string', regex: /"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'/g },
      { type: 'keyword', regex: /\b(set|get|delete|add|replace|append|prepend|incr|decr|flush_all|stats|version|quit)\b/gi },
      { type: 'number', regex: /\b\d+\b/g },
      { type: 'operator', regex: /[\r\n]/g },
    ],
  };

  function escapeHtml(text) {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function detectLanguage(code, className) {
    if (className) {
      if (className.includes('language-bash') || className.includes('language-sh')) return 'bash';
      if (className.includes('language-toml')) return 'toml';
      if (className.includes('language-memcached')) return 'memcached';
    }
    if (/\[.+\]/.test(code) && /=/.test(code) && !/^(cargo|git|curl|brew|echo|cat|ls|mkdir|rm|cp|make|sudo|chmod|mv|cd)\b/m.test(code)) return 'toml';
    if (/\b(set|get|delete)\b/i.test(code) && /^[a-z]+\s+/im.test(code)) return 'memcached';
    return 'bash';
  }

  function highlightElement(element) {
    if (!element || element.dataset.highlighted === 'true') return;

    const code = element.textContent;
    const className = element.className || '';
    const language = detectLanguage(code, className);
    const patterns = TOKEN_PATTERNS[language] || TOKEN_PATTERNS.bash;

    const tokens = [];
    const seen = new Set();

    patterns.forEach(function (pattern) {
      const regex = new RegExp(pattern.regex.source, pattern.regex.flags);
      let match;
      while ((match = regex.exec(code)) !== null) {
        const key = match.index + ':' + match[0].length;
        if (!seen.has(key)) {
          seen.add(key);
          tokens.push({
            index: match.index,
            length: match[0].length,
            text: match[0],
            type: pattern.type,
          });
        }
      }
    });

    tokens.sort(function (a, b) {
      if (a.index !== b.index) return a.index - b.index;
      return b.length - a.length;
    });

    const filtered = [];
    let lastEnd = -1;
    tokens.forEach(function (token) {
      if (token.index >= lastEnd) {
        filtered.push(token);
        lastEnd = token.index + token.length;
      }
    });

    let result = '';
    let pos = 0;
    filtered.forEach(function (token) {
      if (token.index > pos) {
        result += escapeHtml(code.substring(pos, token.index));
      }
      result += '<span class="syntax-' + token.type + '">' + escapeHtml(token.text) + '</span>';
      pos = token.index + token.length;
    });
    if (pos < code.length) {
      result += escapeHtml(code.substring(pos));
    }

    element.innerHTML = result || escapeHtml(code);
    element.dataset.highlighted = 'true';
    element.dataset.language = language;
  }

  function highlightAll() {
    const blocks = document.querySelectorAll('pre code');
    blocks.forEach(highlightElement);
  }

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { highlightAll, highlightElement, escapeHtml, detectLanguage };
  } else {
    window.SyntaxHighlight = { highlightAll, highlightElement, escapeHtml, detectLanguage };

    document.addEventListener('DOMContentLoaded', highlightAll);
  }
})();

/**
 * Code Syntax Highlighting Utility.
 * Owner: Scenario 11 - Code Syntax Highlighting
 *
 * Requirements: REQ-11
 *
 * Provides syntax highlighting using Prism.js for shell, rust, and other languages.
 * Uses CSS custom properties for theme-aware highlighting colors.
 */

import Prism from 'prismjs';
import 'prismjs/components/prism-bash';
import 'prismjs/components/prism-rust';
import 'prismjs/components/prism-toml';
import 'prismjs/components/prism-json';

/**
 * Map of language aliases to Prism grammar names.
 */
const languageAliases: Record<string, string> = {
  shell: 'bash',
  sh: 'bash',
  zsh: 'bash',
  console: 'bash',
  terminal: 'bash',
  rs: 'rust',
  ini: 'toml',
  config: 'toml',
};

/**
 * Resolves a language name to its Prism grammar name.
 * @param language - The language identifier
 * @returns The resolved Prism grammar name
 */
function resolveLanguage(language: string): string {
  const normalizedLang = language.toLowerCase().trim();
  return languageAliases[normalizedLang] || normalizedLang;
}

/**
 * Checks if a language is supported by Prism.
 * @param language - The language to check
 * @returns True if the language is supported
 */
export function isLanguageSupported(language: string): boolean {
  const resolved = resolveLanguage(language);
  return resolved in Prism.languages;
}

/**
 * Highlights code with syntax highlighting using Prism.js.
 * Falls back to plain text if language is not supported.
 *
 * @param code - The code string to highlight
 * @param language - The language for highlighting (e.g., 'bash', 'rust', 'toml')
 * @returns HTML string with syntax highlighting spans
 *
 * @example
 * ```typescript
 * const highlighted = highlightCode('cargo install mirdb', 'bash');
 * // Returns: '<span class="token function">cargo</span> install mirdb'
 * ```
 */
export function highlightCode(code: string, language: string): string {
  if (!code || typeof code !== 'string') {
    return '';
  }

  const resolvedLang = resolveLanguage(language);
  const grammar = Prism.languages[resolvedLang];

  if (!grammar) {
    // Return escaped HTML if language is not supported
    return escapeHtml(code);
  }

  try {
    return Prism.highlight(code, grammar, resolvedLang);
  } catch {
    // Fall back to escaped HTML on error
    return escapeHtml(code);
  }
}

/**
 * Escapes HTML special characters to prevent XSS.
 * @param text - The text to escape
 * @returns HTML-escaped text
 */
function escapeHtml(text: string): string {
  const htmlEscapes: Record<string, string> = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  };
  return text.replace(/[&<>"']/g, (char) => htmlEscapes[char]);
}

/**
 * Highlights all code blocks on the page that have a language class.
 * Looks for <code> elements with class="language-*".
 *
 * @param container - Optional container element to scope the highlighting (defaults to document)
 */
export function highlightAllCodeBlocks(container?: HTMLElement | Document): void {
  const root = container || document;
  const codeBlocks = root.querySelectorAll<HTMLElement>('pre > code[class*="language-"]');

  codeBlocks.forEach((codeEl) => {
    // Extract language from class
    const classes = codeEl.className.split(' ');
    const langClass = classes.find((cls) => cls.startsWith('language-'));

    if (langClass) {
      const language = langClass.replace('language-', '');
      const code = codeEl.textContent || '';

      // Only highlight if not already highlighted
      if (!codeEl.classList.contains('prism-highlighted')) {
        codeEl.innerHTML = highlightCode(code, language);
        codeEl.classList.add('prism-highlighted');
      }
    }
  });
}

/**
 * Creates a highlighted code element.
 *
 * @param code - The code string
 * @param language - The language for highlighting
 * @returns A <code> element with highlighted content
 */
export function createHighlightedCodeElement(
  code: string,
  language: string
): HTMLElement {
  const codeEl = document.createElement('code');
  codeEl.className = `language-${language} prism-highlighted`;
  codeEl.innerHTML = highlightCode(code, language);
  return codeEl;
}

/**
 * Initializes syntax highlighting on the page.
 * Should be called after the DOM is ready.
 *
 * This function:
 * 1. Highlights all existing code blocks with language classes
 * 2. Sets up a MutationObserver to highlight dynamically added code blocks
 */
export function initHighlighting(): void {
  // Highlight existing code blocks
  highlightAllCodeBlocks();

  // Set up observer for dynamically added code blocks
  if (typeof MutationObserver !== 'undefined') {
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        mutation.addedNodes.forEach((node) => {
          if (node instanceof HTMLElement) {
            // Check if the node is a code block or contains code blocks
            if (node.matches?.('pre > code[class*="language-"]')) {
              highlightAllCodeBlocks(node.parentElement || document);
            } else if (node.querySelectorAll) {
              const codeBlocks = node.querySelectorAll<HTMLElement>(
                'pre > code[class*="language-"]'
              );
              if (codeBlocks.length > 0) {
                highlightAllCodeBlocks(node);
              }
            }
          }
        });
      });
    });

    observer.observe(document.body, {
      childList: true,
      subtree: true,
    });
  }
}

/**
 * Gets a list of all supported languages.
 * @returns Array of supported language names
 */
export function getSupportedLanguages(): string[] {
  return Object.keys(Prism.languages).filter(
    (lang) => typeof Prism.languages[lang] === 'object'
  );
}

export default {
  highlightCode,
  highlightAllCodeBlocks,
  initHighlighting,
  isLanguageSupported,
  createHighlightedCodeElement,
  getSupportedLanguages,
};

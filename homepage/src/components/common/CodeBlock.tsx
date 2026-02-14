/**
 * Code block component with syntax highlighting.
 * Owner: Scenario 6 - Code Examples and Quick Start
 *
 * Features:
 * - Syntax highlighting using Prism.js
 * - Copy-to-clipboard button
 * - Line numbers (optional)
 * - Shell/terminal syntax support
 */
import React, { useEffect, useRef } from 'react';
import Prism from 'prismjs';
import 'prismjs/components/prism-bash';
import { useCopyToClipboard } from '../../hooks/useCopyToClipboard';

interface CodeBlockProps {
  code: string;
  language: string;
  showLineNumbers?: boolean;
  title?: string;
}

export const CodeBlock: React.FC<CodeBlockProps> = ({
  code,
  language,
  showLineNumbers = false,
  title,
}) => {
  const codeRef = useRef<HTMLElement>(null);
  const { copy, copied } = useCopyToClipboard();

  useEffect(() => {
    if (codeRef.current) {
      Prism.highlightElement(codeRef.current);
    }
  }, [code, language]);

  const handleCopy = () => {
    copy(code);
  };

  const lines = code.split('\n');

  return (
    <div className="relative rounded-lg overflow-hidden bg-gray-900 dark:bg-gray-950">
      {title && (
        <div className="px-4 py-2 bg-gray-800 dark:bg-gray-900 border-b border-gray-700 text-gray-300 text-sm font-medium">
          {title}
        </div>
      )}
      <div className="relative">
        <button
          onClick={handleCopy}
          className="absolute top-3 right-3 px-3 py-1.5 text-sm rounded-md bg-gray-700 hover:bg-gray-600 text-gray-200 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500"
          aria-label={copied ? 'Copied!' : 'Copy code to clipboard'}
        >
          {copied ? 'Copied!' : 'Copy'}
        </button>
        <div className="overflow-x-auto">
          <pre className="p-4 pr-20 text-sm leading-relaxed">
            {showLineNumbers && (
              <span
                className="select-none text-gray-500 mr-4 inline-block text-right"
                style={{ minWidth: `${String(lines.length).length}ch` }}
                aria-hidden="true"
              >
                {lines.map((_, i) => (
                  <span key={i} className="block">
                    {i + 1}
                  </span>
                ))}
              </span>
            )}
            <code
              ref={codeRef}
              className={`language-${language}`}
            >
              {code}
            </code>
          </pre>
        </div>
      </div>
    </div>
  );
};

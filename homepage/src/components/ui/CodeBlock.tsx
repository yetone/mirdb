/**
 * Code Block Component with syntax highlighting and copy button.
 * Owner: Scenario 3 - Quick Start Section with Code Examples
 *
 * Props:
 * - code: string - The code to display
 * - language: string - Programming language for highlighting
 * - title?: string - Optional title for the code block
 *
 * Features:
 * - Syntax highlighting (Prism.js)
 * - Copy-to-clipboard button with feedback
 * - Responsive scrolling for long lines
 */

import { useEffect, useRef } from 'react';
import { Copy, Check } from 'lucide-react';
import Prism from 'prismjs';
import 'prismjs/themes/prism-tomorrow.css';
import 'prismjs/components/prism-bash';
import 'prismjs/components/prism-javascript';
import 'prismjs/components/prism-typescript';
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard';

interface CodeBlockProps {
  code: string;
  language: string;
  title?: string;
}

export function CodeBlock({ code, language, title }: CodeBlockProps) {
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

  return (
    <div className="relative group rounded-lg overflow-hidden bg-gray-900 dark:bg-gray-800">
      {title && (
        <div className="px-4 py-2 bg-gray-800 dark:bg-gray-700 border-b border-gray-700 dark:border-gray-600">
          <span className="text-sm font-medium text-gray-300">{title}</span>
        </div>
      )}
      <div className="relative">
        <pre className={`language-${language} overflow-x-auto p-4 m-0 text-sm`}>
          <code
            ref={codeRef}
            role="code"
            className={`language-${language}`}
          >
            {code}
          </code>
        </pre>
        <button
          onClick={handleCopy}
          className="absolute top-2 right-2 p-2 rounded-md bg-gray-700 hover:bg-gray-600 text-gray-300 hover:text-white transition-colors opacity-0 group-hover:opacity-100 focus:opacity-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
          aria-label={copied ? 'Copied!' : 'Copy code'}
        >
          {copied ? (
            <Check className="w-4 h-4 text-green-400" aria-label="Copied!" />
          ) : (
            <Copy className="w-4 h-4" aria-label="Copy code" />
          )}
        </button>
      </div>
    </div>
  );
}

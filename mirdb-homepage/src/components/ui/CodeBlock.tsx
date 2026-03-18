/**
 * Code Block Component with Syntax Highlighting.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 *
 * Props:
 * - code: string
 * - language: string (e.g., 'rust', 'bash')
 * - filename?: string
 * - showCopyButton?: boolean
 *
 * Features:
 * - Prism.js syntax highlighting
 * - Copy to clipboard button
 * - Optional filename display
 */

import { useEffect, useRef } from 'react';
import Prism from 'prismjs';
import 'prismjs/components/prism-bash';
import { CopyButton } from './CopyButton';

export interface CodeBlockProps {
  code: string;
  language?: string;
  filename?: string;
  showCopyButton?: boolean;
  className?: string;
}

export function CodeBlock({
  code,
  language = 'bash',
  filename,
  showCopyButton = true,
  className = '',
}: CodeBlockProps) {
  const codeRef = useRef<HTMLElement>(null);

  useEffect(() => {
    if (codeRef.current) {
      Prism.highlightElement(codeRef.current);
    }
  }, [code, language]);

  return (
    <div className={`code-block relative rounded-lg overflow-hidden bg-gray-900 ${className}`}>
      {filename && (
        <div className="code-block-header flex items-center justify-between px-4 py-2 bg-gray-800 border-b border-gray-700">
          <span className="text-sm text-gray-400 font-mono">{filename}</span>
          {showCopyButton && (
            <CopyButton text={code} className="opacity-70 hover:opacity-100" />
          )}
        </div>
      )}
      <div className="relative">
        {!filename && showCopyButton && (
          <div className="absolute top-2 right-2 z-10">
            <CopyButton text={code} className="opacity-70 hover:opacity-100" />
          </div>
        )}
        <pre className="p-4 overflow-x-auto text-sm leading-relaxed">
          <code
            ref={codeRef}
            className={`language-${language}`}
            data-testid="code-content"
          >
            {code}
          </code>
        </pre>
      </div>
    </div>
  );
}

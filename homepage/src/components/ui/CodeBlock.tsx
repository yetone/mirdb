/**
 * Code Block Component
 * Owner: Scenario 3 - Quick Start Section
 *
 * Displays code with syntax highlighting and copy button.
 */

import { Copy, Check } from 'lucide-react';
import { useCopyToClipboard } from '../../hooks/useCopyToClipboard';
import type { CodeBlockProps } from '../../types';

const languageClasses: Record<string, string> = {
  bash: 'text-green-400',
  rust: 'text-orange-400',
  json: 'text-yellow-400',
  text: 'text-gray-300',
};

export function CodeBlock({
  code,
  language = 'text',
  showCopyButton = true,
  title,
}: CodeBlockProps) {
  const { copy, copied } = useCopyToClipboard();

  const handleCopy = () => {
    copy(code);
  };

  return (
    <div className="relative rounded-lg bg-gray-900 overflow-hidden" data-testid="code-block">
      {title && (
        <div className="px-4 py-2 bg-gray-800 border-b border-gray-700 text-sm text-gray-400">
          {title}
        </div>
      )}
      <div className="relative">
        <pre className="p-4 overflow-x-auto">
          <code className={`text-sm font-mono ${languageClasses[language] || languageClasses.text}`}>
            {code}
          </code>
        </pre>
        {showCopyButton && (
          <button
            onClick={handleCopy}
            className="absolute top-2 right-2 p-2 rounded-md bg-gray-700 hover:bg-gray-600 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500"
            aria-label={copied ? 'Copied!' : 'Copy to clipboard'}
            data-testid="copy-button"
          >
            {copied ? (
              <Check className="w-4 h-4 text-green-400" data-testid="check-icon" />
            ) : (
              <Copy className="w-4 h-4 text-gray-300" data-testid="copy-icon" />
            )}
          </button>
        )}
      </div>
    </div>
  );
}

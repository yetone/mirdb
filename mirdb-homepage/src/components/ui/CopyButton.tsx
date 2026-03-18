/**
 * Copy to Clipboard Button Component.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 *
 * Props:
 * - text: string - Text to copy
 * - onCopy?: () => void - Callback after copy
 *
 * Features:
 * - Visual feedback on copy
 * - Accessible with aria-label
 */

import { useCopyToClipboard } from '../../hooks/useCopyToClipboard';

export interface CopyButtonProps {
  text: string;
  onCopy?: () => void;
  className?: string;
}

export function CopyButton({ text, onCopy, className = '' }: CopyButtonProps) {
  const { copy, copied } = useCopyToClipboard();

  const handleClick = async () => {
    const success = await copy(text);
    if (success && onCopy) {
      onCopy();
    }
  };

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`copy-button inline-flex items-center justify-center p-2 rounded-md text-sm font-medium transition-colors duration-200 ${
        copied
          ? 'bg-green-500/20 text-green-400'
          : 'bg-white/10 hover:bg-white/20 text-gray-300 hover:text-white'
      } ${className}`}
      aria-label={copied ? 'Copied!' : 'Copy to clipboard'}
      title={copied ? 'Copied!' : 'Copy to clipboard'}
    >
      {copied ? (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <polyline points="20 6 9 17 4 12" />
        </svg>
      ) : (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
        </svg>
      )}
      <span className="sr-only">{copied ? 'Copied!' : 'Copy to clipboard'}</span>
    </button>
  );
}

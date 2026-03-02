/**
 * Copy to Clipboard Button Component.
 * Owner: Scenario 2 - Guest URL Creation Flow
 *
 * Button that copies text to clipboard:
 * - Visual feedback on success ("Copied!")
 * - Uses useCopyToClipboard hook
 * - Accessible button with proper ARIA
 */
import { Copy, Check } from 'lucide-react';
import { useCopyToClipboard } from '../../hooks/useCopyToClipboard';

interface CopyButtonProps {
  text: string;
  className?: string;
  label?: string;
}

export function CopyButton({ text, className = '', label = 'Copy to clipboard' }: CopyButtonProps) {
  const { copy, isCopied, error } = useCopyToClipboard();

  const handleClick = async () => {
    await copy(text);
  };

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`btn btn-sm ${isCopied ? 'btn-success' : 'btn-ghost'} gap-2 ${className}`}
      aria-label={isCopied ? 'Copied!' : label}
      data-testid="copy-button"
    >
      {isCopied ? (
        <>
          <Check className="w-4 h-4" aria-hidden="true" />
          <span>Copied!</span>
        </>
      ) : (
        <>
          <Copy className="w-4 h-4" aria-hidden="true" />
          <span>Copy</span>
        </>
      )}
      {error && (
        <span className="sr-only" role="alert">
          {error}
        </span>
      )}
    </button>
  );
}

/**
 * Command Input sub-component.
 * Owner: Scenario 11 - Interactive Demo and Quick Try
 *
 * Provides the command input field with keyboard shortcuts.
 */

import { useRef, useCallback } from 'react';

interface CommandInputProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
  onClear: () => void;
  disabled?: boolean;
}

export default function CommandInput({
  value,
  onChange,
  onExecute,
  onClear,
  disabled = false,
}: CommandInputProps) {
  const inputRef = useRef<HTMLInputElement>(null);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        onExecute();
      } else if (e.key === 'l' && e.ctrlKey) {
        e.preventDefault();
        onClear();
        inputRef.current?.focus();
      }
    },
    [onExecute, onClear]
  );

  return (
    <div
      className="interactive-demo__input-wrapper"
      data-testid="command-input-wrapper"
    >
      <span className="interactive-demo__input-prompt" aria-hidden="true">
        $
      </span>
      <input
        ref={inputRef}
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Enter a memcached command..."
        disabled={disabled}
        className="interactive-demo__input"
        data-testid="command-input"
        aria-label="Memcached command input"
        aria-describedby="command-input-hint"
      />
      <button
        type="button"
        onClick={onExecute}
        disabled={disabled || !value.trim()}
        className="interactive-demo__execute-button"
        data-testid="execute-button"
      >
        {disabled ? 'Running...' : 'Execute'}
      </button>
      <span id="command-input-hint" className="interactive-demo__input-hint" data-testid="command-input-hint">
        Try: set key value | get key | delete key | flush_all
      </span>
    </div>
  );
}

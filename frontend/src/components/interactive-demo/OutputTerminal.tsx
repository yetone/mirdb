/**
 * Output Terminal sub-component.
 * Owner: Scenario 11 - Interactive Demo and Quick Try
 *
 * Displays command history in a terminal-like format.
 */

import { useRef, useEffect } from 'react';

export interface HistoryEntry {
  id: string;
  type: 'command' | 'response' | 'error' | 'info';
  text: string;
  timestamp: number;
}

interface OutputTerminalProps {
  history: HistoryEntry[];
}

export default function OutputTerminal({ history }: OutputTerminalProps) {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [history]);

  if (history.length === 0) {
    return (
      <div
        className="interactive-demo__terminal interactive-demo__terminal--empty"
        data-testid="output-terminal"
        aria-label="Command output terminal"
        role="log"
        aria-live="polite"
      >
        <div className="interactive-demo__terminal-placeholder" data-testid="terminal-placeholder">
          Type a command and press Enter to see the result...
        </div>
      </div>
    );
  }

  return (
    <div
      ref={scrollRef}
      className="interactive-demo__terminal"
      data-testid="output-terminal"
      aria-label="Command output terminal"
      role="log"
      aria-live="polite"
    >
      {history.map((entry) => (
        <div
          key={entry.id}
          className={`interactive-demo__terminal-line interactive-demo__terminal-line--${entry.type}`}
          data-testid={`terminal-line-${entry.type}`}
        >
          {entry.type === 'command' && (
            <span className="interactive-demo__terminal-prompt">$ </span>
          )}
          <span className="interactive-demo__terminal-text">{entry.text}</span>
        </div>
      ))}
    </div>
  );
}

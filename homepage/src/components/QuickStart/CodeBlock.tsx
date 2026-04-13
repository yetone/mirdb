import type { CodeBlockProps } from '../../types';
import './QuickStart.css';

/**
 * CodeBlock Component
 * Renders code snippets with proper formatting and syntax highlighting.
 */
export function CodeBlock({ code, language = 'bash', title }: CodeBlockProps) {
  return (
    <div className="code-block" data-testid="code-block">
      {title && (
        <div className="code-block-header" data-testid="code-block-title">
          {title}
        </div>
      )}
      <pre className="code-block-pre">
        <code
          className={`code-block-code language-${language}`}
          data-language={language}
        >
          {code}
        </code>
      </pre>
    </div>
  );
}

/**
 * CodeBlock component for displaying syntax-highlighted code.
 * Owner: Scenario 4 - Getting Started Section
 *
 * Displays code with syntax highlighting using CSS classes
 * for different code elements (comments, strings, commands).
 */

interface CodeBlockProps {
  code: string
  language?: string
}

function highlightCode(code: string): string {
  // Apply syntax highlighting using regex patterns
  let highlighted = code
    // Escape HTML first
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    // Highlight comments (lines starting with #)
    .replace(/^(#.*)$/gm, '<span class="text-slate-500">$1</span>')
    // Highlight memcached commands (set, get, delete, etc.)
    .replace(/^(set|get|gets|delete|add|replace|append|prepend|info|major_compaction)\b/gm, '<span class="text-cyan-400 font-semibold">$1</span>')
    // Highlight responses (STORED, DELETED, VALUE, END, etc.)
    .replace(/^(STORED|DELETED|NOT_FOUND|NOT_STORED|EXISTS|END|ERROR|OK)$/gm, '<span class="text-emerald-400">$1</span>')
    // Highlight VALUE response line
    .replace(/^(VALUE)\s+(\S+)\s+(\d+)\s+(\d+)$/gm, '<span class="text-emerald-400">$1</span> <span class="text-yellow-300">$2</span> <span class="text-purple-400">$3</span> <span class="text-purple-400">$4</span>')
    // Highlight cargo/git commands
    .replace(/\b(cargo|git|cd|telnet|nc)\b/g, '<span class="text-pink-400">$1</span>')
    // Highlight flags (--release, etc.)
    .replace(/(--\w+)/g, '<span class="text-orange-400">$1</span>')
    // Highlight Python keywords
    .replace(/\b(import|from|as)\b/g, '<span class="text-pink-400">$1</span>')

  return highlighted
}

export function CodeBlock({ code, language = 'bash' }: CodeBlockProps) {
  const highlightedCode = highlightCode(code)

  return (
    <div className="relative group" data-testid="code-block">
      <pre
        className="bg-slate-950 rounded-lg p-4 overflow-x-auto text-sm font-mono border border-slate-700"
        data-language={language}
        data-highlighted="true"
      >
        <code
          className="text-slate-300"
          dangerouslySetInnerHTML={{ __html: highlightedCode }}
        />
      </pre>
      <button
        className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity bg-slate-700 hover:bg-slate-600 text-slate-300 px-2 py-1 rounded text-xs"
        onClick={() => navigator.clipboard.writeText(code)}
        aria-label="Copy code to clipboard"
      >
        Copy
      </button>
    </div>
  )
}

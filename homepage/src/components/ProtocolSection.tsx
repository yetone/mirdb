import { PROTOCOL_COMMANDS } from '@/lib/constants';
import { cn } from '@/lib/utils';

export default function ProtocolSection() {
  return (
    <section
      id="protocol"
      className="py-20 px-4 sm:px-6 lg:px-8"
      data-testid="protocol-section"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2 className={cn('text-3xl sm:text-4xl font-bold text-[var(--foreground)] mb-4')}>
            Protocol Reference
          </h2>
          <p className="text-lg text-[var(--muted-foreground)] max-w-2xl mx-auto">
            MirDB supports the standard memcached text protocol. Use existing clients without modification.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6" data-testid="protocol-grid">
          {PROTOCOL_COMMANDS.map((cmd) => (
            <div
              key={cmd.name}
              className={cn(
                'p-6 rounded-xl',
                'border border-[var(--border)]',
                'bg-[var(--card)]'
              )}
              data-testid="protocol-card"
            >
              <div className="flex items-center gap-3 mb-3">
                <span className="px-3 py-1 rounded-full bg-brand-500/10 text-brand-500 text-sm font-bold">
                  {cmd.name}
                </span>
              </div>
              <code className="block px-3 py-2 rounded bg-[var(--code-bg)] text-[var(--code-fg)] text-sm font-mono mb-3">
                {cmd.syntax}
              </code>
              <p className="text-[var(--muted-foreground)] mb-3">{cmd.description}</p>
              <pre className="text-sm text-[var(--muted-foreground)] bg-[var(--background)] p-3 rounded border border-[var(--border)]">
                <code>{cmd.example}</code>
              </pre>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

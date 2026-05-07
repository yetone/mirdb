import { CONFIG_OPTIONS } from '@/lib/constants';
import { cn } from '@/lib/utils';

export default function ConfigSection() {
  return (
    <section
      id="config"
      className="py-20 px-4 sm:px-6 lg:px-8"
      data-testid="config-section"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2 className={cn('text-3xl sm:text-4xl font-bold text-[var(--foreground)] mb-4')}>
            Configuration
          </h2>
          <p className="text-lg text-[var(--muted-foreground)] max-w-2xl mx-auto">
            Default configuration options for MirDB server.
          </p>
        </div>

        <div className="overflow-x-auto">
          <table
            className={cn(
              'w-full max-w-4xl mx-auto',
              'border border-[var(--border)] rounded-xl',
              'overflow-hidden'
            )}
            data-testid="config-table"
          >
            <thead className="bg-[var(--card)]">
              <tr>
                <th className="px-6 py-3 text-left text-sm font-semibold text-[var(--foreground)]">Setting</th>
                <th className="px-6 py-3 text-left text-sm font-semibold text-[var(--foreground)]">Default</th>
                <th className="px-6 py-3 text-left text-sm font-semibold text-[var(--foreground)]">Description</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {CONFIG_OPTIONS.map((opt) => (
                <tr key={opt.name} className="hover:bg-[var(--card)]/50">
                  <td className="px-6 py-4 text-sm font-mono text-brand-500">{opt.name}</td>
                  <td className="px-6 py-4 text-sm font-mono text-[var(--muted-foreground)]">{opt.default}</td>
                  <td className="px-6 py-4 text-sm text-[var(--muted-foreground)]">{opt.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

import { cn } from '@/lib/utils';

export default function StatusSection() {
  return (
    <section
      id="status"
      className="py-20 px-4 sm:px-6 lg:px-8"
      data-testid="status-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        <h2 className={cn('text-3xl sm:text-4xl font-bold text-[var(--foreground)] mb-4')}>
          Project Status
        </h2>
        <p className="text-lg text-[var(--muted-foreground)] mb-12">
          Build health and project statistics.
        </p>

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-6" data-testid="status-grid">
          <div className="p-6 rounded-xl border border-[var(--border)] bg-[var(--card)]">
            <div className="text-3xl font-bold text-green-500 mb-2">Passing</div>
            <div className="text-sm text-[var(--muted-foreground)]">CI Build Status</div>
          </div>
          <div className="p-6 rounded-xl border border-[var(--border)] bg-[var(--card)]">
            <div className="text-3xl font-bold text-brand-500 mb-2">Rust</div>
            <div className="text-sm text-[var(--muted-foreground)]">Primary Language</div>
          </div>
          <div className="p-6 rounded-xl border border-[var(--border)] bg-[var(--card)]">
            <div className="text-3xl font-bold text-[var(--foreground)] mb-2">MIT</div>
            <div className="text-sm text-[var(--muted-foreground)]">License</div>
          </div>
        </div>
      </div>
    </section>
  );
}

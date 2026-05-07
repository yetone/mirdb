import { cn } from '@/lib/utils';

export default function Footer() {
  return (
    <footer
      className={cn(
        'py-12 px-4 sm:px-6 lg:px-8',
        'border-t border-[var(--border)]',
        'bg-[var(--card)]'
      )}
      data-testid="footer"
    >
      <div className="max-w-7xl mx-auto">
        <div className="flex flex-col md:flex-row items-center justify-between gap-6">
          <div className="flex items-center gap-2">
            <span className="text-lg font-bold text-brand-500">MirDB</span>
            <span className="text-sm text-[var(--muted-foreground)]">
              A Persistent Key-Value Store
            </span>
          </div>

          <nav className="flex items-center gap-6" aria-label="Footer navigation">
            <a
              href="https://github.com/yetone/mirdb"
              className="text-sm text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors"
            >
              GitHub
            </a>
            <a
              href="#"
              className="text-sm text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors"
            >
              Documentation
            </a>
            <a
              href="#"
              className="text-sm text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors"
            >
              Issues
            </a>
          </nav>

          <p className="text-sm text-[var(--muted-foreground)]">
            &copy; {new Date().getFullYear()} MirDB. Open source under MIT License.
          </p>
        </div>
      </div>
    </footer>
  );
}

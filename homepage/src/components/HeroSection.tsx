import { cn } from '@/lib/utils';

export default function HeroSection() {
  return (
    <section
      id="hero"
      className={cn(
        'relative pt-32 pb-20 md:pt-40 md:pb-28',
        'px-4 sm:px-6 lg:px-8'
      )}
      data-testid="hero-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        <h1
          className={cn(
            'text-4xl sm:text-5xl md:text-6xl lg:text-7xl',
            'font-extrabold tracking-tight',
            'text-[var(--foreground)]',
            'mb-6'
          )}
          data-testid="hero-headline"
        >
          MirDB
        </h1>
        <p
          className={cn(
            'text-xl sm:text-2xl md:text-3xl',
            'font-medium text-[var(--muted-foreground)]',
            'max-w-3xl mx-auto',
            'mb-8'
          )}
          data-testid="hero-tagline"
        >
          A Persistent Key-Value Store with Memcached Protocol Compatibility
        </p>
        <p
          className={cn(
            'text-base sm:text-lg',
            'text-[var(--muted-foreground)]',
            'max-w-2xl mx-auto',
            'leading-relaxed'
          )}
          data-testid="hero-description"
        >
          Built in Rust with LSM-tree architecture for high-performance,
          durable data storage. Drop-in replacement for memcached with
          automatic persistence to disk.
        </p>
      </div>
    </section>
  );
}

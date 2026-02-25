import styles from './TechHighlights.module.css';

interface TechHighlight {
  icon: string;
  title: string;
  description: string;
}

const TECH_HIGHLIGHTS: TechHighlight[] = [
  {
    icon: '🌳',
    title: 'LSM Tree Architecture',
    description:
      'Built on a Log-Structured Merge-tree design for optimal write performance. Data flows through an in-memory MemTable backed by a SkipList, then gets flushed to immutable SSTables with automatic compaction.',
  },
  {
    icon: '🦀',
    title: 'Rust Implementation',
    description:
      'Written in safe Rust for memory safety and zero-cost abstractions. Leverages async/await with Tokio for non-blocking I/O and efficient concurrent request handling.',
  },
  {
    icon: '⚡',
    title: 'Performance Characteristics',
    description:
      'O(1) writes to MemTable, efficient range queries via sorted SSTables, and background compaction to optimize read amplification. Designed for high-throughput workloads.',
  },
];

export function TechHighlights() {
  return (
    <section
      id="architecture"
      className={styles.techHighlights}
      aria-labelledby="tech-highlights-heading"
      role="region"
    >
      <h2 id="tech-highlights-heading" className={styles.heading}>
        Technical Highlights
      </h2>
      <p className={styles.subtitle}>
        Understanding the architecture behind MirDB
      </p>
      <div className={styles.grid}>
        {TECH_HIGHLIGHTS.map((highlight) => (
          <article key={highlight.title} className={styles.card}>
            <span className={styles.icon} aria-hidden="true">
              {highlight.icon}
            </span>
            <h3 className={styles.cardTitle}>{highlight.title}</h3>
            <p className={styles.cardDescription}>{highlight.description}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

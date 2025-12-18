import './FeaturesSection.css'

const MemcachedIcon = () => (
  <svg
    data-testid="feature-icon"
    aria-label="Memcached compatibility icon"
    viewBox="0 0 24 24"
    width="48"
    height="48"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z" />
    <polyline points="22,6 12,13 2,6" />
  </svg>
)

const PersistenceIcon = () => (
  <svg
    data-testid="feature-icon"
    aria-label="Persistent storage icon"
    viewBox="0 0 24 24"
    width="48"
    height="48"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <ellipse cx="12" cy="5" rx="9" ry="3" />
    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
  </svg>
)

const PerformanceIcon = () => (
  <svg
    data-testid="feature-icon"
    aria-label="High performance icon"
    viewBox="0 0 24 24"
    width="48"
    height="48"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
  </svg>
)

const FeatureCard = ({ testId, icon: Icon, title, description }) => (
  <div className="feature-card" data-testid={testId}>
    <div className="feature-icon-wrapper">
      <Icon />
    </div>
    <h3 className="feature-title">{title}</h3>
    <p className="feature-description">{description}</p>
  </div>
)

const FeaturesSection = () => {
  const features = [
    {
      testId: 'feature-card-memcached',
      icon: MemcachedIcon,
      title: 'Memcached Compatible',
      description:
        'Use existing memcached clients seamlessly. Drop-in replacement with full protocol support for GET, SET, DELETE, and more.',
    },
    {
      testId: 'feature-card-persistent',
      icon: PersistenceIcon,
      title: 'Persistent Storage',
      description:
        'Data survives restarts with SSTable storage and write-ahead logging. Never lose your cached data again.',
    },
    {
      testId: 'feature-card-performance',
      icon: PerformanceIcon,
      title: 'High Performance',
      description:
        'Built on LSM tree architecture with async I/O powered by Tokio. Optimized for both reads and writes.',
    },
  ]

  return (
    <section className="features-section" id="features">
      <h2 className="features-heading">Key Features</h2>
      <div className="features-container" data-testid="features-container">
        {features.map((feature) => (
          <FeatureCard key={feature.testId} {...feature} />
        ))}
      </div>
    </section>
  )
}

export default FeaturesSection

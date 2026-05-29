import FeatureCard from './FeatureCard';

export interface FeatureItem {
  icon: string;
  title: string;
  description: string;
}

export const featuresData: FeatureItem[] = [
  {
    icon: '🔗',
    title: 'URL Shortening',
    description:
      'Transform long, complex URLs into short, clean links that are easy to share and remember.',
  },
  {
    icon: '📊',
    title: 'Analytics & Click Tracking',
    description:
      'Track every click with detailed analytics. Monitor engagement, geographic data, and referral sources in real-time.',
  },
  {
    icon: '🔒',
    title: 'Security & Privacy',
    description:
      'Your links are protected with enterprise-grade security. HTTPS encryption and optional password protection keep your data safe.',
  },
  {
    icon: '📁',
    title: 'Easy URL Management',
    description:
      'Organize, edit, and manage all your shortened URLs from a single intuitive dashboard.',
  },
];

export default function FeaturesSection() {
  return (
    <section
      data-testid="features-section"
      aria-label="Features"
      className="features-section py-16 px-4"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          data-testid="features-heading"
          className="features-heading text-3xl font-bold text-center mb-12"
        >
          Why Choose Our URL Shortener?
        </h2>
        <div
          data-testid="features-grid"
          className="features-grid grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
        >
          {featuresData.map((feature, index) => (
            <FeatureCard
              key={index}
              icon={<span role="img" aria-label={feature.title}>{feature.icon}</span>}
              title={feature.title}
              description={feature.description}
            />
          ))}
        </div>
      </div>
    </section>
  );
}

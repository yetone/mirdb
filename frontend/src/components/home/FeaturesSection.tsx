import React from 'react';
import { GlassMorphismCard } from '../common/GlassMorphismCard';

interface FeatureItem {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
}

const LinkIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
  </svg>
);

const ChartIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
  </svg>
);

const ShieldIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
  </svg>
);

const CodeIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
  </svg>
);

const features: FeatureItem[] = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Transform long, unwieldy URLs into short, memorable links that are easy to share and track.',
    icon: <LinkIcon />,
  },
  {
    id: 'click-analytics',
    title: 'Click Analytics',
    description: 'Get detailed analytics on every click including geographic data, device information, and referral sources.',
    icon: <ChartIcon />,
  },
  {
    id: 'secure-management',
    title: 'Secure Management',
    description: 'Manage your links securely with user authentication, access controls, and encrypted data storage.',
    icon: <ShieldIcon />,
  },
  {
    id: 'custom-short-codes',
    title: 'Custom Short Codes',
    description: 'Create branded short codes that reflect your identity and make links more recognizable.',
    icon: <CodeIcon />,
  },
];

export function FeaturesSection() {
  return (
    <section className="py-16 px-4" data-testid="features-section">
      <div className="max-w-6xl mx-auto">
        <h2 className="text-3xl font-bold text-center mb-12">
          Powerful Features for Your Links
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((feature) => (
            <GlassMorphismCard
              key={feature.id}
              testId={`feature-card-${feature.id}`}
            >
              <div className="flex flex-col items-center text-center">
                <div className="text-primary mb-4" data-testid={`feature-icon-${feature.id}`}>
                  {feature.icon}
                </div>
                <h3 className="text-xl font-semibold mb-2" data-testid={`feature-title-${feature.id}`}>
                  {feature.title}
                </h3>
                <p className="text-base-content/70" data-testid={`feature-description-${feature.id}`}>
                  {feature.description}
                </p>
              </div>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  );
}

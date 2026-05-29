import GlassMorphismCard from '../GlassMorphismCard';

export interface FeatureCardProps {
  icon: React.ReactNode;
  title: string;
  description: string;
}

export default function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <GlassMorphismCard
      data-testid="feature-card"
      className="feature-card p-6 flex flex-col items-center text-center gap-4"
    >
      <div data-testid="feature-icon" className="feature-icon text-4xl mb-2">
        {icon}
      </div>
      <h3 data-testid="feature-title" className="feature-title text-xl font-semibold">
        {title}
      </h3>
      <p data-testid="feature-description" className="feature-description text-sm opacity-80">
        {description}
      </p>
    </GlassMorphismCard>
  );
}

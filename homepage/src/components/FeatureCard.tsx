import React from 'react';

export interface FeatureCardProps {
  title: string;
  description: string;
  icon?: React.ReactNode;
  testId?: string;
}

export const FeatureCard: React.FC<FeatureCardProps> = ({
  title,
  description,
  icon,
  testId,
}) => {
  return (
    <div className="feature-card" data-testid={testId}>
      {icon && (
        <div className="feature-icon" data-testid={testId ? `${testId}-icon` : 'feature-icon'}>
          {icon}
        </div>
      )}
      <h3 className="feature-title">{title}</h3>
      <p className="feature-description">{description}</p>
    </div>
  );
};

export default FeatureCard;

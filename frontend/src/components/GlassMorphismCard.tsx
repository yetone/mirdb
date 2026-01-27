import React from 'react';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
}

export const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className = '',
}) => {
  return (
    <div
      className={`backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-2xl shadow-xl ${className}`}
      data-testid="glass-morphism-card"
    >
      {children}
    </div>
  );
};

export default GlassMorphismCard;

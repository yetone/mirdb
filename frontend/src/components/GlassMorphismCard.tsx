import React from 'react';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
  'data-testid'?: string;
}

export const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className = '',
  'data-testid': testId,
}) => {
  return (
    <div
      data-testid={testId}
      className={`
        backdrop-blur-md
        bg-base-100/70
        border border-base-content/10
        rounded-xl
        shadow-xl
        ${className}
      `}
    >
      {children}
    </div>
  );
};

export default GlassMorphismCard;

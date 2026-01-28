import { ReactNode } from 'react';
import { motion } from 'framer-motion';
import { cn } from '../utils';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
  hover?: boolean;
}

export const GlassMorphismCard = ({
  children,
  className,
  hover = true,
}: GlassMorphismCardProps) => {
  const baseStyles = `
    backdrop-blur-md
    bg-base-100/30
    border border-base-content/10
    rounded-xl
    shadow-xl
  `;

  return (
    <motion.div
      data-testid="glass-morphism-card"
      className={cn(baseStyles, className)}
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      whileHover={hover ? { scale: 1.02, boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)' } : undefined}
      transition={{ duration: 0.3 }}
    >
      {children}
    </motion.div>
  );
};

export default GlassMorphismCard;

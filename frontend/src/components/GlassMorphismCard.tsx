import React from 'react';
import { motion } from 'framer-motion';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
  hover?: boolean;
}

export const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className = '',
  hover = true,
}) => {
  return (
    <motion.div
      className={`backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-xl p-6 shadow-lg ${className}`}
      whileHover={hover ? { y: -5, boxShadow: '0 20px 40px rgba(0,0,0,0.1)' } : undefined}
      transition={{ duration: 0.3 }}
    >
      {children}
    </motion.div>
  );
};

export default GlassMorphismCard;

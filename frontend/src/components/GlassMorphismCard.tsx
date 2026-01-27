import { ReactNode } from 'react';
import { motion } from 'framer-motion';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
}

export default function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className={`backdrop-blur-lg bg-base-100/70 border border-base-content/10 rounded-2xl shadow-xl ${className}`}
    >
      {children}
    </motion.div>
  );
}

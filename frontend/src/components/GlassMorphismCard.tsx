import { ReactNode } from 'react';
import { motion } from 'framer-motion';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
}

export default function GlassMorphismCard({
  children,
  className = '',
}: GlassMorphismCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5 }}
      className={`card bg-base-200/50 backdrop-blur-md border border-base-300 shadow-xl ${className}`}
    >
      {children}
    </motion.div>
  );
}

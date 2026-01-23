import React from 'react';
import { motion } from 'framer-motion';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
}

const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({ children, className = '' }) => {
  return (
    <motion.div
      className={`card bg-base-100/70 backdrop-blur-md shadow-xl ${className}`}
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
      viewport={{ once: true }}
    >
      <div className="card-body">{children}</div>
    </motion.div>
  );
};

export default GlassMorphismCard;

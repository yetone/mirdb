import React from 'react';
import { Server, Database, Zap, Layers, ListTree, RefreshCw } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { FEATURES } from '../../utils/constants';

const iconMap: Record<string, LucideIcon> = {
  Server,
  Database,
  Zap,
  Layers,
  ListTree,
  RefreshCw,
};

export default function Features() {
  return (
    <section id="features" aria-label="Key Features" className="py-20 px-4">
      <div className="max-w-6xl mx-auto">
        <h2 className="text-3xl font-bold text-center mb-12 text-gray-900 dark:text-white">
          Key Features
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          {FEATURES.map((feature) => {
            const Icon = iconMap[feature.icon];
            return (
              <div
                key={feature.id}
                className="group rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-6 transition-all duration-300 hover:shadow-lg hover:scale-[1.02] hover:border-brand-400 dark:hover:border-brand-500"
              >
                <div className="w-12 h-12 rounded-lg bg-brand-100 dark:bg-brand-900/30 flex items-center justify-center mb-4 group-hover:bg-brand-200 dark:group-hover:bg-brand-900/50 transition-colors duration-300">
                  {Icon && <Icon className="w-6 h-6 text-brand-600 dark:text-brand-400" aria-hidden="true" />}
                </div>
                <h3 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">
                  {feature.title}
                </h3>
                <p className="text-sm text-gray-600 dark:text-gray-400 leading-relaxed">
                  {feature.description}
                </p>
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
}

import { HTMLAttributes, ReactNode } from 'react';
import { cn } from '../../utils';

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode;
  hover?: boolean;
}

export function Card({ children, hover = false, className, ...props }: CardProps) {
  return (
    <div
      className={cn(
        'bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-6',
        hover && 'transition-all duration-200 hover:shadow-lg hover:-translate-y-1 hover:border-primary-300 dark:hover:border-primary-600',
        className
      )}
      {...props}
    >
      {children}
    </div>
  );
}

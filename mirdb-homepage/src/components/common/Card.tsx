/**
 * Reusable Card Component
 * Owner: Scenario 2 - Features Section Display
 *
 * A flexible card component with icon, title, and description.
 * Features hover effects for interactivity.
 */

import { ReactNode } from 'react';
import './Card.css';

export interface CardProps {
  title: string;
  description: string;
  icon?: ReactNode;
  className?: string;
  testId?: string;
}

export function Card({ title, description, icon, className = '', testId }: CardProps) {
  return (
    <article
      className={`card ${className}`.trim()}
      data-testid={testId}
    >
      {icon && (
        <div className="card__icon" aria-hidden="true">
          {icon}
        </div>
      )}
      <h3 className="card__title">{title}</h3>
      <p className="card__description">{description}</p>
    </article>
  );
}

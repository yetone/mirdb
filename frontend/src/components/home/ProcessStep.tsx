/**
 * Process Step Component.
 * Owner: Scenario 5 - How It Works Section
 *
 * Individual step in the "How It Works" section:
 * - Step number indicator
 * - Icon
 * - Title
 * - Optional description
 *
 * Props: ProcessStepData
 */
import { ProcessStepData } from '../../types/home';

interface ProcessStepProps extends ProcessStepData {
  className?: string;
}

export function ProcessStep({
  stepNumber,
  title,
  description,
  icon,
  className = '',
}: ProcessStepProps) {
  return (
    <div
      className={`process-step flex flex-col items-center text-center ${className}`}
      data-testid={`process-step-${stepNumber}`}
    >
      {/* Step Number Badge */}
      <div
        className="step-number w-8 h-8 rounded-full bg-primary text-primary-content flex items-center justify-center font-bold text-sm mb-4"
        aria-hidden="true"
      >
        {stepNumber}
      </div>

      {/* Icon */}
      <div
        className="step-icon w-16 h-16 rounded-full bg-base-200 flex items-center justify-center mb-4 text-primary"
        data-testid={`step-${stepNumber}-icon`}
        aria-hidden="true"
      >
        {icon}
      </div>

      {/* Title */}
      <h3
        className="step-title text-lg font-semibold mb-2"
        data-testid={`step-${stepNumber}-title`}
      >
        {title}
      </h3>

      {/* Description */}
      {description && (
        <p
          className="step-description text-sm text-base-content/70 max-w-[200px]"
          data-testid={`step-${stepNumber}-description`}
        >
          {description}
        </p>
      )}
    </div>
  );
}

/**
 * Call-to-Action Button Component.
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Expected features:
 * - Primary variant (filled, accent color)
 * - Secondary variant (outlined)
 * - onClick handler or href prop
 * - Accessible button/link semantics
 */

interface CTAButtonProps {
  children: React.ReactNode;
  variant?: 'primary' | 'secondary';
  href?: string;
  onClick?: () => void;
  external?: boolean;
  className?: string;
}

export function CTAButton({
  children,
  variant = 'primary',
  href,
  onClick,
  external = false,
  className = '',
}: CTAButtonProps) {
  const baseClasses =
    'inline-flex items-center justify-center px-6 py-3 font-medium rounded-lg transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-background';

  const variantClasses = {
    primary:
      'bg-accent text-background hover:bg-accent/90 focus:ring-accent',
    secondary:
      'border-2 border-accent text-accent hover:bg-accent/10 focus:ring-accent',
  };

  const combinedClasses = `${baseClasses} ${variantClasses[variant]} ${className}`;

  if (href) {
    return (
      <a
        href={href}
        className={combinedClasses}
        {...(external && {
          target: '_blank',
          rel: 'noopener noreferrer',
        })}
        onClick={onClick}
      >
        {children}
      </a>
    );
  }

  return (
    <button type="button" className={combinedClasses} onClick={onClick}>
      {children}
    </button>
  );
}

export default CTAButton;

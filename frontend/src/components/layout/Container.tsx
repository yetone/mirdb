/**
 * Container component.
 * Owner: Scenario 13 - Responsive Layout
 *
 * Provides a responsive max-width container with consistent padding.
 * Centers content and adapts padding at different breakpoints.
 */

export interface ContainerProps {
  children: React.ReactNode;
  className?: string;
  as?: keyof JSX.IntrinsicElements;
}

export default function Container({
  children,
  className = '',
  as: Component = 'div',
}: ContainerProps) {
  return (
    <Component className={`container ${className}`.trim()} data-testid="layout-container">
      {children}
    </Component>
  );
}

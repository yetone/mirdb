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
  id?: string;
}

export default function Container({
  children,
  className = '',
  as: Component = 'div',
  id,
}: ContainerProps) {
  return (
    <Component className={`container ${className}`.trim()} data-testid="layout-container" id={id}>
      {children}
    </Component>
  );
}

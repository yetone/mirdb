/**
 * Layout component.
 * Owner: Scenario 13 - Responsive Layout
 *
 * Main layout wrapper providing responsive grid/flex layout,
 * mobile navigation, and consistent spacing across viewports.
 */

import Header from './Header';
import Container from './Container';

export interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  return (
    <div className="layout" data-testid="layout-root">
      <Header />
      <Container as="main" className="layout__main">
        {children}
      </Container>
      <footer className="layout-footer" data-testid="layout-footer">
        <Container>
          <p className="layout-footer__text">
            MirDB - Fast, persistent key-value store with Memcached protocol support
          </p>
        </Container>
      </footer>
    </div>
  );
}

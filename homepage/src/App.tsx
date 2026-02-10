import { Layout } from './components/layout/Layout'
import { Hero } from './components/sections/Hero'
import { Features } from './components/sections/Features'
import { UsageExample } from './components/sections/UsageExample'

function App() {
  return (
    <Layout>
      <Hero />
      <Features />
      <UsageExample />
      {/* GettingStarted section will be added by Scenario 4 */}
    </Layout>
  )
}

export default App

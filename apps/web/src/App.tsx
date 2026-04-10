import { useState } from 'react';
import type { ViewKey } from '@/types';
import { AuthProvider, useAuth } from '@/contexts/AuthContext';
import { LoginPage } from '@/components/LoginPage';
import { UserMenu } from '@/components/UserMenu';
import { Overview } from '@/components/Overview';
import { OnboardingWizard } from '@/components/OnboardingWizard';
import { PipelineList } from '@/components/PipelineList';
import { YamlStudio } from '@/components/YamlStudio';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

const NAV_ITEMS: Array<{ key: ViewKey; label: string; eyebrow: string }> = [
  { key: 'overview', label: 'Overview', eyebrow: 'Control plane' },
  { key: 'onboarding', label: 'Onboarding', eyebrow: 'Source → destination' },
  { key: 'jobs', label: 'Job status', eyebrow: 'Operators' },
  { key: 'yaml', label: 'YAML studio', eyebrow: 'Declarative workflows' },
];

function AppShell() {
  const { user, isLoading } = useAuth();
  const [activeView, setActiveView] = useState<ViewKey>('overview');
  const [refreshToken, setRefreshToken] = useState(0);

  if (isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background text-muted-foreground text-sm">
        Loading…
      </div>
    );
  }

  if (!user) {
    return <LoginPage />;
  }

  function handlePipelineApplied() {
    setRefreshToken((n) => n + 1);
    setActiveView('jobs');
  }

  return (
    <div className="flex min-h-screen bg-background text-foreground">
      {/* Sidebar */}
      <aside className="flex w-64 shrink-0 flex-col gap-6 border-r border-border bg-card/40 p-5 backdrop-blur-lg">
        <div>
          <div className="text-2xl font-extrabold tracking-tight">Astra</div>
          <p className="mt-1 text-sm text-muted-foreground">
            Fast data replication, fewer cursed onboarding flows.
          </p>
        </div>

        <nav className="flex flex-col gap-1 flex-1">
          {NAV_ITEMS.map((item) => (
            <Button
              key={item.key}
              variant={item.key === activeView ? 'secondary' : 'ghost'}
              className="h-auto flex-col items-start justify-start gap-0.5 py-3 px-3 text-left"
              onClick={() => setActiveView(item.key)}
            >
              <span className="text-xs uppercase tracking-widest text-muted-foreground">{item.eyebrow}</span>
              <span className="text-sm">{item.label}</span>
            </Button>
          ))}
        </nav>

        <UserMenu />
      </aside>

      {/* Main content */}
      <main className="flex flex-1 flex-col gap-5 overflow-auto p-8">
        {/* Hero header */}
        <header className="flex items-start justify-between gap-4 rounded-lg border border-border bg-card p-5">
          <div>
            <p className="text-xs uppercase tracking-widest text-muted-foreground">v0.1</p>
            <h1 className="mt-1 text-xl font-bold">Astra control plane</h1>
            <p className="mt-1 max-w-xl text-sm text-muted-foreground">
              Self-hostable Postgres replication. Define pipelines in YAML, execute snapshots, observe runs.
            </p>
          </div>
          <div className="flex shrink-0 gap-2">
            <Badge variant="outline">Self-hostable</Badge>
            <Badge variant="muted">Local-dev friendly</Badge>
          </div>
        </header>

        {activeView === 'overview' && <Overview />}
        {activeView === 'onboarding' && <OnboardingWizard onApplied={handlePipelineApplied} />}
        {activeView === 'jobs' && <PipelineList refreshToken={refreshToken} />}
        {activeView === 'yaml' && <YamlStudio />}
      </main>
    </div>
  );
}

export function App() {
  return (
    <AuthProvider>
      <AppShell />
    </AuthProvider>
  );
}

import { useState } from 'react';
import type { WizardState, WizardStep } from '@/types';
import { applySpec } from '@/api';
import { generateWizardYaml } from '@/utils';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { PrefixedInput } from '@/components/ui/prefixed-input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';

const DEFAULT_WIZARD: WizardState = {
  step: 1,
  pipelineName: '',
  source: {
    host: 'localhost',
    port: '5432',
    database: '',
    username: '',
    passwordRef: 'POSTGRES_PASSWORD',
    tables: 'public.users',
  },
  destination: {
    host: 'localhost',
    port: '5432',
    database: '',
    username: '',
    passwordRef: 'DEST_POSTGRES_PASSWORD',
  },
  applyStatus: '',
  applying: false,
};

const STEP_LABELS: Record<WizardStep, string> = {
  1: 'Pipeline',
  2: 'Source',
  3: 'Destination',
  4: 'Review',
};

type Props = { onApplied: () => void };

export function OnboardingWizard({ onApplied }: Props) {
  const [wizard, setWizard] = useState<WizardState>(DEFAULT_WIZARD);

  function setStep(step: WizardStep) {
    setWizard((prev) => ({ ...prev, step }));
  }

  async function handleApply() {
    setWizard((prev) => ({ ...prev, applying: true, applyStatus: 'Applying spec…' }));
    try {
      await applySpec(generateWizardYaml(wizard), 'web-ui');
      setWizard(DEFAULT_WIZARD);
      onApplied();
    } catch (error) {
      setWizard((prev) => ({
        ...prev,
        applying: false,
        applyStatus: error instanceof Error ? `Error: ${error.message}` : 'Failed to apply spec.',
      }));
    }
  }

  return (
    <Card>
      <CardContent className="pt-5 space-y-6">
        <div>
          <p className="text-xs uppercase tracking-widest text-muted-foreground">Source → destination</p>
          <h2 className="text-lg font-semibold mt-1">Set up a new pipeline</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Configure source and destination. Astra generates the spec — you review and apply it.
          </p>
        </div>

        {/* Step indicator */}
        <div className="flex rounded-lg border border-border overflow-hidden">
          {([1, 2, 3, 4] as WizardStep[]).map((n) => {
            const done = wizard.step > n;
            const active = wizard.step === n;
            return (
              <div
                key={n}
                className={[
                  'flex items-center gap-2 px-4 py-3 flex-1 text-sm border-r border-border last:border-r-0',
                  active ? 'bg-primary/10 text-foreground' : done ? 'text-success' : 'text-muted-foreground',
                ].join(' ')}
              >
                <span
                  className={[
                    'flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-xs font-bold',
                    active ? 'bg-primary/30' : done ? 'bg-success/20' : 'bg-muted',
                  ].join(' ')}
                >
                  {done ? '✓' : n}
                </span>
                <span>{STEP_LABELS[n]}</span>
              </div>
            );
          })}
        </div>

        {/* Step 1 — Pipeline */}
        {wizard.step === 1 && (
          <div className="space-y-5">
            <div>
              <h3 className="text-sm font-semibold">Pipeline</h3>
              <p className="text-sm text-muted-foreground mt-0.5">A unique name for this replication pipeline.</p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="wz-name">Pipeline name</Label>
              <Input
                id="wz-name"
                placeholder="e.g. postgres-analytics"
                value={wizard.pipelineName}
                onChange={(e) => setWizard((prev) => ({ ...prev, pipelineName: e.target.value }))}
              />
            </div>
            <div className="space-y-2">
              <Label>Mode</Label>
              <div className="flex items-center gap-3">
                <Badge variant="muted">snapshot</Badge>
                <span className="text-xs text-muted-foreground">Only snapshot mode is supported in v0.1.</span>
              </div>
            </div>
            <div className="flex justify-end">
              <Button disabled={!wizard.pipelineName.trim()} onClick={() => setStep(2)}>
                Next: Source →
              </Button>
            </div>
          </div>
        )}

        {/* Step 2 — Source */}
        {wizard.step === 2 && (
          <div className="space-y-5">
            <div>
              <h3 className="text-sm font-semibold">Source — Postgres</h3>
              <p className="text-sm text-muted-foreground mt-0.5">
                Connection details for the database you want to replicate from.
              </p>
            </div>
            <div className="grid grid-cols-[1fr_7rem] gap-3">
              <div className="space-y-2">
                <Label htmlFor="wz-src-host">Host</Label>
                <Input
                  id="wz-src-host"
                  placeholder="localhost"
                  value={wizard.source.host}
                  onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, host: e.target.value } }))}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="wz-src-port">Port</Label>
                <Input
                  id="wz-src-port"
                  placeholder="5432"
                  value={wizard.source.port}
                  onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, port: e.target.value } }))}
                />
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="wz-src-db">Database</Label>
              <Input
                id="wz-src-db"
                placeholder="app"
                value={wizard.source.database}
                onChange={(e) =>
                  setWizard((prev) => ({ ...prev, source: { ...prev.source, database: e.target.value } }))
                }
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label htmlFor="wz-src-user">Username</Label>
                <Input
                  id="wz-src-user"
                  placeholder="app_user"
                  value={wizard.source.username}
                  onChange={(e) =>
                    setWizard((prev) => ({ ...prev, source: { ...prev.source, username: e.target.value } }))
                  }
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="wz-src-pass">Password env var</Label>
                <PrefixedInput
                  id="wz-src-pass"
                  prefix="env:"
                  placeholder="POSTGRES_PASSWORD"
                  value={wizard.source.passwordRef}
                  onChange={(e) =>
                    setWizard((prev) => ({ ...prev, source: { ...prev.source, passwordRef: e.target.value } }))
                  }
                />
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="wz-src-tables">Tables to replicate</Label>
              <p className="text-xs text-muted-foreground">
                One per line in <code className="font-mono">schema.table</code> format.
              </p>
              <Textarea
                id="wz-src-tables"
                className="font-mono text-xs min-h-[6rem]"
                placeholder={'public.users\npublic.orders'}
                value={wizard.source.tables}
                onChange={(e) =>
                  setWizard((prev) => ({ ...prev, source: { ...prev.source, tables: e.target.value } }))
                }
              />
            </div>
            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(1)}>
                ← Back
              </Button>
              <Button
                disabled={
                  !wizard.source.host.trim() ||
                  !wizard.source.database.trim() ||
                  !wizard.source.username.trim() ||
                  !wizard.source.tables.trim()
                }
                onClick={() => setStep(3)}
              >
                Next: Destination →
              </Button>
            </div>
          </div>
        )}

        {/* Step 3 — Destination */}
        {wizard.step === 3 && (
          <div className="space-y-5">
            <div>
              <h3 className="text-sm font-semibold">Destination — Postgres</h3>
              <p className="text-sm text-muted-foreground mt-0.5">
                Connection details for the target database. Data is loaded into the{' '}
                <code className="font-mono">astra_raw</code> schema.
              </p>
            </div>
            <div className="grid grid-cols-[1fr_7rem] gap-3">
              <div className="space-y-2">
                <Label htmlFor="wz-dst-host">Host</Label>
                <Input
                  id="wz-dst-host"
                  placeholder="localhost"
                  value={wizard.destination.host}
                  onChange={(e) =>
                    setWizard((prev) => ({ ...prev, destination: { ...prev.destination, host: e.target.value } }))
                  }
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="wz-dst-port">Port</Label>
                <Input
                  id="wz-dst-port"
                  placeholder="5432"
                  value={wizard.destination.port}
                  onChange={(e) =>
                    setWizard((prev) => ({ ...prev, destination: { ...prev.destination, port: e.target.value } }))
                  }
                />
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="wz-dst-db">Database</Label>
              <Input
                id="wz-dst-db"
                placeholder="warehouse"
                value={wizard.destination.database}
                onChange={(e) =>
                  setWizard((prev) => ({ ...prev, destination: { ...prev.destination, database: e.target.value } }))
                }
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label htmlFor="wz-dst-user">Username</Label>
                <Input
                  id="wz-dst-user"
                  placeholder="warehouse_user"
                  value={wizard.destination.username}
                  onChange={(e) =>
                    setWizard((prev) => ({ ...prev, destination: { ...prev.destination, username: e.target.value } }))
                  }
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="wz-dst-pass">Password env var</Label>
                <PrefixedInput
                  id="wz-dst-pass"
                  prefix="env:"
                  placeholder="DEST_POSTGRES_PASSWORD"
                  value={wizard.destination.passwordRef}
                  onChange={(e) =>
                    setWizard((prev) => ({
                      ...prev,
                      destination: { ...prev.destination, passwordRef: e.target.value },
                    }))
                  }
                />
              </div>
            </div>
            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(2)}>
                ← Back
              </Button>
              <Button
                disabled={
                  !wizard.destination.host.trim() ||
                  !wizard.destination.database.trim() ||
                  !wizard.destination.username.trim()
                }
                onClick={() => setStep(4)}
              >
                Review spec →
              </Button>
            </div>
          </div>
        )}

        {/* Step 4 — Review */}
        {wizard.step === 4 && (
          <div className="space-y-5">
            <div>
              <h3 className="text-sm font-semibold">Review spec</h3>
              <p className="text-sm text-muted-foreground mt-0.5">Go back to change any field.</p>
            </div>
            <pre className="rounded-md border border-border bg-input p-4 text-xs font-mono text-foreground overflow-auto whitespace-pre-wrap">
              {generateWizardYaml(wizard)}
            </pre>
            {wizard.applyStatus && (
              <p className={wizard.applyStatus.startsWith('Error') ? 'text-sm text-destructive' : 'text-sm text-success'}>
                {wizard.applyStatus}
              </p>
            )}
            <div className="flex justify-between">
              <Button
                variant="outline"
                disabled={wizard.applying}
                onClick={() => setWizard((prev) => ({ ...prev, step: 3, applyStatus: '' }))}
              >
                ← Back
              </Button>
              <div className="flex gap-2">
                <Button variant="ghost" disabled={wizard.applying} onClick={() => setWizard(DEFAULT_WIZARD)}>
                  Start over
                </Button>
                <Button disabled={wizard.applying} onClick={() => void handleApply()}>
                  {wizard.applying ? 'Applying…' : 'Apply spec'}
                </Button>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

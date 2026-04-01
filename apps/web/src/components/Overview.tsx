import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

export function Overview() {
  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
      <Card>
        <CardHeader>
          <p className="text-xs uppercase tracking-widest text-muted-foreground">What it does</p>
          <CardTitle>Postgres replication, honestly</CardTitle>
        </CardHeader>
        <CardContent>
          <ul className="space-y-2 text-sm text-muted-foreground list-disc list-inside">
            <li>YAML spec validated before any network call</li>
            <li>Snapshot execution with resumable chunked staging</li>
            <li>Run history and table-level status wired to real backend state</li>
            <li>Onboarding wizard generates a valid spec — no hand-editing required</li>
          </ul>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <p className="text-xs uppercase tracking-widest text-muted-foreground">v0.1 boundaries</p>
          <CardTitle>Honest about what's stubbed</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            CDC, Python connectors, secrets management beyond env refs, and multi-table parallelism are not yet
            implemented. The UI surfaces what the backend actually does — nothing more.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}

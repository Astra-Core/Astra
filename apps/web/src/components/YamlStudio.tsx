import { useEffect, useState } from 'react';
import { fetchExampleYaml, applySpec } from '@/api';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';

export function YamlStudio() {
  const [yaml, setYaml] = useState('');
  const [yamlStatus, setYamlStatus] = useState('Loading canonical example…');
  const [applyStatus, setApplyStatus] = useState('');

  useEffect(() => {
    fetchExampleYaml()
      .then((text) => {
        setYaml(text);
        setYamlStatus('Canonical example loaded from examples/postgres-to-warehouse.astra.yaml');
      })
      .catch((err: unknown) => {
        setYamlStatus(err instanceof Error ? err.message : 'Failed to load example.');
        setYaml('# Failed to load example YAML\n');
      });
  }, []);

  async function handleApply() {
    setApplyStatus('Applying YAML spec…');
    try {
      const payload = await applySpec(yaml, 'web-ui');
      setApplyStatus(`${payload.message} Saved ${payload.pipeline_name} v${payload.spec_version}.`);
    } catch (error) {
      setApplyStatus(error instanceof Error ? error.message : 'Failed to apply YAML spec.');
    }
  }

  async function handleReset() {
    setApplyStatus('Reloading canonical example…');
    try {
      const text = await fetchExampleYaml();
      setYaml(text);
      setApplyStatus('Canonical example restored.');
    } catch (error) {
      setApplyStatus(error instanceof Error ? error.message : 'Failed to reload example YAML.');
    }
  }

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 items-start">
      <Card>
        <CardHeader>
          <p className="text-xs uppercase tracking-widest text-muted-foreground">YAML editor</p>
          <CardTitle>Spec editor</CardTitle>
          <CardDescription>
            Seeded from the canonical example so the UI stays attached to the real Astra contract.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="yaml-editor">Pipeline spec</Label>
            <Textarea
              id="yaml-editor"
              className="min-h-[28rem] font-mono text-xs"
              value={yaml}
              onChange={(e) => setYaml(e.target.value)}
            />
          </div>
          <div className="flex gap-2">
            <Button onClick={() => void handleApply()}>Apply spec</Button>
            <Button variant="outline" onClick={() => void handleReset()}>
              Reset example
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">{yamlStatus}</p>
          {applyStatus && (
            <p
              className={
                applyStatus.startsWith('Applying') || applyStatus.includes('Saved')
                  ? 'text-sm text-success'
                  : 'text-sm text-destructive'
              }
            >
              {applyStatus}
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <p className="text-xs uppercase tracking-widest text-muted-foreground">Preview</p>
          <CardTitle>What the operator is about to apply</CardTitle>
        </CardHeader>
        <CardContent>
          <pre className="rounded-md border border-border bg-input p-4 text-xs font-mono text-foreground overflow-auto whitespace-pre-wrap min-h-[28rem]">
            {yaml}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}

import { useEffect, useState, type FormEvent } from 'react';
import type { SavedConnectionRequest, SavedConnectionResponse } from '@/types';
import { createConnection, updateConnection } from '@/api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent } from '@/components/ui/card';

const KINDS = ['postgres', 'mysql', 'snowflake'] as const;
type Kind = (typeof KINDS)[number];

const DEFAULT_PORTS: Record<Kind, number> = {
  postgres: 5432,
  mysql: 3306,
  snowflake: 443,
};

interface Props {
  /** When set, the form is in edit mode for this connection. */
  editing?: SavedConnectionResponse;
  onSaved: (conn: SavedConnectionResponse) => void;
  onCancel: () => void;
}

export function ConnectionForm({ editing, onSaved, onCancel }: Props) {
  const [name, setName] = useState('');
  const [kind, setKind] = useState<Kind>('postgres');
  const [host, setHost] = useState('');
  const [port, setPort] = useState(5432);
  const [database, setDatabase] = useState('');
  const [username, setUsername] = useState('');
  const [secretRef, setSecretRef] = useState('');
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);

  // Populate fields when editing an existing connection.
  useEffect(() => {
    if (editing) {
      setName(editing.name);
      setKind((editing.kind as Kind) ?? 'postgres');
      setHost(editing.host);
      setPort(editing.port);
      setDatabase(editing.database);
      setUsername(editing.username);
      setSecretRef(editing.secret_ref ?? '');
    }
  }, [editing]);

  // When kind changes in create mode, update the default port.
  function handleKindChange(k: Kind) {
    setKind(k);
    if (!editing) setPort(DEFAULT_PORTS[k]);
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError('');
    setSaving(true);
    const req: SavedConnectionRequest = {
      name,
      kind,
      host,
      port,
      database,
      username,
      ...(secretRef.trim() ? { secret_ref: secretRef.trim() } : {}),
    };
    try {
      const saved = editing
        ? await updateConnection(editing.name, req)
        : await createConnection(req);
      onSaved(saved);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Save failed.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm p-4">
      <Card className="w-full max-w-lg">
        <CardContent className="pt-6 space-y-4">
          <div>
            <h2 className="text-base font-semibold">
              {editing ? 'Edit connection' : 'New connection'}
            </h2>
            <p className="text-sm text-muted-foreground mt-0.5">
              Saved connections can be referenced by name in pipeline specs.
            </p>
          </div>

          <form onSubmit={(e) => void handleSubmit(e)} className="space-y-4">
            {/* Name */}
            <div className="space-y-2">
              <Label htmlFor="cf-name">Name</Label>
              <Input
                id="cf-name"
                placeholder="my-postgres"
                value={name}
                onChange={(e) => setName(e.target.value)}
                disabled={!!editing}
                required
              />
              <p className="text-xs text-muted-foreground">
                Lowercase letters, digits, and hyphens only (e.g. <code>prod-db</code>).
              </p>
            </div>

            {/* Kind */}
            <div className="space-y-2">
              <Label>Kind</Label>
              <div className="flex gap-2">
                {KINDS.map((k) => (
                  <button
                    key={k}
                    type="button"
                    onClick={() => handleKindChange(k)}
                    className={[
                      'px-3 py-1.5 rounded-md text-xs font-medium border transition-colors',
                      kind === k
                        ? 'bg-primary text-primary-foreground border-primary'
                        : 'bg-background text-muted-foreground border-border hover:border-foreground/40',
                    ].join(' ')}
                  >
                    {k}
                  </button>
                ))}
              </div>
            </div>

            {/* Host + Port */}
            <div className="grid grid-cols-[1fr_7rem] gap-3">
              <div className="space-y-2">
                <Label htmlFor="cf-host">Host</Label>
                <Input
                  id="cf-host"
                  placeholder="localhost"
                  value={host}
                  onChange={(e) => setHost(e.target.value)}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="cf-port">Port</Label>
                <Input
                  id="cf-port"
                  type="number"
                  min={1}
                  max={65535}
                  value={port}
                  onChange={(e) => setPort(Number(e.target.value))}
                  required
                />
              </div>
            </div>

            {/* Database */}
            <div className="space-y-2">
              <Label htmlFor="cf-database">Database</Label>
              <Input
                id="cf-database"
                placeholder="app"
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
                required
              />
            </div>

            {/* Username + Secret Ref */}
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label htmlFor="cf-username">Username</Label>
                <Input
                  id="cf-username"
                  placeholder="app_user"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="cf-secret">Secret ref</Label>
                <Input
                  id="cf-secret"
                  placeholder="MY_DB_PASSWORD"
                  value={secretRef}
                  onChange={(e) => setSecretRef(e.target.value)}
                />
              </div>
            </div>

            {error && <p className="text-sm text-destructive">{error}</p>}

            <div className="flex justify-end gap-2 pt-1">
              <Button type="button" variant="outline" onClick={onCancel} disabled={saving}>
                Cancel
              </Button>
              <Button type="submit" disabled={saving}>
                {saving ? 'Saving…' : editing ? 'Update' : 'Create'}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}

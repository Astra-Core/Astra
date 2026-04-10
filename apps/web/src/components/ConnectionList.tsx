import { useEffect, useState } from 'react';
import type { SavedConnectionResponse } from '@/types';
import { fetchConnections, deleteConnection } from '@/api';
import { ConnectionForm } from '@/components/ConnectionForm';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';

export function ConnectionList() {
  const [connections, setConnections] = useState<SavedConnectionResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [editTarget, setEditTarget] = useState<SavedConnectionResponse | undefined>();

  async function load() {
    setLoading(true);
    setError('');
    try {
      setConnections(await fetchConnections());
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load connections.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
  }, []);

  async function handleDelete(name: string) {
    if (!confirm(`Delete connection "${name}"? This cannot be undone.`)) return;
    try {
      await deleteConnection(name);
      setConnections((prev) => prev.filter((c) => c.name !== name));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Delete failed.');
    }
  }

  function openCreate() {
    setEditTarget(undefined);
    setShowForm(true);
  }

  function openEdit(conn: SavedConnectionResponse) {
    setEditTarget(conn);
    setShowForm(true);
  }

  function handleSaved(conn: SavedConnectionResponse) {
    setConnections((prev) => {
      const idx = prev.findIndex((c) => c.name === conn.name);
      if (idx >= 0) {
        const next = [...prev];
        next[idx] = conn;
        return next;
      }
      return [...prev, conn];
    });
    setShowForm(false);
    setEditTarget(undefined);
  }

  return (
    <>
      {showForm && (
        <ConnectionForm
          editing={editTarget}
          onSaved={handleSaved}
          onCancel={() => {
            setShowForm(false);
            setEditTarget(undefined);
          }}
        />
      )}

      <Card>
        <CardContent className="pt-5 space-y-4">
          <div className="flex items-center justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-widest text-muted-foreground">
                Infrastructure
              </p>
              <h2 className="text-lg font-semibold mt-1">Saved connections</h2>
              <p className="text-sm text-muted-foreground mt-0.5">
                Reusable connection configs referenced by name in pipeline specs.
              </p>
            </div>
            <Button onClick={openCreate} className="shrink-0">
              New connection
            </Button>
          </div>

          {loading && (
            <p className="text-sm text-muted-foreground">Loading connections…</p>
          )}

          {error && <p className="text-sm text-destructive">{error}</p>}

          {!loading && connections.length === 0 && !error && (
            <p className="text-sm text-muted-foreground">
              No connections yet. Click <strong>New connection</strong> to add one.
            </p>
          )}

          {connections.length > 0 && (
            <div className="overflow-x-auto rounded-md border border-border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/30 text-xs uppercase tracking-wider text-muted-foreground">
                    <th className="px-4 py-2 text-left font-medium">Name</th>
                    <th className="px-4 py-2 text-left font-medium">Kind</th>
                    <th className="px-4 py-2 text-left font-medium">Host</th>
                    <th className="px-4 py-2 text-left font-medium">Database</th>
                    <th className="px-4 py-2 text-left font-medium">Username</th>
                    <th className="px-4 py-2 text-left font-medium">Secret ref</th>
                    <th className="px-4 py-2 text-right font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {connections.map((conn) => (
                    <tr
                      key={conn.name}
                      className="border-b border-border last:border-b-0 hover:bg-muted/20 transition-colors"
                    >
                      <td className="px-4 py-3 font-mono text-xs">{conn.name}</td>
                      <td className="px-4 py-3">
                        <Badge variant="outline">{conn.kind}</Badge>
                      </td>
                      <td className="px-4 py-3 text-muted-foreground">
                        {conn.host}:{conn.port}
                      </td>
                      <td className="px-4 py-3 text-muted-foreground">{conn.database}</td>
                      <td className="px-4 py-3 text-muted-foreground">{conn.username}</td>
                      <td className="px-4 py-3 text-muted-foreground">
                        {conn.secret_ref ? (
                          <code className="text-xs">{conn.secret_ref}</code>
                        ) : (
                          <span className="text-muted-foreground/50">—</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <div className="flex justify-end gap-2">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => openEdit(conn)}
                          >
                            Edit
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="text-destructive hover:text-destructive"
                            onClick={() => void handleDelete(conn.name)}
                          >
                            Delete
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </>
  );
}

import { useAuth } from '@/contexts/AuthContext';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';

export function UserMenu() {
  const { user, logout } = useAuth();
  if (!user) return null;

  return (
    <div className="flex flex-col gap-2 border-t border-border pt-4">
      <div className="flex items-center gap-2 min-w-0">
        <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-primary/20 text-xs font-bold uppercase">
          {user.email[0]}
        </div>
        <div className="min-w-0 flex-1">
          <p className="truncate text-xs font-medium">{user.email}</p>
          <Badge variant="muted" className="mt-0.5 text-[10px] px-1.5 py-0">
            {user.auth_source}
          </Badge>
        </div>
      </div>
      <Button
        variant="ghost"
        size="sm"
        className="w-full justify-start text-muted-foreground hover:text-foreground"
        onClick={() => void logout()}
      >
        Sign out
      </Button>
    </div>
  );
}

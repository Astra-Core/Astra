import * as React from 'react';
import { cn } from '@/lib/utils';
import { Input } from '@/components/ui/input';

export interface PrefixedInputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  prefix: string;
}

const PrefixedInput = React.forwardRef<HTMLInputElement, PrefixedInputProps>(
  ({ prefix, className, ...props }, ref) => (
    <div
      className={cn(
        'flex items-center overflow-hidden rounded-md border border-border bg-input focus-within:ring-1 focus-within:ring-ring',
        className
      )}
    >
      <span className="shrink-0 border-r border-border px-3 py-2 font-mono text-xs text-muted-foreground">
        {prefix}
      </span>
      <Input
        ref={ref}
        className="rounded-none border-0 bg-transparent focus-visible:ring-0 focus-visible:ring-offset-0"
        {...props}
      />
    </div>
  )
);
PrefixedInput.displayName = 'PrefixedInput';

export { PrefixedInput };

import { PropsWithChildren } from 'react';

export const PageWrapper = ({ children }: PropsWithChildren) => (
  <div className="space-y-5 p-6">{children}</div>
);

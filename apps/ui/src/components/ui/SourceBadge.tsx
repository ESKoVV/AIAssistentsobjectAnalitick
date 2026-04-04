import { Globe, MessageSquare, Rss, Send } from 'lucide-react';
import { SourceType } from '../../types';
import { sourceLabels } from '../../utils/sourceLabels';

const iconBySource: Record<SourceType, JSX.Element> = {
  vk_post: <MessageSquare size={14} />,
  vk_comment: <MessageSquare size={14} />,
  telegram_post: <Send size={14} />,
  telegram_comment: <Send size={14} />,
  max_post: <MessageSquare size={14} />,
  max_comment: <MessageSquare size={14} />,
  rss_article: <Rss size={14} />,
  portal_appeal: <Globe size={14} />
};

export const SourceBadge = ({ sourceType }: { sourceType: SourceType }) => (
  <span className="inline-flex items-center gap-1 rounded bg-slate-800 px-2 py-1 text-xs">
    {iconBySource[sourceType]} {sourceLabels[sourceType]}
  </span>
);

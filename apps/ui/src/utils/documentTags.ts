import { NormalizedDocument, TopicTag } from '../types';

export const ALL_TAGS: TopicTag[] = ['СВО', 'ЖКХ', 'Транспорт', 'Медицина', 'Образование', 'Безопасность'];

const keywordMap: Record<TopicTag, string[]> = {
  СВО: ['сво', 'мобилизац', 'военн'],
  ЖКХ: ['вода', 'отоплен', 'жкх', 'освещен', 'коммун'],
  Транспорт: ['автобус', 'маршрут', 'транспорт', 'метро', 'дорог'],
  Медицина: ['врач', 'больниц', 'поликлиник', 'медицин'],
  Образование: ['школ', 'университет', 'образован', 'детсад'],
  Безопасность: ['авар', 'безопас', 'чп', 'пожар']
};

export const inferDocumentTags = (doc: NormalizedDocument): TopicTag[] => {
  const text = doc.text.toLowerCase();
  const tags = ALL_TAGS.filter((tag) => keywordMap[tag].some((word) => text.includes(word)));
  return tags.length ? tags : ['ЖКХ'];
};

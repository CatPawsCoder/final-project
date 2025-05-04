# цель -  иметь единый датасет, который объединяет:

# Посты, найденные по ключевым словам через newsfeed.search.

# Посты из заранее заданных депрессивных групп (по ID).


import vk_api
from config import VK_TOKEN
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import csv

# Слова, по которым фильтруем посты (можно расширить)
depression_keywords = [
    'депрессия', 'устал', 'тоска', 'боль', 'одиночество', 'не хочу жить', 'ничего не радует',
    'пустота', 'безысходность', 'подавленность', 'не вижу смысла', 'хандра', 'тревога', 'страх',
    'слезы', 'апатия', 'психотерапия', 'выгорание', 'отчаяние'
]

def contains_keywords(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in depression_keywords)

def to_unix(dt):
    return int(dt.timestamp())

def get_posts_by_search(query, start_time, end_time, count=200):
    vk_session = vk_api.VkApi(token=VK_TOKEN)
    vk = vk_session.get_api()

    try:
        response = vk.newsfeed.search(q=query, count=count, start_time=start_time, end_time=end_time)
        return response.get('items', [])
    except Exception as e:
        print(f"Ошибка VK API при поиске постов: {e}")
        return []

def load_group_ids(input_file="data/depression_groups.csv"):
    group_ids = []
    try:
        with open(input_file, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Пропускаем заголовок
            group_ids = [row[0] for row in reader]
    except Exception as e:
        print(f"Ошибка при загрузке ID групп: {e}")
    return group_ids

def crawl_by_keywords_and_groups(output_csv="data/raw/vk_depression_dataset_full_search.csv", days_back=90, step_days=7):
    today = datetime.now()
    intervals = [
        (today - timedelta(days=i + step_days), today - timedelta(days=i))
        for i in range(0, days_back, step_days)
    ]

    collected = []

    if os.path.exists(output_csv):
        existing_df = pd.read_csv(output_csv)
        existing_texts = set(existing_df['text'])
    else:
        existing_df = None
        existing_texts = set()

    # Загружаем депрессивные группы
    depression_groups = load_group_ids(input_file="data/depression_groups.csv")
    print(f"Найдено {len(depression_groups)} депрессивных групп.")

    # Собираем посты по ключевым словам
    for keyword in depression_keywords:
        print(f"🔍 Ключевое слово: {keyword}")
        for start, end in intervals:
            unix_start = to_unix(start)
            unix_end = to_unix(end)

            print(f"  ⏳ Период: {start.date()} — {end.date()}")
            items = get_posts_by_search(keyword, unix_start, unix_end)

            for item in items:
                text = item.get('text', '').strip()
                if not text or text in existing_texts:
                    continue
                if not contains_keywords(text):
                    continue

                post_data = {
                    'text': text,
                    'date': item.get('date'),
                    'from_id': item.get('from_id'),
                    'post_id': item.get('id'),
                    'likes': item.get('likes', {}).get('count', 0),
                    'comments': item.get('comments', {}).get('count', 0)
                }
                collected.append(post_data)
                existing_texts.add(text)

            time.sleep(1.2)  # пауза чтобы избежать бана

    # Собираем посты из групп
    for group_id in depression_groups:
        print(f"🔍 Собираем посты из группы: {group_id}")
        for start, end in intervals:
            unix_start = to_unix(start)
            unix_end = to_unix(end)

            print(f"  ⏳ Период: {start.date()} — {end.date()}")
            try:
                response = vk_api.VkApi(token=VK_TOKEN).get_api().wall.get(owner_id=-int(group_id), count=200, offset=0)
                items = response.get('items', [])
                for item in items:
                    text = item.get('text', '').strip()
                    if not text or text in existing_texts:
                        continue
                    if not contains_keywords(text):
                        continue

                    post_data = {
                        'text': text,
                        'date': item.get('date'),
                        'from_id': item.get('from_id'),
                        'post_id': item.get('id'),
                        'likes': item.get('likes', {}).get('count', 0),
                        'comments': item.get('comments', {}).get('count', 0)
                    }
                    collected.append(post_data)
                    existing_texts.add(text)

            except Exception as e:
                print(f"Ошибка при сборе постов из группы {group_id}: {e}")
                continue

            time.sleep(1.2)  # пауза чтобы избежать бана

    if collected:
        new_df = pd.DataFrame(collected)
        if existing_df is not None:
            full_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(subset='text')
        else:
            full_df = new_df

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        full_df.to_csv(output_csv, index=False)
        print(f"✅ Добавлено {len(collected)} новых постов. Всего: {len(full_df)}")
    else:
        print("❌ Новых постов не найдено.")

# Пример вызова функции
if __name__ == "__main__":
    crawl_by_keywords_and_groups()







# import vk_api
# from config import VK_TOKEN
# import pandas as pd
# import time


# def get_wall_posts(group_id, total_posts=1000):
#     vk_session = vk_api.VkApi(token=VK_TOKEN)
#     vk = vk_session.get_api()

#     posts = []
#     offset = 0
#     batch_size = 100

#     while len(posts) < total_posts:
#         try:
#             response = vk.wall.get(owner_id=-group_id, count=batch_size, offset=offset)
#             items = response.get('items', [])
#             if not items:
#                 break
#             for item in items:
#                 if 'text' in item:
#                     text = item['text'].strip()
#                     if text and not any(p['text'] == text for p in posts):
#                         # Пытаемся найти фото
#                         image_url = ''
#                         if 'attachments' in item:
#                             for att in item['attachments']:
#                                 if att['type'] == 'photo':
#                                     sizes = att['photo'].get('sizes', [])
#                                     if sizes:
#                                         image_url = sizes[-1].get('url')

#                         post_data = {
#                             'source': f"group_{group_id}",
#                             'text': text,
#                             'date': item.get('date'),
#                             'from_id': item.get('from_id'),
#                             'post_id': item.get('id'),
#                             'likes': item.get('likes', {}).get('count', 0),
#                             'comments': item.get('comments', {}).get('count', 0),
#                             'image_url': image_url
#                         }
#                         posts.append(post_data)
#             offset += batch_size
#             time.sleep(1.5)
#         except Exception as e:
#             print(f"Ошибка при запросе: {e}")
#             break

#     return posts

# if __name__ == "__main__":
#     # ID групп (числовые), можно брать на сайте vk.com/groups или через vk.com/id...
#     group_ids = [
#         203728426,  # psychologytoday
#         55873153,   # psylive
#         213673161,  # vyhoranie
#         192618568,  # samopomosh
#         132863911   # anti-depressant
#     ]

#     all_data = []
#     collected_texts = set()

#     for group_id in group_ids:
#         print(f"Собираем посты из группы: {group_id}")
#         posts = get_wall_posts(group_id, total_posts=500)
#         print(f"Найдено {len(posts)} постов из группы {group_id}")
#         for post in posts:
#             if post['text'] not in collected_texts:
#                 all_data.append(post)
#                 collected_texts.add(post['text'])

#     # Загружаем существующий CSV, если он есть
#     try:
#         old_df = pd.read_csv('data/raw/vk_depression_dataset_full_with_images.csv')
#         new_df = pd.DataFrame(all_data)
#         combined_df = pd.concat([old_df, new_df], ignore_index=True)
#         # Убираем дубли
#         combined_df = combined_df.drop_duplicates(subset='text')
#         combined_df.to_csv('data/raw/vk_depression_dataset_full_with_images.csv', index=False)
#         print(f"Новые данные добавлены. Итоговое количество постов: {len(combined_df)}")
#     except FileNotFoundError:
#         # Если CSV ещё нет — создаём новый
#         new_df = pd.DataFrame(all_data)
#         new_df.to_csv('data/raw/vk_depression_dataset_full_with_images.csv', index=False)
#         print(f"Создан новый CSV. Количество постов: {len(new_df)}")


# #  версия 1 сбор постов через метод — newsfeed.search и поиск по словам.
# # def get_vk_posts(query, total_posts=500):
# #     vk_session = vk_api.VkApi(token=VK_TOKEN)
# #     vk = vk_session.get_api()

# #     posts = []
# #     offset = 0
# #     batch_size = 200

# #     while len(posts) < total_posts:
# #         try:
# #             response = vk.newsfeed.search(q=query, count=batch_size, offset=offset)
# #             items = response.get('items', [])
# #             if not items:
# #                 break
# #             for item in items:
# #                 if 'text' in item and not any(p['text'] == item['text'] for p in posts):
# #                     # Пытаемся найти фото
# #                     image_url = ''
# #                     if 'attachments' in item:
# #                         for att in item['attachments']:
# #                             if att['type'] == 'photo':
# #                                 sizes = att['photo'].get('sizes', [])
# #                                 if sizes:
# #                                     image_url = sizes[-1].get('url')  # Самое большое фото

# #                     post_data = {
# #                         'keyword': query,
# #                         'text': item['text'],
# #                         'date': item.get('date'),
# #                         'from_id': item.get('from_id'),
# #                         'post_id': item.get('id'),
# #                         'owner_id': item.get('owner_id'),
# #                         'likes': item.get('likes', {}).get('count', 0),
# #                         'comments': item.get('comments', {}).get('count', 0),
# #                         'image_url': image_url
# #                     }
# #                     posts.append(post_data)
# #             offset += batch_size
# #             time.sleep(0.5)
# #         except Exception as e:
# #             print(f"Ошибка при запросе: {e}")
# #             break

# #     return posts

# # if __name__ == "__main__":
# #     keywords = [
# #         'депрессия',
# #         'грусть',
# #         'тоска',
# #         'печаль',
# #         'усталость',
# #         'не хочу жить',
# #         'ничего не радует',
# #         'одиночество',
# #         'паника',
# #         'тревога',
# #         'пустота',
# #         'безысходность',
# #         'боль',
# #         'хочу умереть',
# #         'отчаяние',
# #         'подавленность',
# #         'не вижу смысла',
# #         'хандра',
# #         'слёзы',
# #         'страх',
# #         'апатия'
# #     ]

# #     all_data = []

# #     for keyword in keywords:
# #         print(f"Собираем посты по слову: {keyword}")
# #         posts = get_vk_posts(keyword, total_posts=500)
# #         all_data.extend(posts)

# #     df = pd.DataFrame(all_data)
# #     df.to_csv('data/raw/vk_depression_dataset_full_with_images.csv', index=False)
# #     print("Сбор данных завершён. Файл сохранён в data/raw/vk_depression_dataset_full_with_images.csv")

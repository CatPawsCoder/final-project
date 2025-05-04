import vk_api
from config import VK_TOKEN
import pandas as pd
import time


def get_wall_posts(group_id, total_posts=1000):
    vk_session = vk_api.VkApi(token=VK_TOKEN)
    vk = vk_session.get_api()

    posts = []
    offset = 0
    batch_size = 100

    while len(posts) < total_posts:
        try:
            response = vk.wall.get(owner_id=-group_id, count=batch_size, offset=offset)
            items = response.get('items', [])
            if not items:
                break
            for item in items:
                if 'text' in item:
                    text = item['text'].strip()
                    if text and not any(p['text'] == text for p in posts):
                        # Пытаемся найти фото
                        image_url = ''
                        if 'attachments' in item:
                            for att in item['attachments']:
                                if att['type'] == 'photo':
                                    sizes = att['photo'].get('sizes', [])
                                    if sizes:
                                        image_url = sizes[-1].get('url')

                        post_data = {
                            'source': f"group_{group_id}",
                            'text': text,
                            'date': item.get('date'),
                            'from_id': item.get('from_id'),
                            'post_id': item.get('id'),
                            'likes': item.get('likes', {}).get('count', 0),
                            'comments': item.get('comments', {}).get('count', 0),
                            'image_url': image_url
                        }
                        posts.append(post_data)
            offset += batch_size
            time.sleep(1.5)
        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            break

    return posts

if __name__ == "__main__":
    # ID групп (числовые), можно брать на сайте vk.com/groups или через vk.com/id...
    group_ids = [
        203728426,  # psychologytoday
        55873153,   # psylive
        213673161,  # vyhoranie
        192618568,  # samopomosh
        132863911   # anti-depressant
    ]

    all_data = []
    collected_texts = set()

    for group_id in group_ids:
        print(f"Собираем посты из группы: {group_id}")
        posts = get_wall_posts(group_id, total_posts=500)
        print(f"Найдено {len(posts)} постов из группы {group_id}")
        for post in posts:
            if post['text'] not in collected_texts:
                all_data.append(post)
                collected_texts.add(post['text'])

    # Загружаем существующий CSV, если он есть
    try:
        old_df = pd.read_csv('data/raw/vk_depression_dataset_full_with_images.csv')
        new_df = pd.DataFrame(all_data)
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
        # Убираем дубли
        combined_df = combined_df.drop_duplicates(subset='text')
        combined_df.to_csv('data/raw/vk_depression_dataset_full_with_images.csv', index=False)
        print(f"Новые данные добавлены. Итоговое количество постов: {len(combined_df)}")
    except FileNotFoundError:
        # Если CSV ещё нет — создаём новый
        new_df = pd.DataFrame(all_data)
        new_df.to_csv('data/raw/vk_depression_dataset_full_with_images.csv', index=False)
        print(f"Создан новый CSV. Количество постов: {len(new_df)}")


#  версия 1 сбор постов через метод — newsfeed.search и поиск по словам.
# def get_vk_posts(query, total_posts=500):
#     vk_session = vk_api.VkApi(token=VK_TOKEN)
#     vk = vk_session.get_api()

#     posts = []
#     offset = 0
#     batch_size = 200

#     while len(posts) < total_posts:
#         try:
#             response = vk.newsfeed.search(q=query, count=batch_size, offset=offset)
#             items = response.get('items', [])
#             if not items:
#                 break
#             for item in items:
#                 if 'text' in item and not any(p['text'] == item['text'] for p in posts):
#                     # Пытаемся найти фото
#                     image_url = ''
#                     if 'attachments' in item:
#                         for att in item['attachments']:
#                             if att['type'] == 'photo':
#                                 sizes = att['photo'].get('sizes', [])
#                                 if sizes:
#                                     image_url = sizes[-1].get('url')  # Самое большое фото

#                     post_data = {
#                         'keyword': query,
#                         'text': item['text'],
#                         'date': item.get('date'),
#                         'from_id': item.get('from_id'),
#                         'post_id': item.get('id'),
#                         'owner_id': item.get('owner_id'),
#                         'likes': item.get('likes', {}).get('count', 0),
#                         'comments': item.get('comments', {}).get('count', 0),
#                         'image_url': image_url
#                     }
#                     posts.append(post_data)
#             offset += batch_size
#             time.sleep(0.5)
#         except Exception as e:
#             print(f"Ошибка при запросе: {e}")
#             break

#     return posts

# if __name__ == "__main__":
#     keywords = [
#         'депрессия',
#         'грусть',
#         'тоска',
#         'печаль',
#         'усталость',
#         'не хочу жить',
#         'ничего не радует',
#         'одиночество',
#         'паника',
#         'тревога',
#         'пустота',
#         'безысходность',
#         'боль',
#         'хочу умереть',
#         'отчаяние',
#         'подавленность',
#         'не вижу смысла',
#         'хандра',
#         'слёзы',
#         'страх',
#         'апатия'
#     ]

#     all_data = []

#     for keyword in keywords:
#         print(f"Собираем посты по слову: {keyword}")
#         posts = get_vk_posts(keyword, total_posts=500)
#         all_data.extend(posts)

#     df = pd.DataFrame(all_data)
#     df.to_csv('data/raw/vk_depression_dataset_full_with_images.csv', index=False)
#     print("Сбор данных завершён. Файл сохранён в data/raw/vk_depression_dataset_full_with_images.csv")

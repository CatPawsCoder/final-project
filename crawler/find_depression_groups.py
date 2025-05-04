import vk_api
import csv
from config import VK_TOKEN

def get_depression_groups(query="депрессия OR депрессивный OR грусть OR тоска OR печаль OR печально OR грустно OR печальный OR печальная OR грустный OR грустная OR темно OR темнота OR бессмысленность OR страдание", count=100):
    vk_session = vk_api.VkApi(token=VK_TOKEN)
    vk = vk_session.get_api()

    try:
        response = vk.groups.search(q=query, count=count)
        groups = response.get('items', [])
        group_ids = [group['id'] for group in groups]
        return group_ids
    except Exception as e:
        print(f"Ошибка при поиске групп: {e}")
        return []

def save_group_ids(group_ids, output_file="data/depression_groups.csv"):
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["group_id"])  # Заголовок
        for group_id in group_ids:
            writer.writerow([group_id])

if __name__ == "__main__":
    # Ищем депрессивные группы по ключевым словам
    query = "депрессия OR депрессивный OR грусть OR тоска OR печаль OR печально OR грустно OR печальный OR печальная OR грустный OR грустная OR темно OR темнота OR бессмысленность OR страдание"
    group_ids = get_depression_groups(query=query, count=100)  # Здесь можешь указать нужное количество
    if group_ids:
        save_group_ids(group_ids)
        print(f"Найдено {len(group_ids)} групп. Сохранено в файл 'data/depression_groups.csv'")
    else:
        print("Не удалось найти группы.")

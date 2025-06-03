'''
@file: parser.py
@description: Парсер и переводчик CSV для тегов HTML с batching запросами к OpenAI, фильтрацией дублей и пустых значений
@dependencies: pandas, beautifulsoup4, openai, python-dotenv, tqdm
@created: 2025-05-26
@updated: 2025-05-26 - добавлена фильтрация типов строк, асинхронность, кэширование
@updated: 2025-12-20 - реорганизация структуры папок проекта
'''

import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from typing import List, Dict, Set
import argparse
import os
from dotenv import load_dotenv
import openai
import asyncio
from tqdm import tqdm
import logging
import hashlib
import json
import re
import warnings

# Подавляем предупреждение о парсинге XML как HTML
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Структура папок проекта
INPUT_DIR = 'input'
OUTPUT_DIR = 'output'
CACHE_DIR = 'cache'
CACHE_FILE = os.path.join(CACHE_DIR, 'translation_cache.json')

# Создаем папки если их нет
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Константы для фильтрации типов строк
ALLOWED_ROW_TYPES = {
    'catalog-product',
    'wysiwyg.viewer.components.WRichText',
    'document.api.MenuService.Menu'
}

FORBIDDEN_ROW_TYPES = {
    'offline_payment_method',
    'wixui.ImageX',
    'wix.forms.v4.Form', 
    'ribbon',
    'shipping-rule'
}

# Теги для обычного перевода
TAGS = ['b', 'h1', 'h2', 'li', 'ol', 'p', 'ul']

# Специальные теги для меню
MENU_TAGS = ['label']

# Логирование только ошибок
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')

# Загрузка ключа OpenAI
load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

MODEL = 'gpt-4o'
BATCH_SEPARATOR = '\n###TRANSLATE_SEPARATOR###\n'

# Кэш для переводов
translation_cache = {}

LANGUAGE_CODE_TO_NAME = {
    'pl': 'Polish',
    'en': 'English',
    'de': 'German',
    'fr': 'French',
    'es': 'Spanish',
    'it': 'Italian',
    'ru': 'Russian',
    'uk': 'Ukrainian',
    'cs': 'Czech',
    'sk': 'Slovak',
    'lt': 'Lithuanian',
    'lv': 'Latvian',
    'et': 'Estonian',
    'zh': 'Chinese',
    'ja': 'Japanese',
    'ko': 'Korean',
    'tr': 'Turkish',
    'ar': 'Arabic',
    'he': 'Hebrew',
    'pt': 'Portuguese',
    'nl': 'Dutch',
    'sv': 'Swedish',
    'fi': 'Finnish',
    'no': 'Norwegian',
    'da': 'Danish',
    'el': 'Greek',
    'hu': 'Hungarian',
    'ro': 'Romanian',
    'bg': 'Bulgarian',
    'hr': 'Croatian',
    'sr': 'Serbian',
    'sl': 'Slovenian',
    'th': 'Thai',
    'vi': 'Vietnamese',
    'id': 'Indonesian',
    'ms': 'Malay',
    'hi': 'Hindi',
    'bn': 'Bengali',
    'fa': 'Persian',
    'ur': 'Urdu',
    'ta': 'Tamil',
    'te': 'Telugu',
    'ml': 'Malayalam',
    'kn': 'Kannada',
    'mr': 'Marathi',
    'gu': 'Gujarati',
    'pa': 'Punjabi',
    'sw': 'Swahili',
    'zu': 'Zulu',
    'af': 'Afrikaans',
    'am': 'Amharic',
    'yo': 'Yoruba',
    'ig': 'Igbo',
    'om': 'Oromo',
    'so': 'Somali',
    'ha': 'Hausa',
    'st': 'Southern Sotho',
    'tn': 'Tswana',
    'xh': 'Xhosa',
    'ts': 'Tsonga',
    'ss': 'Swati',
    've': 'Venda',
    'nr': 'South Ndebele',
    'rw': 'Kinyarwanda',
    'ln': 'Lingala',
    'kg': 'Kongo',
    'lu': 'Luba-Katanga',
    'ny': 'Chichewa',
    'mg': 'Malagasy',
    'mt': 'Maltese',
    'ga': 'Irish',
    'cy': 'Welsh',
    'gd': 'Scottish Gaelic',
    'kw': 'Cornish',
    'gv': 'Manx',
    'br': 'Breton',
    'co': 'Corsican',
    'eu': 'Basque',
    'gl': 'Galician',
    'oc': 'Occitan',
    'ca': 'Catalan',
    'lb': 'Luxembourgish',
    'fo': 'Faroese',
    'is': 'Icelandic',
    'sm': 'Samoan',
    'to': 'Tongan',
    'fj': 'Fijian',
    'mi': 'Maori',
    'ty': 'Tahitian',
    'qu': 'Quechua',
    'ay': 'Aymara',
    'gn': 'Guarani',
    'tt': 'Tatar',
    'ba': 'Bashkir',
    'cv': 'Chuvash',
    'ce': 'Chechen',
    'cu': 'Church Slavic',
    'kv': 'Komi',
    'os': 'Ossetic',
    'sah': 'Yakut',
    'udm': 'Udmurt',
    'mhr': 'Meadow Mari',
    'myv': 'Erzya',
    'mdf': 'Moksha',
    'chm': 'Mari',
    'koi': 'Komi-Permyak',
    'kvk': 'Komi-Zyrian',
    'nog': 'Nogai',
    'kum': 'Kumyk',
    'av': 'Avar',
    'dar': 'Dargwa',
    'lez': 'Lezgian',
    'tab': 'Tabasaran',
    'lbe': 'Lak',
    'khv': 'Khvarshi',
    'inh': 'Ingush',
    'ab': 'Abkhaz',
    'ady': 'Adyghe',
    'kbd': 'Kabardian',
    'udm': 'Udmurt',
    'mns': 'Mansi',
    'sel': 'Selkup',
    'kca': 'Khanty',
    'sjo': 'Xibe',
    'evn': 'Evenki',
    'evk': 'Evenki',
    'mnc': 'Manchu',
    'gld': 'Nanai',
    'niv': 'Nivkh',
    'yuk': 'Yukaghir',
    'ckt': 'Chukchi',
    'kpy': 'Koryak',
    'itk': 'Itelmen',
    'enq': 'Enets',
    'nim': 'Nimadi',
    'krc': 'Karachay-Balkar',
    'tyv': 'Tuvan',
    'alt': 'Altai',
    'xal': 'Kalmyk',
    'tut': 'Altaic',
    'tkl': 'Tokelauan',
    'tvl': 'Tuvaluan',
    'wls': 'Wallisian',
    'niu': 'Niuean',
    'rar': 'Rarotongan',
    'pih': 'Pitcairn-Norfolk',
    'tpi': 'Tok Pisin',
    'bi': 'Bislama',
    'ho': 'Hiri Motu',
    'sm': 'Samoan',
    'to': 'Tongan',
    'fj': 'Fijian',
    'mi': 'Maori',
    'ty': 'Tahitian',
    'qu': 'Quechua',
    'ay': 'Aymara',
    'gn': 'Guarani',
    'tt': 'Tatar',
    'ba': 'Bashkir',
    'cv': 'Chuvash',
    'ce': 'Chechen',
    'cu': 'Church Slavic',
    'kv': 'Komi',
    'os': 'Ossetic',
    'sah': 'Yakut',
    'udm': 'Udmurt',
    'mhr': 'Meadow Mari',
    'myv': 'Erzya',
    'mdf': 'Moksha',
    'chm': 'Mari',
    'koi': 'Komi-Permyak',
    'kvk': 'Komi-Zyrian',
    'nog': 'Nogai',
    'kum': 'Kumyk',
    'av': 'Avar',
    'dar': 'Dargwa',
    'lez': 'Lezgian',
    'tab': 'Tabasaran',
    'lbe': 'Lak',
    'khv': 'Khvarshi',
    'inh': 'Ingush',
    'ab': 'Abkhaz',
    'ady': 'Adyghe',
    'kbd': 'Kabardian',
    'udm': 'Udmurt',
    'mns': 'Mansi',
    'sel': 'Selkup',
    'kca': 'Khanty',
    'sjo': 'Xibe',
    'evn': 'Evenki',
    'evk': 'Evenki',
    'mnc': 'Manchu',
    'gld': 'Nanai',
    'niv': 'Nivkh',
    'yuk': 'Yukaghir',
    'ckt': 'Chukchi',
    'kpy': 'Koryak',
    'itk': 'Itelmen',
    'enq': 'Enets',
    'nim': 'Nimadi',
}

def extract_tagged_text(html: str, tags: List[str] = TAGS) -> Dict[str, List[str]]:
    """
    Извлекает текст из указанных тегов в HTML-строке или XML.
    Возвращает словарь: тег -> список текстов.
    """
    result = {}
    
    # Для каждого тега ищем тексты
    for tag in tags:
        if tag == 'label':
            # Для label тегов используем регулярные выражения для XML
            import re
            pattern = rf'<{tag}[^>]*>(.*?)</{tag}>'
            matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
            result[tag] = [match.strip() for match in matches if match.strip()]
        else:
            # Для HTML тегов используем BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            elements = soup.find_all(tag)
            result[tag] = [el.get_text(strip=True) for el in elements if el.get_text(strip=True)]
    
    return result


def replace_tagged_text(html: str, translations: Dict[str, List[str]], tags: List[str] = TAGS) -> str:
    """
    Заменяет текст в указанных тегах HTML или XML переводами.
    """
    content = html
    
    for tag in tags:
        if tag in translations:
            if tag == 'label':
                # Для label тегов используем регулярные выражения для XML
                import re
                pattern = rf'<{tag}[^>]*>(.*?)</{tag}>'
                matches = list(re.finditer(pattern, content, re.DOTALL | re.IGNORECASE))
                
                # Заменяем в обратном порядке, чтобы не сбить позиции
                for i, match in enumerate(reversed(matches)):
                    if i < len(translations[tag]):
                        # Правильный индекс для получения перевода
                        translation_index = len(matches) - 1 - i
                        if translation_index < len(translations[tag]):
                            # Заменяем содержимое между тегами
                            start_pos = match.start()
                            end_pos = match.end()
                            opening_tag = match.group(0)[:match.start(1) - match.start()]
                            closing_tag = match.group(0)[match.end(1) - match.start():]
                            new_content = opening_tag + translations[tag][translation_index] + closing_tag
                            content = content[:start_pos] + new_content + content[end_pos:]
            else:
                # Для HTML тегов используем BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')
                elements = soup.find_all(tag)
                for i, el in enumerate(elements):
                    if i < len(translations[tag]):
                        el.string = translations[tag][i]
                content = str(soup)
    
    return content


def get_lang_columns(columns: List[str]):
    """Определяет исходную колонку и список целевых колонок на основе заголовков"""
    source_cols = [col for col in columns if 'source' in col.lower() and 'language' in col.lower()]
    target_cols = [col for col in columns if 'target' in col.lower() and 'language' in col.lower()]
    
    if not source_cols:
        raise ValueError("Не найдена исходная колонка (должна содержать 'source' и 'language')")
    
    return source_cols[0], target_cols


async def openai_translate_batch_async(texts: List[str], src_lang: str, tgt_lang: str) -> List[str]:
    """Асинхронная функция перевода списка текстов одним батчем с кэшированием"""
    # Проверяем кэш для каждого текста
    cached_results = []
    texts_to_translate = []
    text_indices = []
    
    for i, text in enumerate(texts):
        if not text or not text.strip():
            cached_results.append('')
            continue
            
        cache_key = get_cache_key(text, src_lang, tgt_lang)
        if cache_key in translation_cache:
            cached_results.append(translation_cache[cache_key])
        else:
            cached_results.append(None)
            texts_to_translate.append(text)
            text_indices.append(i)
    
    # Если все тексты закэшированы, возвращаем результат
    if not texts_to_translate:
        return [result if result is not None else '' for result in cached_results]
    
    # Убираем дублирующиеся тексты для экономии API вызовов
    unique_texts = list(dict.fromkeys(texts_to_translate))
    
    if not unique_texts:
        return ['' for _ in texts]
    
    try:
        # Используем async context manager для автоматического управления ресурсами
        async with openai.AsyncOpenAI(api_key=OPENAI_API_KEY) as client:
            batch_text = BATCH_SEPARATOR.join(unique_texts)
            
            prompt = f"""Переведи следующие тексты с {src_lang} на {tgt_lang}. 
Сохрани HTML теги и структуру точно как в оригинале. 
Переводи только содержимое тегов, не сами теги.
Пронумеруй переводы от 1 до {len(unique_texts)}.

{chr(10).join(f"{i+1}. {text}" for i, text in enumerate(unique_texts))}"""

            response = await client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "Ты переводчик HTML контента. Сохраняй все HTML теги и структуру."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            
            translated_batch = response.choices[0].message.content.strip()
            
            # Парсим нумерованный список переводов
            translated_list = []
            import re
            lines = translated_batch.split('\n')
            current_translation = ""
            
            for line in lines:
                # Ищем строки, начинающиеся с номера и точки
                match = re.match(r'^\d+\.\s*(.*)$', line)
                if match:
                    # Если это новый номер и у нас есть предыдущий перевод, сохраняем его
                    if current_translation:
                        translated_list.append(current_translation.strip())
                    # Начинаем новый перевод
                    current_translation = match.group(1)
                else:
                    # Продолжаем текущий перевод
                    if current_translation:
                        current_translation += '\n' + line
            
            # Добавляем последний перевод
            if current_translation:
                translated_list.append(current_translation.strip())
            
            # Если парсинг не сработал, используем старый метод с разделителем как fallback
            if len(translated_list) != len(unique_texts):
                translated_list = translated_batch.split(BATCH_SEPARATOR)
            
            # Кэшируем результаты
            for original, translated in zip(unique_texts, translated_list):
                cache_key = get_cache_key(original, src_lang, tgt_lang)
                translation_cache[cache_key] = translated.strip()
            
            # Создаем словарь переводов для быстрого поиска
            translation_dict = {original: translated.strip() for original, translated in zip(unique_texts, translated_list)}
            
            # Собираем финальный результат
            final_results = []
            translate_index = 0
            
            for i, cached_result in enumerate(cached_results):
                if cached_result is not None:
                    final_results.append(cached_result)
                else:
                    original_text = texts_to_translate[translate_index]
                    final_results.append(translation_dict.get(original_text, original_text))
                    translate_index += 1
            
            return final_results
        
    except Exception as e:
        logging.error(f"Ошибка OpenAI API: {e}")
        return texts  # Возвращаем оригинальные тексты при ошибке

# Оставляем старую синхронную функцию для обратной совместимости
def openai_translate_batch(texts: List[str], src_lang: str, tgt_lang: str) -> List[str]:
    """Синхронная обертка для асинхронной функции перевода"""
    return asyncio.run(openai_translate_batch_async(texts, src_lang, tgt_lang))


def translate_plain_text(text: str, src_lang: str, tgt_lang: str) -> str:
    """Переводит обычный текст без HTML-тегов"""
    if not text or not text.strip():
        return text
    
    prompt = f"Переведи следующий текст с {src_lang} на {tgt_lang}. Сохрани стиль и смысл:\n\n{text}"
    try:
        response = openai.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1024,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return text


def process_row(row, src_col, tgt_col, src_lang, tgt_lang):
    """Обрабатывает одну строку CSV с учетом типа строки"""
    # Проверяем, можно ли переводить эту строку
    if not is_row_translatable(row):
        # Если строка не подлежит переводу, возвращаем исходное значение
        return row[src_col] if pd.notna(row[src_col]) else ''
    
    # Получаем теги для данного типа строки
    tags_to_process = get_tags_for_row_type(row)
    
    if pd.isna(row[src_col]) or not row[src_col]:
        return ''
    
    original_content = str(row[src_col])
    
    # Извлекаем теги для перевода
    tagged_texts = extract_tagged_text(original_content, tags_to_process)
    
    # Собираем все тексты для батчевого перевода
    all_texts = []
    for tag_name, texts in tagged_texts.items():
        all_texts.extend(texts)
    
    if not all_texts:
        # Если нет тегов, проверяем, является ли это простым текстом
        soup = BeautifulSoup(original_content, 'html.parser')
        if not soup.find() and original_content.strip():
            # Это простой текст без тегов
            return translate_plain_text(original_content, src_lang, tgt_lang)
        return original_content
    
    # Переводим все тексты одним батчем
    translated_texts = openai_translate_batch(all_texts, src_lang, tgt_lang)
    
    # Группируем переводы обратно по тегам
    translated_by_tag = {}
    text_index = 0
    for tag_name, texts in tagged_texts.items():
        translated_by_tag[tag_name] = translated_texts[text_index:text_index + len(texts)]
        text_index += len(texts)
    
    # Заменяем тексты в HTML
    return replace_tagged_text(original_content, translated_by_tag, tags_to_process)


def add_target_language_column(df):
    if 'SOURCE_LANGUAGE' in df.columns:
        df['TARGET_LANGUAGE'] = df['SOURCE_LANGUAGE'].map(lambda code: LANGUAGE_CODE_TO_NAME.get(str(code).lower(), 'Unknown'))
    return df


def main(csv_path: str, output_path: str, limit: int = None):
    """Основная функция обработки CSV файла"""
    print(f'Загрузка кэша переводов...')
    load_cache_from_file()
    
    print(f'Чтение файла {csv_path}...')
    df = pd.read_csv(csv_path)
    
    if limit:
        df = df.head(limit)
        print(f'Ограничиваем обработку {limit} строками для тестирования')
    
    print(f'Найдено {len(df)} строк')
    
    # Определяем языковые колонки  
    src_col, tgt_cols = get_lang_columns(df.columns.tolist())
    print(f'Исходная колонка: {src_col}')
    print(f'Целевые колонки: {tgt_cols}')
    
    # Определяем исходный язык из названия колонки
    # Ищем код языка в скобках, например "Source Language (EN)"
    src_match = re.search(r'\(([A-Za-z]{2,3})\)', src_col)
    src_lang_code = src_match.group(1).lower() if src_match else 'en'
    src_lang = LANGUAGE_CODE_TO_NAME.get(src_lang_code, src_lang_code.title())
    
    # Добавляем колонку с названием исходного языка
    add_target_language_column(df)
    
    # Фильтруем строки, которые можно переводить
    translatable_mask = df.apply(is_row_translatable, axis=1)
    translatable_count = translatable_mask.sum()
    total_count = len(df)
    
    print(f'Строк для перевода: {translatable_count} из {total_count}')
    
    # Обрабатываем каждую целевую колонку
    for tgt_col in tgt_cols:
        # Определяем целевой язык из названия колонки
        tgt_match = re.search(r'\(([A-Za-z]{2,3})\)', tgt_col)
        tgt_lang_code = tgt_match.group(1).lower() if tgt_match else 'en'
        tgt_lang = LANGUAGE_CODE_TO_NAME.get(tgt_lang_code, tgt_lang_code.title())
        
        print(f'Переводим с {src_lang} на {tgt_lang}...')
        
        # Инициализируем целевую колонку
        df[tgt_col] = ''
        
        # Обрабатываем только переводимые строки
        translatable_rows = df[translatable_mask]
        
        # Используем tqdm для отображения прогресса
        with tqdm(total=len(translatable_rows), desc=f'Перевод на {tgt_lang}') as pbar:
            for idx, row in translatable_rows.iterrows():
                try:
                    translated_content = process_row(row, src_col, tgt_col, src_lang, tgt_lang)
                    df.at[idx, tgt_col] = translated_content
                except Exception as e:
                    logging.error(f"Ошибка обработки строки {idx}: {e}")
                    df.at[idx, tgt_col] = row[src_col] if pd.notna(row[src_col]) else ''
                pbar.update(1)
    
    print(f'Сохранение кэша переводов...')
    save_cache_to_file()
    
    print(f'Сохранение результата в {output_path}...')
    df.to_csv(output_path, index=False)
    print(f'Готово! Результат записан в {output_path}')
    print(f'Использовано переводов из кэша: {len(translation_cache)}')


def get_cache_key(text: str, src_lang: str, tgt_lang: str) -> str:
    """Генерирует ключ кэша для перевода"""
    content = f"{text}|{src_lang}|{tgt_lang}"
    return hashlib.md5(content.encode()).hexdigest()

def save_cache_to_file(cache_file: str = CACHE_FILE):
    """Сохраняет кэш в файл"""
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(translation_cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Ошибка сохранения кэша: {e}")

def load_cache_from_file(cache_file: str = CACHE_FILE):
    """Загружает кэш из файла"""
    global translation_cache
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                translation_cache = json.load(f)
    except Exception as e:
        logging.error(f"Ошибка загрузки кэша: {e}")

def is_row_translatable(row) -> bool:
    """Проверяет, можно ли переводить строку на основе её типа"""
    # Проверяем наличие колонки с типом элемента
    type_columns = ['Element type', 'element_type', 'type', 'Type', 'row_type', 'Row_Type']
    row_type = None
    
    for col in type_columns:
        if col in row.index and pd.notna(row[col]):
            row_type = str(row[col]).strip()
            break
    
    if not row_type:
        # Если тип не найден, считаем строку переводимой
        return True
    
    # Запрещенные типы
    if row_type in FORBIDDEN_ROW_TYPES:
        return False
    
    # Разрешенные типы
    if row_type in ALLOWED_ROW_TYPES:
        return True
    
    # Для остальных типов - не переводим
    return False

def get_tags_for_row_type(row) -> List[str]:
    """Возвращает список тегов для перевода в зависимости от типа строки"""
    type_columns = ['Element type', 'element_type', 'type', 'Type', 'row_type', 'Row_Type']
    row_type = None
    
    for col in type_columns:
        if col in row.index and pd.notna(row[col]):
            row_type = str(row[col]).strip()
            break
    
    if row_type == 'document.api.MenuService.Menu':
        return MENU_TAGS
    else:
        return TAGS

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Парсер и переводчик Wix CSV (batch)')
    parser.add_argument('input_file', type=str, help='Путь к исходному CSV файлу (относительно папки input/ или абсолютный путь)')
    parser.add_argument('--output', '-o', type=str, default=None, help='Путь к выходному CSV файлу (по умолчанию: сохраняется в папку output/)')
    parser.add_argument('--limit', type=int, default=None, help='Ограничить количество обрабатываемых строк для тестирования')
    args = parser.parse_args()
    
    # Обработка входного файла
    input_file = args.input_file
    if not os.path.isabs(input_file) and not os.path.exists(input_file):
        # Проверяем в папке input/
        input_in_dir = os.path.join(INPUT_DIR, input_file)
        if os.path.exists(input_in_dir):
            input_file = input_in_dir
        else:
            print(f"Файл не найден: {input_file}")
            print(f"Проверяемые пути:")
            print(f"  - {input_file}")
            print(f"  - {input_in_dir}")
            exit(1)
    
    # Обработка выходного файла
    if args.output is None:
        # Создаем имя выходного файла автоматически
        input_basename = os.path.basename(input_file)
        if input_basename.endswith('.csv'):
            output_basename = input_basename[:-4] + '_translated.csv'
        else:
            output_basename = input_basename + '_translated.csv'
        output_file = os.path.join(OUTPUT_DIR, output_basename)
    else:
        output_file = args.output
        if not os.path.isabs(output_file):
            # Если относительный путь, сохраняем в папку output/
            output_file = os.path.join(OUTPUT_DIR, output_file)
    
    main(input_file, output_file, args.limit) 
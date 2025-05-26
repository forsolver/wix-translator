'''
@file: parser.py
@description: Парсер и переводчик CSV для тегов HTML с batching запросами к OpenAI, фильтрацией дублей и пустых значений
@dependencies: pandas, beautifulsoup4, openai, python-dotenv, tqdm
@created: 2025-05-26
'''

import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict
import argparse
import os
from dotenv import load_dotenv
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging

TAGS = ['h', 'p', 'b', 'li', 'ul']

# Логирование только ошибок
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')

# Загрузка ключа OpenAI
load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

MODEL = 'gpt-4o'
BATCH_SEPARATOR = '\n---\n'


def extract_tagged_text(html: str, tags: List[str] = TAGS) -> Dict[str, List[str]]:
    """
    Извлекает текст из указанных тегов в HTML-строке.
    Возвращает словарь: тег -> список текстов.
    """
    soup = BeautifulSoup(html, 'html.parser')
    result = {tag: [el.get_text(strip=True) for el in soup.find_all(tag)] for tag in tags}
    return result


def replace_tagged_text(html: str, translations: Dict[str, List[str]], tags: List[str] = TAGS) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    for tag in tags:
        elements = soup.find_all(tag)
        for i, el in enumerate(elements):
            if tag in translations and i < len(translations[tag]):
                el.string = translations[tag][i]
    return str(soup)


def get_lang_columns(columns: List[str]):
    src_col = next((c for c in columns if c.lower().startswith('source language')), None)
    tgt_col = next((c for c in columns if c.lower().startswith('target language')), None)
    src_lang = src_col.split('(')[-1].replace(')', '').strip() if src_col else None
    tgt_lang = tgt_col.split('(')[-1].replace(')', '').strip() if tgt_col else None
    return src_col, tgt_col, src_lang, tgt_lang


def openai_translate_batch(texts: List[str], src_lang: str, tgt_lang: str) -> List[str]:
    # Убираем пустые и дублирующиеся тексты
    filtered = [(i, t) for i, t in enumerate(texts) if t and t.strip()]
    if not filtered:
        return texts
    idxs, unique_texts = zip(*filtered)
    prompt = (
        f"Переведи каждый из следующих фрагментов с {src_lang} на {tgt_lang}. "
        f"Сохрани стиль и смысл. Ответ дай в том же порядке, каждый перевод отделяй строкой '{BATCH_SEPARATOR.strip()}'.\n\n"
        + BATCH_SEPARATOR.join(unique_texts)
    )
    try:
        response = openai.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=2048,
        )
        result = response.choices[0].message.content.strip().split(BATCH_SEPARATOR)
        # Восстанавливаем исходный порядок, пустые и неуникальные тексты не переводим повторно
        out = list(texts)
        for i, val in zip(idxs, result):
            out[i] = val.strip()
        return out
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return texts


def process_row(row, src_col, tgt_col, src_lang, tgt_lang):
    html = str(row.get(src_col, ''))
    extracted = extract_tagged_text(html)
    translations = {}
    for tag, texts in extracted.items():
        if texts:
            translations[tag] = openai_translate_batch(texts, src_lang, tgt_lang)
    if any(translations.values()):
        new_html = replace_tagged_text(html, translations)
        row[tgt_col] = new_html
    return row


def main(csv_path: str, output_path: str):
    df = pd.read_csv(csv_path)
    src_col, tgt_col, src_lang, tgt_lang = get_lang_columns(df.columns)
    if not src_col or not tgt_col:
        raise ValueError('Не найдены колонки Source/Target language')
    tqdm.pandas(desc="Translating rows (batch)")
    df = df.progress_apply(lambda row: process_row(row, src_col, tgt_col, src_lang, tgt_lang), axis=1)
    df.to_csv(output_path, index=False)
    print(f'Готово! Результат записан в {output_path}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Парсер и переводчик Wix CSV (batch)')
    parser.add_argument('--input', type=str, default='export_en.csv', help='Путь к исходному CSV')
    parser.add_argument('--output', type=str, default='translated.csv', help='Путь к выходному CSV')
    args = parser.parse_args()
    main(args.input, args.output) 
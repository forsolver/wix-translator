
# Назначение

Разработать систему для автоматизированного перевода экспортированных данных из Wix (csv) с использованием OpenAI API.

# Краткое описание

Имеется csv файл, экспортированный из Wix (пример: export_en.csv). В отдельных ячейках этого файла содержатся html-теги с текстом или просто текст. Требуется перевести тексты внутри тегов <h>, <p>, <b>, <li>, <ul>, а также простой текст, которые видны пользователю на сайте.

**Важно:**
- Структура html-тегов должна сохраняться.
- Структура csv-файла также должна оставаться прежней (добавляются только новые столбцы с переводами).
- Перевод выполняется только для строк, относящихся к типам элементов, видимых пользователю:
  - catalog-product
  - wysiwyg.viewer.components.WRichText
  - document.api.MenuService.Menu (только содержимое тегов <label>)
- Строки с типом offline_payment_method не переводятся.

# Этапы

1. Прочитать исходный csv файл.
2. Извлечь из ячеек тексты внутри html-тегов (нужные теги: h, p, b, li, ul), а также простой текст, предназначенный для отображения пользователю.
3. Для строк типа document.api.MenuService.Menu — извлекать и переводить только содержимое тегов <label>.
4. Сформировать массив текстов для перевода.
5. Отправить массив в OpenAI API (GPT-4o) одним промптом для максимального ускорения.
6. Получить переведённые тексты, сохранить порядок и привязку к исходным ячейкам.
7. Вставить переводы обратно внутрь тегов.
8. Сформировать новый столбец с переведённым содержимым для каждого языка.
9. Сохранить итоговый csv файл с новыми столбцами.

# Требования

- Производительность: обработка больших csv (до 50 000 строк) без потери структуры и консистентности данных.
- Безопасность: отсутствие утечки данных, защита ключей OpenAI, корректная обработка ошибок API.
- Масштабируемость: возможность легко добавлять новые языки и поддерживать другие форматы csv.
- Минимум внешних зависимостей (pandas, beautifulsoup4, openai, tqdm, logging, dotenv).

# Технические детали

- Язык реализации: Python 3.10+
- Использовать pandas для работы с csv, BeautifulSoup для html.
- Поддержка асинхронности (через openai.AsyncClient или аналоги).
- Логирование только ошибок и критических событий.
- Реализация кэширования переводов для повторяющихся фрагментов (если время позволит).
- Конфигурируемость через .env (ключ OpenAI, языки, пути файлов).
- CLI-интерфейс для запуска и управления параметрами.

# Результат

- Скрипт/модуль для перевода csv Wix с сохранением структуры html и файла.
- Документация для команды (описание архитектуры, этапы, стандарты, возможные ошибки и решения).
- Примеры запуска и тестовые файлы.

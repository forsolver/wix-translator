## [2025-05-26] - Старт проекта, документация и архитектура
### Добавлено
- docs/Project.md — описание архитектуры, целей, этапов, стандартов
- docs/Tasktracker.md — трекер задач с приоритетами
- docs/Diary.md — дневник наблюдений, решений и проблем
- docs/qa.md — вопросы по архитектуре и реализации
- Прототип функции для чтения и обработки export_en.csv
- Batching: перевод всех текстов одного тега одним промптом
- Фильтрация пустых и дублирующихся текстов
- Логирование только ошибок
- Добавлены задачи на кэширование и асинхронность (TODO)
- В парсер добавлен словарь ISO-кодов языков и их английских названий
- Автоматическое заполнение столбца TARGET_LANGUAGE по SOURCE_LANGUAGE
- Функция translate_plain_text для перевода текста без HTML-тегов
- Логика определения типа контента (HTML или обычный текст)
### Изменено
- Обновлён Tasktracker.md (статус задачи по парсеру)
- Project.md и Tasktracker.md актуализированы
### Исправлено
- Нет 
- Проблема с символами '---' в переведенных тегах h1, h2
- Корректная обработка разделителя BATCH_SEPARATOR 
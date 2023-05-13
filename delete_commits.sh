#!/bin/bash

# Переключаемся на новую ветку
git checkout --orphan new_branch

# Добавляем все файлы в индекс Git
git add -A

# Создаем новый коммит
git commit -am "Initial commit"

# Удаляем старую ветку
git branch -D main

# Переименовываем новую ветку в main
git branch -m main

# Отправляем изменения на сервер
git push -f origin main

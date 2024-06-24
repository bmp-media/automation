import subprocess

def restore_repository():
    # Восстановление изначальных версий файлов до последнего коммита
    subprocess.run(['git', 'restore', '.'])

    # Удаление всех неотслеживаемых файлов
    subprocess.run(['git', 'clean', '-fd'])

    # Получение обновлений из удалённого репозитория
    subprocess.run(['git','pull'])

# Вызов функции для восстановления репозитория

if __name__ == '__main__':
    restore_repository()
